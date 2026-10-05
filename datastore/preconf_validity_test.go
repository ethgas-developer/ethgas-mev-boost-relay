package datastore

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"bitbucket.org/infinity-exchange/mev-boost-relay/common"
	"github.com/alicebob/miniredis/v2"
	builderApi "github.com/attestantio/go-builder-client/api"
	builderSpec "github.com/attestantio/go-builder-client/spec"
	"github.com/attestantio/go-eth2-client/spec"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/holiman/uint256"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

type preconfTestBid struct {
	trace   *common.BidTraceV2WithBlobFields
	payload *common.VersionedSubmitBlockRequest
	body    *builderApi.VersionedSubmitBlindedBlockResponse
	header  *builderSpec.VersionedSignedBuilderBid
}

func newPreconfTestBid(t *testing.T, version spec.DataVersion, builder byte, value uint64) preconfTestBid {
	t.Helper()
	payload, body, header := common.CreateTestBlockSubmission(t, (phase0.BLSPubKey{builder}).String(), uint256.NewInt(value), &common.CreateTestBlockSubmissionOpts{Slot: 42, Version: version})
	trace, err := payload.BidTrace()
	require.NoError(t, err)
	return preconfTestBid{trace: &common.BidTraceV2WithBlobFields{BidTrace: *trace}, payload: payload, body: body, header: header}
}

func (b preconfTestBid) save(ctx context.Context, cache *RedisCache, cancellable, valid bool) (SaveBidAndUpdateTopBidResponse, error) {
	return cache.SaveBidAndUpdateTopBid(ctx, cache.NewPipeline(), b.trace, b.payload, b.body, b.header, time.Now(), cancellable, nil, valid)
}

func (b preconfTestBid) marker(t *testing.T, cache *RedisCache) bool {
	t.Helper()
	valid, err := cache.GetIsValidPreconf(b.trace.Slot, b.trace.ParentHash.String(), b.trace.ProposerPubkey.String(), b.trace.BuilderPubkey.String())
	require.NoError(t, err)
	return valid
}

func newPreconfTestCache(t *testing.T, server *miniredis.Miniredis) *RedisCache {
	t.Helper()
	cache, err := NewRedisCache("", server.Addr(), "")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cache.client.Close()) })
	return cache
}

func TestPreconfValidityPersistence(t *testing.T) {
	for _, version := range []spec.DataVersion{spec.DataVersionCapella, spec.DataVersionDeneb} {
		for _, tc := range []struct {
			name               string
			cancellable, valid bool
			value, otherValue  uint64
		}{
			{"cancellable valid", true, true, 10, 0},
			{"noncancellable valid", false, true, 10, 0},
			{"cancellable fallback", true, false, 10, 0},
			{"noncancellable fallback", false, false, 10, 0},
			{"valid below another builder", true, true, 10, 20},
			{"fallback below another builder", true, false, 10, 20},
			{"valid zero bid", true, true, 0, 0},
		} {
			t.Run(version.String()+"/"+tc.name, func(t *testing.T) {
				server := miniredis.RunT(t)
				cache := newPreconfTestCache(t, server)
				if tc.otherValue > 0 {
					_, err := newPreconfTestBid(t, version, 2, tc.otherValue).save(t.Context(), cache, true, true)
					require.NoError(t, err)
				}
				bid := newPreconfTestBid(t, version, 1, tc.value)
				result, err := bid.save(t.Context(), cache, tc.cancellable, tc.valid)
				require.NoError(t, err)
				require.True(t, result.WasBidSaved)
				require.Equal(t, tc.valid, bid.marker(t, cache))
				markerKey := cache.keyIsValidPreconf(42, bid.trace.ParentHash.String(), bid.trace.ProposerPubkey.String(), bid.trace.BuilderPubkey.String())
				require.True(t, server.Exists(markerKey), "false fallback status is saved too")
				require.Equal(t, expiryBidCache, server.TTL(markerKey))
				latest, err := cache.GetBuilderLatestBid(42, bid.trace.ParentHash.String(), bid.trace.ProposerPubkey.String(), bid.trace.BuilderPubkey.String())
				require.NoError(t, err)
				require.NotNil(t, latest, "bid must be committed even if the top value is unchanged")
			})
		}
	}
}

func TestPreconfValidityTransitions(t *testing.T) {
	for _, priorCancellable := range []bool{false, true} {
		for _, nextCancellable := range []bool{false, true} {
			for _, priorValid := range []bool{false, true} {
				for _, nextValid := range []bool{false, true} {
					t.Run(fmt.Sprintf("cancellable_%t_to_%t/valid_%t_to_%t", priorCancellable, nextCancellable, priorValid, nextValid), func(t *testing.T) {
						server := miniredis.RunT(t)
						cache := newPreconfTestCache(t, server)
						first := newPreconfTestBid(t, spec.DataVersionCapella, 1, 10)
						_, err := first.save(t.Context(), cache, priorCancellable, priorValid)
						require.NoError(t, err)
						next := newPreconfTestBid(t, spec.DataVersionCapella, 1, 20)
						snapshotKeys := []string{
							cache.keyLatestBidByBuilder(42, first.trace.ParentHash.String(), first.trace.ProposerPubkey.String(), first.trace.BuilderPubkey.String()),
							cache.keyCacheGetHeaderResponse(42, first.trace.ParentHash.String(), first.trace.ProposerPubkey.String()),
							cache.keyExecPayloadCapella(42, first.trace.ProposerPubkey.String(), first.trace.BlockHash.String()),
							cache.keyCacheBidTrace(42, first.trace.ProposerPubkey.String(), first.trace.BlockHash.String()),
						}
						before := make([]string, len(snapshotKeys))
						for i, key := range snapshotKeys {
							before[i], err = cache.client.Get(t.Context(), key).Result()
							require.NoError(t, err)
						}
						// Same claimed hash, different payload bytes: a rejected replacement
						// must not even overwrite the already retrievable execution payload.
						next.payload.Capella.ExecutionPayload.ExtraData = []byte{9}
						result, err := next.save(t.Context(), cache, nextCancellable, nextValid)
						if priorValid && !nextValid {
							require.ErrorIs(t, err, ErrInvalidPreconfReplacement)
							require.False(t, result.WasBidSaved)
							require.True(t, first.marker(t, cache))
							for i, key := range snapshotKeys {
								after, getErr := cache.client.Get(t.Context(), key).Result()
								require.NoError(t, getErr)
								require.Equal(t, before[i], after)
							}
						} else {
							require.NoError(t, err)
							require.True(t, result.WasBidSaved)
							require.Equal(t, nextValid, next.marker(t, cache))
						}
					})
				}
			}
		}
	}
}

func TestPreconfValiditySameValueUpgrade(t *testing.T) {
	cache := newPreconfTestCache(t, miniredis.RunT(t))
	bid := newPreconfTestBid(t, spec.DataVersionCapella, 1, 10)
	_, err := bid.save(t.Context(), cache, true, false)
	require.NoError(t, err)
	result, err := bid.save(t.Context(), cache, true, true)
	require.NoError(t, err)
	require.True(t, result.WasBidSaved)
	require.True(t, bid.marker(t, cache))
	_, err = bid.save(t.Context(), cache, true, false)
	require.ErrorIs(t, err, ErrInvalidPreconfReplacement)
}

func TestPreconfValidityFailedSave(t *testing.T) {
	cache := newPreconfTestCache(t, miniredis.RunT(t))
	bid := newPreconfTestBid(t, spec.DataVersionCapella, 1, 10)
	bid.payload.Capella.ExecutionPayload.ExtraData = make([]byte, 33)
	_, err := bid.save(t.Context(), cache, true, true)
	require.Error(t, err)
	require.False(t, bid.marker(t, cache))
	latest, err := cache.GetBuilderLatestBid(42, bid.trace.ParentHash.String(), bid.trace.ProposerPubkey.String(), bid.trace.BuilderPubkey.String())
	require.NoError(t, err)
	require.Nil(t, latest)
}

// Pause an invalid request after reading the old marker, then let a separate
// relay client commit a valid bid before the invalid transaction executes.
type preconfReadBarrier struct {
	key     string
	reached chan struct{}
	resume  chan struct{}
	used    atomic.Bool
}

func (h *preconfReadBarrier) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) { return next(ctx, network, addr) }
}
func (h *preconfReadBarrier) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *preconfReadBarrier) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if cmd.Name() == "get" && len(cmd.Args()) == 2 && cmd.Args()[1] == h.key && h.used.CompareAndSwap(false, true) {
			close(h.reached)
			select {
			case <-h.resume:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return err
	}
}

func TestPreconfValidityConcurrentReplacement(t *testing.T) {
	for _, cancelBelowFloor := range []bool{false, true} {
		t.Run(fmt.Sprintf("delete_%t", cancelBelowFloor), func(t *testing.T) {
			server := miniredis.RunT(t)
			invalidCache, validCache := newPreconfTestCache(t, server), newPreconfTestCache(t, server)
			invalid, valid := newPreconfTestBid(t, spec.DataVersionCapella, 1, 30), newPreconfTestBid(t, spec.DataVersionCapella, 1, 20)
			barrier := &preconfReadBarrier{key: invalidCache.keyIsValidPreconf(42, invalid.trace.ParentHash.String(), invalid.trace.ProposerPubkey.String(), invalid.trace.BuilderPubkey.String()), reached: make(chan struct{}), resume: make(chan struct{})}
			invalidCache.client.AddHook(barrier)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				if cancelBelowFloor {
					done <- invalidCache.DelBuilderBid(ctx, invalidCache.NewPipeline(), 42, invalid.trace.ParentHash.String(), invalid.trace.ProposerPubkey.String(), invalid.trace.BuilderPubkey.String(), false)
					return
				}
				_, err := invalid.save(ctx, invalidCache, true, false)
				done <- err
			}()
			select {
			case <-barrier.reached:
			case <-ctx.Done():
				t.Fatal("invalid request did not reach the marker read")
			}
			_, err := valid.save(ctx, validCache, true, true)
			require.NoError(t, err)
			close(barrier.resume)
			select {
			case err := <-done:
				require.ErrorIs(t, err, ErrInvalidPreconfReplacement)
			case <-ctx.Done():
				t.Fatal("invalid request did not finish")
			}
			require.True(t, valid.marker(t, validCache))
			value, err := validCache.GetBuilderLatestValue(42, valid.trace.ParentHash.String(), valid.trace.ProposerPubkey.String(), valid.trace.BuilderPubkey.String())
			require.NoError(t, err)
			require.Equal(t, int64(20), value.Int64())
		})
	}
}

func TestPreconfValidityCancellationPreservesGuard(t *testing.T) {
	cache := newPreconfTestCache(t, miniredis.RunT(t))
	bid := newPreconfTestBid(t, spec.DataVersionCapella, 1, 10)
	_, err := bid.save(t.Context(), cache, true, true)
	require.NoError(t, err)
	require.NoError(t, cache.DelBuilderBid(t.Context(), cache.NewPipeline(), 42, bid.trace.ParentHash.String(), bid.trace.ProposerPubkey.String(), bid.trace.BuilderPubkey.String(), true))
	require.True(t, bid.marker(t, cache))
	_, err = bid.save(t.Context(), cache, true, false)
	require.ErrorIs(t, err, ErrInvalidPreconfReplacement)
	// A different builder has an independent marker and may still submit fallback bids.
	_, err = newPreconfTestBid(t, spec.DataVersionCapella, 2, 20).save(t.Context(), cache, true, false)
	require.NoError(t, err)
}
