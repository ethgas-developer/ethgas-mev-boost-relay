package datastore

import (
	"testing"

	"bitbucket.org/infinity-exchange/mev-boost-relay/common"
	"github.com/alicebob/miniredis/v2"
	"github.com/attestantio/go-eth2-client/spec"
	"github.com/attestantio/go-eth2-client/spec/bellatrix"
	"github.com/attestantio/go-eth2-client/spec/electra"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/stretchr/testify/require"
)

// Keep the claimed execution hash identical while changing the reveal-only
// access list and every request list. A rejected replacement must not corrupt
// the reveal data associated with an already admitted bid.
func newGloasPreconfTestBid(t *testing.T, value uint64, marker byte) preconfTestBid {
	t.Helper()
	bid := newPreconfTestBid(t, spec.DataVersionFulu, 1, value)
	requests := bid.payload.Fulu.ExecutionRequests
	requests.Deposits = []*electra.DepositRequest{{
		Pubkey:                phase0.BLSPubKey{marker},
		WithdrawalCredentials: make([]byte, 32),
		Amount:                phase0.Gwei(marker),
	}}
	requests.Withdrawals = []*electra.WithdrawalRequest{{
		SourceAddress:   bellatrix.ExecutionAddress{marker},
		ValidatorPubkey: phase0.BLSPubKey{marker},
		Amount:          phase0.Gwei(marker),
	}}
	requests.Consolidations = []*electra.ConsolidationRequest{{
		SourceAddress: bellatrix.ExecutionAddress{marker},
		SourcePubkey:  phase0.BLSPubKey{marker},
		TargetPubkey:  phase0.BLSPubKey{marker + 1},
	}}
	fullRequests := common.NewExecutionRequestsGloas(requests)
	fullRequests.BuilderDeposits = []*common.BuilderDepositRequestGloas{{
		Pubkey:                phase0.BLSPubKey{marker},
		WithdrawalCredentials: phase0.Root{marker},
		Amount:                phase0.Gwei(marker),
	}}
	fullRequests.BuilderExits = []*common.BuilderExitRequestGloas{{
		SourceAddress: bellatrix.ExecutionAddress{marker},
		Pubkey:        phase0.BLSPubKey{marker},
	}}
	gloasPayload, err := common.NewExecutionPayloadGloas(bid.payload.Fulu.ExecutionPayload, []byte{0xc1, marker}, bid.trace.Slot)
	require.NoError(t, err)
	bid.payload.Gloas = &common.GloasPayloadContents{
		ExecutionPayload:  gloasPayload,
		BlobsBundle:       bid.payload.Fulu.BlobsBundle,
		ExecutionRequests: fullRequests,
	}
	return bid
}

func TestGloasPreconfReplacementPreservesReveal(t *testing.T) {
	for _, cancellable := range []bool{false, true} {
		name := "noncancellable"
		if cancellable {
			name = "cancellable"
		}
		t.Run(name, func(t *testing.T) {
			server := miniredis.RunT(t)
			cache := newPreconfTestCache(t, server)
			first := newGloasPreconfTestBid(t, 10, 1)
			result, err := first.save(t.Context(), cache, cancellable, true)
			require.NoError(t, err)
			require.True(t, result.WasBidSaved)
			stored, err := cache.GetGloasPayload(first.trace.Slot, first.trace.BlockHash.String())
			require.NoError(t, err)
			require.NotNil(t, stored, "admitted bids must persist complete reveal data without a separate API write")
			require.Equal(t, first.payload.Gloas, stored.Contents)
			require.Equal(t, first.trace, stored.BidTrace)
			require.Equal(t, expiryBidCache, server.TTL(cache.keyGloasPayload(first.trace.Slot, first.trace.BlockHash.String())))

			keys := []string{
				cache.keyGloasPayload(first.trace.Slot, first.trace.BlockHash.String()),
				cache.keyLatestBidByBuilder(first.trace.Slot, first.trace.ParentHash.String(), first.trace.ProposerPubkey.String(), first.trace.BuilderPubkey.String()),
				cache.keyCacheGetHeaderResponse(first.trace.Slot, first.trace.ParentHash.String(), first.trace.ProposerPubkey.String()),
				cache.keyCacheBidTrace(first.trace.Slot, first.trace.ProposerPubkey.String(), first.trace.BlockHash.String()),
			}
			before := make([]string, len(keys))
			for i, key := range keys {
				before[i], err = cache.client.Get(t.Context(), key).Result()
				require.NoError(t, err)
			}

			invalid := newGloasPreconfTestBid(t, 20, 2)
			require.Equal(t, first.trace.BlockHash, invalid.trace.BlockHash)
			result, err = invalid.save(t.Context(), cache, cancellable, false)
			require.ErrorIs(t, err, ErrInvalidPreconfReplacement)
			require.False(t, result.WasBidSaved)
			for i, key := range keys {
				after, getErr := cache.client.Get(t.Context(), key).Result()
				require.NoError(t, getErr)
				require.Equal(t, before[i], after, "rejected replacement changed %s", key)
			}
			preserved, err := cache.GetGloasPayload(first.trace.Slot, first.trace.BlockHash.String())
			require.NoError(t, err)
			require.Equal(t, stored, preserved)
			require.True(t, first.marker(t, cache))
			latest, err := cache.GetBuilderLatestValue(first.trace.Slot, first.trace.ParentHash.String(), first.trace.ProposerPubkey.String(), first.trace.BuilderPubkey.String())
			require.NoError(t, err)
			require.Equal(t, int64(10), latest.Int64())

			// A valid same-value replacement still commits the complete updated
			// reveal, even though the top-value fast path skips the top-bid update.
			replacement := newGloasPreconfTestBid(t, 10, 3)
			result, err = replacement.save(t.Context(), cache, cancellable, true)
			require.NoError(t, err)
			require.True(t, result.WasBidSaved)
			updated, err := cache.GetGloasPayload(replacement.trace.Slot, replacement.trace.BlockHash.String())
			require.NoError(t, err)
			require.Equal(t, replacement.payload.Gloas, updated.Contents)
			require.NotEqual(t, stored.Contents, updated.Contents)
		})
	}
}
