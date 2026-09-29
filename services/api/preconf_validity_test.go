package api

import (
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"bitbucket.org/infinity-exchange/mev-boost-relay/common"
	"bitbucket.org/infinity-exchange/mev-boost-relay/datastore"
	"bitbucket.org/infinity-exchange/mev-boost-relay/metrics"
	"github.com/alicebob/miniredis/v2"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/flashbots/go-boost-utils/bls"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
)

func TestPreconfValidityLateRejection(t *testing.T) {
	for _, belowFloor := range []bool{false, true} {
		name := "replacement"
		if belowFloor {
			name = "below floor cancellation"
		}
		t.Run(name, func(t *testing.T) {
			server := miniredis.RunT(t)
			cache, err := datastore.NewRedisCache("", server.Addr(), "")
			require.NoError(t, err)
			sk, pk, err := bls.GenerateNewKeypair()
			require.NoError(t, err)
			var publicKey phase0.BLSPubKey
			copy(publicKey[:], bls.PublicKeyToBytes(pk))
			api := &RelayAPI{log: common.TestLog, redis: cache, blsSk: sk, publicKey: &publicKey}
			valid, _, _ := common.CreateTestBlockSubmission(t, testBuilderPubkey, uint256.NewInt(10), &common.CreateTestBlockSubmissionOpts{Slot: 42})
			first := httptest.NewRecorder()
			saved, _, ok := api.updateRedisBid(redisUpdateBidOpts{w: first, tx: cache.NewPipeline(), log: common.TestLog, payload: valid, receivedAt: time.Now(), cancellationsEnabled: true, isValidPreconf: true})
			require.True(t, ok, first.Body.String())
			require.True(t, saved.WasBidSaved)
			invalid, _, _ := common.CreateTestBlockSubmission(t, testBuilderPubkey, uint256.NewInt(20), &common.CreateTestBlockSubmissionOpts{Slot: 42})
			submission, err := common.GetBlockSubmissionInfo(invalid)
			require.NoError(t, err)
			rr := httptest.NewRecorder()
			// Invoke the final admission paths directly to simulate a request that
			// passed the initial marker check before the valid bid was saved.
			if belowFloor {
				require.NoError(t, cache.SetFloorBidValue(42, submission.BidTrace.ParentHash.String(), submission.BidTrace.ProposerPubkey.String(), "30"))
				_, ok = api.checkFloorBidValue(bidFloorOpts{w: rr, tx: cache.NewPipeline(), log: common.TestLog, cancellationsEnabled: true, isValidPreconf: false, simResultC: make(chan *blockSimResult, 1), submission: submission})
			} else {
				_, _, ok = api.updateRedisBid(redisUpdateBidOpts{w: rr, tx: cache.NewPipeline(), log: common.TestLog, payload: invalid, receivedAt: time.Now(), cancellationsEnabled: true, isValidPreconf: false})
			}
			require.False(t, ok)
			require.Equal(t, http.StatusBadRequest, rr.Code)
			require.Contains(t, rr.Body.String(), datastore.ErrInvalidPreconfReplacement.Error())
			latest, err := cache.GetBuilderLatestValue(42, submission.BidTrace.ParentHash.String(), submission.BidTrace.ProposerPubkey.String(), testBuilderPubkey)
			require.NoError(t, err)
			require.Equal(t, big.NewInt(10), latest)
		})
	}
}

func TestPreconfCancellationFloorRules(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		previousValid, nextValid bool
		value, delivered         uint64
		wantStatus               int
		wantLatest               uint64
	}{
		{"valid below floor removes active bid", true, true, 5, 0, http.StatusAccepted, 0},
		{"fallback below floor removes active bid", false, false, 5, 0, http.StatusAccepted, 0},
		{"invalid below floor preserves valid bid", true, false, 5, 0, http.StatusBadRequest, 20},
		{"valid at floor replaces active bid", true, true, 10, 0, http.StatusOK, 10},
		{"valid above floor lowers active bid", true, true, 15, 0, http.StatusOK, 15},
		{"same slot delivered preserves active bid", true, true, 5, 42, http.StatusBadRequest, 20},
		{"later slot delivered preserves active bid", true, true, 5, 43, http.StatusBadRequest, 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache, err := datastore.NewRedisCache("", miniredis.RunT(t).Addr(), "")
			require.NoError(t, err)
			sk, pk, err := bls.GenerateNewKeypair()
			require.NoError(t, err)
			var publicKey phase0.BLSPubKey
			copy(publicKey[:], bls.PublicKeyToBytes(pk))
			api := &RelayAPI{log: common.TestLog, redis: cache, blsSk: sk, publicKey: &publicKey}
			save := func(builder string, value uint64, cancellable, valid bool) {
				payload, _, _ := common.CreateTestBlockSubmission(t, builder, uint256.NewInt(value), &common.CreateTestBlockSubmissionOpts{Slot: 42})
				rr := httptest.NewRecorder()
				_, _, ok := api.updateRedisBid(redisUpdateBidOpts{w: rr, tx: cache.NewPipeline(), log: common.TestLog, payload: payload, receivedAt: time.Now(), cancellationsEnabled: cancellable, isValidPreconf: valid})
				require.True(t, ok, rr.Body.String())
			}
			// Another builder's noncancellable bid establishes a real floor/header.
			save((phase0.BLSPubKey{2}).String(), 10, false, true)
			save(testBuilderPubkey, 20, true, tc.previousValid)
			if tc.delivered != 0 {
				require.NoError(t, cache.CheckAndSetLastSlotAndHashDelivered(tc.delivered, (phase0.Hash32{}).String()))
			}
			payload, _, _ := common.CreateTestBlockSubmission(t, testBuilderPubkey, uint256.NewInt(tc.value), &common.CreateTestBlockSubmissionOpts{Slot: 42})
			submission, err := common.GetBlockSubmissionInfo(payload)
			require.NoError(t, err)
			rr := httptest.NewRecorder()
			floor, proceed := api.checkFloorBidValue(bidFloorOpts{w: rr, tx: cache.NewPipeline(), log: common.TestLog, cancellationsEnabled: true, isValidPreconf: tc.nextValid, simResultC: make(chan *blockSimResult, 1), submission: submission})
			if proceed {
				// Exercise storage after admission; simulation/signature checks are
				// outside this focused cancellation-rule test.
				_, _, ok := api.updateRedisBid(redisUpdateBidOpts{w: rr, tx: cache.NewPipeline(), log: common.TestLog, payload: payload, receivedAt: time.Now(), floorBidValue: floor, cancellationsEnabled: true, isValidPreconf: tc.nextValid})
				require.True(t, ok, rr.Body.String())
			}
			require.Equal(t, tc.wantStatus, rr.Code, rr.Body.String())
			latest, err := cache.GetBuilderLatestValue(42, submission.BidTrace.ParentHash.String(), submission.BidTrace.ProposerPubkey.String(), testBuilderPubkey)
			require.NoError(t, err)
			require.Equal(t, new(big.Int).SetUint64(tc.wantLatest), latest)
			remainingFloor, err := cache.GetFloorBidValue(t.Context(), cache.NewPipeline(), 42, submission.BidTrace.ParentHash.String(), submission.BidTrace.ProposerPubkey.String())
			require.NoError(t, err)
			require.Equal(t, big.NewInt(10), remainingFloor)
			marker, err := cache.GetIsValidPreconf(42, submission.BidTrace.ParentHash.String(), submission.BidTrace.ProposerPubkey.String(), testBuilderPubkey)
			require.NoError(t, err)
			require.Equal(t, tc.previousValid || proceed && tc.nextValid, marker)
			if tc.delivered != 0 {
				require.Contains(t, rr.Body.String(), "payload for this slot was already delivered")
			}
			if tc.wantLatest == 0 {
				best, err := cache.GetBestBid(42, submission.BidTrace.ParentHash.String(), submission.BidTrace.ProposerPubkey.String())
				require.NoError(t, err)
				value, err := best.Value()
				require.NoError(t, err)
				require.Equal(t, big.NewInt(10), value.ToBig(), "noncancellable floor remains available")
			}
		})
	}
}

func TestPreconfCancellationDisabled(t *testing.T) {
	oldHistogram := metrics.SubmitNewBlockLatencyHistogram
	t.Cleanup(func() { metrics.SubmitNewBlockLatencyHistogram = oldHistogram })
	metrics.SubmitNewBlockLatencyHistogram = noop.Float64Histogram{}
	api := &RelayAPI{log: common.TestLog, ffEnableCancellations: false}
	rr := httptest.NewRecorder()
	api.handleSubmitNewBlock(rr, httptest.NewRequest(http.MethodPost, "/relay/v1/builder/blocks?cancellations=1", nil))
	require.Equal(t, http.StatusBadRequest, rr.Code)
	require.Contains(t, rr.Body.String(), "cancellations are disabled")
}
