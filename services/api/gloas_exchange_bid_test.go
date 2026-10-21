package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"bitbucket.org/infinity-exchange/mev-boost-relay/beaconclient"
	"bitbucket.org/infinity-exchange/mev-boost-relay/common"
	"github.com/attestantio/go-eth2-client/spec"
	"github.com/attestantio/go-eth2-client/spec/bellatrix"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/flashbots/go-boost-utils/ssz"
	"github.com/flashbots/go-boost-utils/utils"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

// The protocol bid recipient is signed preference A, while an authorized
// exchange buyer B receives the execution-layer payout in the original V6 data.
type gloasExchangeBidFixture struct {
	backend       *testBackend
	auth          *common.SignedBuilderRequestAuth
	path          string
	proposer      phase0.BLSPubKey
	preferences   *common.SignedProposerPreferences
	submission    *common.VersionedSubmitBlockRequest
	cachedPayload *common.GloasPayloadCacheEntry
	legacyBidJSON []byte
	payloadJSON   []byte
	cacheJSON     []byte
	requests      atomic.Int32
}

func newGloasExchangeBidFixture(t *testing.T, payout bellatrix.ExecutionAddress, snapshot *bellatrix.ExecutionAddress) *gloasExchangeBidFixture {
	t.Helper()
	f := new(gloasExchangeBidFixture)
	previousClient, previousDisabled := client, disableEthgasMarketAPI
	preconfCacheMutex.Lock()
	previousPreconfs := preconfCache
	preconfCache = make(map[uint64]PreconfBundles)
	preconfCacheMutex.Unlock()
	t.Cleanup(func() {
		client, disableEthgasMarketAPI = previousClient, previousDisabled
		preconfCacheMutex.Lock()
		preconfCache = previousPreconfs
		preconfCacheMutex.Unlock()
		require.Zero(t, f.requests.Load(), "getBid must not make additional exchange HTTP calls")
	})
	disableEthgasMarketAPI = true
	f.backend = newTestBackend(t, 1)
	disableEthgasMarketAPI = false
	client = &ApiClient{AccessToken: "test-token", Client: &http.Client{Transport: exchangeTestTransport(func(*http.Request) (*http.Response, error) {
		f.requests.Add(1)
		return nil, errors.New("unexpected exchange HTTP request")
	})}}
	f.cacheMarketMode()

	const proposerIndex = uint64(17)
	builderIndex := uint64(7)
	f.backend.relay.opts.GloasBuilderIndex = &builderIndex
	f.backend.relay.opts.EthNetDetails.GloasForkVersionHex = "0x80000038"
	f.backend.relay.setGloasBuilderIdentity(builderIndex, true, "active")
	domain, err := common.ComputeDomain(common.DomainTypeProposerPreferences,
		f.backend.relay.opts.EthNetDetails.GloasForkVersionHex,
		f.backend.relay.opts.EthNetDetails.GenesisValidatorsRootHex)
	require.NoError(t, err)
	f.backend.relay.opts.EthNetDetails.DomainProposerPreferences = domain
	auth, proposerSK, proposer := newSignedGloasRequestAuth(t, f.backend.relay.opts.EthNetDetails.DomainBuilderRequestAuth, testSlot, f.backend.relay.opts.GloasRequestAuthData)
	f.auth, f.proposer = auth, proposer
	f.backend.datastore.SetKnownValidator(common.NewPubkeyHex(proposer.String()), proposerIndex)
	parentRoot := "0x23e606c7b3d1faad7e83503ce3dedce4c6bb89b0c28ffb240d713c7b110b9747"
	rootHash, err := utils.HexToHash(parentRoot)
	require.NoError(t, err)
	parentBeaconRoot := phase0.Root(rootHash)
	f.backend.relay.payloadAttributes[getPayloadAttributesKey(testParentHash, testSlot)] = payloadAttributesHelper{
		slot: testSlot, proposerIndex: proposerIndex, parentHash: testParentHash, parentBeaconRoot: &parentBeaconRoot,
		payloadAttributes: beaconclient.PayloadAttributes{SuggestedFeeRecipient: testAddress.String()},
	}
	f.preferences = signedGloasProposerPreferences(t, domain, proposerSK, testSlot, proposerIndex, phase0.Root{0x99}, testAddress, testGasLimit)
	f.backend.relay.processGloasProposerPreferences(beaconclient.ProposerPreferencesEvent{Version: common.ForkVersionStringGloas, Data: f.preferences})
	f.path = fmt.Sprintf("/eth/v1/builder/execution_payload_bid/%d/%s/%s/%s", testSlot, testParentHash, parentRoot, proposer.String())

	payload, _, _ := common.CreateTestBlockSubmission(t, testBuilderPubkey, uint256.MustFromDecimal("11000000001"), &common.CreateTestBlockSubmissionOpts{
		Slot: testSlot, ParentHash: testParentHash, ProposerPubkey: proposer.String(), Version: spec.DataVersionFulu,
	})
	payload.Fulu.Message.ProposerFeeRecipient = payout
	payload.Fulu.Message.BlockHash = phase0.Hash32{0x42}
	payload.Fulu.ExecutionPayload.FeeRecipient = payout
	payload.Fulu.ExecutionPayload.BlockHash = payload.Fulu.Message.BlockHash
	payload.Fulu.ExecutionPayload.GasLimit = testGasLimit
	gloasPayload, err := common.NewExecutionPayloadGloas(payload.Fulu.ExecutionPayload, []byte{0xc0}, testSlot)
	require.NoError(t, err)
	payload.Gloas = &common.GloasPayloadContents{
		ExecutionPayload: gloasPayload, ExecutionRequests: common.NewExecutionRequestsGloas(payload.Fulu.ExecutionRequests), BlobsBundle: payload.Fulu.BlobsBundle,
	}
	payload.GloasExchangeFeeRecipient = snapshot
	f.submission = payload
	getPayload, err := common.BuildGetPayloadResponse(payload)
	require.NoError(t, err)
	getHeader, err := common.BuildGetHeaderResponse(payload, f.backend.relay.blsSk, f.backend.relay.publicKey, f.backend.relay.opts.EthNetDetails.DomainBuilder)
	require.NoError(t, err)
	submissionInfo, err := common.GetBlockSubmissionInfo(payload)
	require.NoError(t, err)
	trace := &common.BidTraceV2WithBlobFields{
		BidTrace: *submissionInfo.BidTrace, BlockNumber: submissionInfo.BlockNumber,
		NumTx: uint64(len(submissionInfo.Transactions)), NumBlobs: uint64(len(submissionInfo.Blobs)),
		BlobGasUsed: submissionInfo.BlobGasUsed, ExcessBlobGas: submissionInfo.ExcessBlobGas,
	}
	_, err = f.backend.redis.SaveBidAndUpdateTopBid(t.Context(), f.backend.redis.NewPipeline(), trace, payload, getPayload, getHeader, time.Now(), false, nil, true)
	require.NoError(t, err)
	f.cachedPayload, err = f.backend.redis.GetGloasPayload(testSlot, gloasPayload.BlockHash.String())
	require.NoError(t, err)
	require.NotNil(t, f.cachedPayload)
	require.Equal(t, snapshot, f.cachedPayload.ExchangeFeeRecipient, "accepted submission must persist its authorization snapshot")
	legacyBid, err := f.backend.redis.GetBestBid(testSlot, testParentHash, proposer.String())
	require.NoError(t, err)
	f.legacyBidJSON, err = json.Marshal(legacyBid)
	require.NoError(t, err)
	f.payloadJSON, err = json.Marshal(payload.Gloas)
	require.NoError(t, err)
	f.cacheJSON, err = json.Marshal(f.cachedPayload)
	require.NoError(t, err)
	require.NoError(t, f.backend.redis.SaveGloasBuilderPreferences(testSlot, proposer.String(), &common.BuilderPreferences{MaxExecutionPayment: ^phase0.Gwei(0)}, time.Minute))
	return f
}

func (f *gloasExchangeBidFixture) cacheMarketMode() {
	multiRelay, realTime := true, false
	f.backend.relay.marketCache.Store(testSlot, &marketCacheEntry{
		value: &WholeBlockMarket{Slot: testSlot, MultiRelay: &multiRelay, RealTime: &realTime}, expiration: time.Now().Add(time.Minute),
	})
}

func (f *gloasExchangeBidFixture) cacheBuyer(buyer string, slot *uint64) {
	preconfCacheMutex.Lock()
	preconfCache[testSlot] = PreconfBundles{Slot: slot, FeeRecipient: buyer}
	preconfCacheMutex.Unlock()
}

func (f *gloasExchangeBidFixture) request(t *testing.T, contentType string) *httptest.ResponseRecorder {
	t.Helper()
	var body []byte
	var err error
	if contentType == common.ApplicationOctetStream {
		body, err = f.auth.MarshalSSZ()
	} else {
		body, err = json.Marshal(f.auth)
	}
	require.NoError(t, err)
	return f.backend.requestBytes(http.MethodPost, f.path, body, map[string]string{
		HeaderEthConsensusVersion: "gloas", HeaderContentType: contentType, HeaderAccept: contentType,
		"Date-Milliseconds": "123456789", "X-Timeout-Ms": "400",
	})
}

func (f *gloasExchangeBidFixture) requireBid(t *testing.T, contentType string) *common.SignedExecutionPayloadBid {
	t.Helper()
	rr := f.request(t, contentType)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	require.Equal(t, "gloas", rr.Header().Get(HeaderEthConsensusVersion))
	var bid *common.SignedExecutionPayloadBid
	if contentType == common.ApplicationOctetStream {
		require.Equal(t, common.ApplicationOctetStream, rr.Header().Get(HeaderContentType))
		bid = new(common.SignedExecutionPayloadBid)
		require.NoError(t, bid.UnmarshalSSZ(rr.Body.Bytes()))
	} else {
		response := new(common.VersionedSignedExecutionPayloadBidResponse)
		require.NoError(t, json.Unmarshal(rr.Body.Bytes(), response))
		require.Equal(t, "gloas", response.Version)
		bid = response.Data
	}
	require.NotNil(t, bid)
	require.NotNil(t, bid.Message)
	require.Equal(t, testAddress, bid.Message.FeeRecipient, "outbound bid must honor signed preference A, not exchange payout B")
	require.Equal(t, phase0.Slot(testSlot), bid.Message.Slot)
	require.Equal(t, f.cachedPayload.BlockHash, bid.Message.BlockHash)
	require.Equal(t, common.Uint64String(7), bid.Message.BuilderIndex)
	require.Equal(t, phase0.Gwei(11), bid.Message.ExecutionPayment, "explicit multi-relay market must not apply the 11K adjustment")
	valid, err := ssz.VerifySignature(bid.Message, f.backend.relay.opts.EthNetDetails.DomainBeaconBuilderGloas, f.backend.relay.publicKey[:], bid.Signature[:])
	require.NoError(t, err)
	require.True(t, valid)
	f.requireUnchanged(t)
	return bid
}

func (f *gloasExchangeBidFixture) requireUnchanged(t *testing.T) {
	t.Helper()
	cached, err := f.backend.redis.GetGloasPayload(testSlot, f.cachedPayload.BlockHash.String())
	require.NoError(t, err)
	cacheJSON, err := json.Marshal(cached)
	require.NoError(t, err)
	require.JSONEq(t, string(f.cacheJSON), string(cacheJSON), "getBid must not rewrite the persisted payout or authorization")
	legacyBid, err := f.backend.redis.GetBestBid(testSlot, testParentHash, f.proposer.String())
	require.NoError(t, err)
	legacyJSON, err := json.Marshal(legacyBid)
	require.NoError(t, err)
	require.JSONEq(t, string(f.legacyBidJSON), string(legacyJSON), "the original signed Fulu bid must remain unchanged")
	payloadJSON, err := json.Marshal(f.submission.Gloas)
	require.NoError(t, err)
	require.JSONEq(t, string(f.payloadJSON), string(payloadJSON), "V6 simulation/reveal data must retain its original payout")
	require.Equal(t, f.cachedPayload.FeeRecipient, f.submission.Fulu.Message.ProposerFeeRecipient)
	require.Equal(t, f.cachedPayload.FeeRecipient, f.submission.Gloas.ExecutionPayload.FeeRecipient)
	require.Equal(t, f.cachedPayload.ExchangeFeeRecipient, f.submission.GloasExchangeFeeRecipient)
	preferences, err := f.backend.relay.resolveGloasProposerPreferences(testSlot, 17, f.proposer)
	require.NoError(t, err)
	require.Equal(t, f.preferences, preferences, "signed proposer preferences must not be modified for the exchange buyer")
	valid, err := ssz.VerifySignature(preferences.Message, f.backend.relay.opts.EthNetDetails.DomainProposerPreferences, f.proposer[:], preferences.Signature[:])
	require.NoError(t, err)
	require.True(t, valid)
	require.Zero(t, f.requests.Load())
}

func TestGloasExchangeBidRecipientSplit(t *testing.T) {
	for _, withSnapshot := range []bool{false, true} {
		t.Run(fmt.Sprintf("persisted override %t", withSnapshot), func(t *testing.T) {
			buyer := testAddress2
			var snapshot *bellatrix.ExecutionAddress
			if withSnapshot {
				snapshot = &buyer
			}
			f := newGloasExchangeBidFixture(t, buyer, snapshot)
			slot := testSlot
			f.cacheBuyer(buyer.String(), &slot)
			jsonBid := f.requireBid(t, common.ApplicationJSON)
			sszBid := f.requireBid(t, common.ApplicationOctetStream)
			require.Equal(t, jsonBid, sszBid)
		})
	}
}

func TestGloasExchangeBidPersistedOverrideAcrossInstances(t *testing.T) {
	buyer := testAddress2
	f := newGloasExchangeBidFixture(t, buyer, &buyer)
	slot := testSlot
	f.cacheBuyer(buyer.String(), &slot)
	firstBid := f.requireBid(t, common.ApplicationJSON)
	firstRelay := f.backend.relay
	preconfCacheMutex.Lock()
	preconfCache = make(map[uint64]PreconfBundles)
	preconfCacheMutex.Unlock()
	previousDisabled := disableEthgasMarketAPI
	disableEthgasMarketAPI = true
	secondRelay, err := NewRelayAPI(firstRelay.opts)
	disableEthgasMarketAPI = previousDisabled
	require.NoError(t, err)
	secondRelay.genesisInfo = firstRelay.genesisInfo
	secondRelay.setGloasBuilderIdentity(*firstRelay.opts.GloasBuilderIndex, true, "active")
	secondRelay.payloadAttributes[getPayloadAttributesKey(testParentHash, testSlot)] = firstRelay.payloadAttributes[getPayloadAttributesKey(testParentHash, testSlot)]
	require.Empty(t, secondRelay.gloasPayloads)
	require.Empty(t, secondRelay.gloasSelectedPayloads)
	f.backend.relay = secondRelay
	f.cacheMarketMode()
	for _, contentType := range []string{common.ApplicationJSON, common.ApplicationOctetStream} {
		recoveredBid := f.requireBid(t, contentType)
		require.Equal(t, firstBid, recoveredBid)
	}
	recovered := secondRelay.gloasPayloads[gloasPayloadKey(testSlot, f.cachedPayload.BlockHash)]
	require.NotNil(t, recovered)
	require.Equal(t, &buyer, recovered.exchangeFeeRecipient)
	require.Equal(t, buyer, recovered.feeRecipient)
	require.Equal(t, buyer, recovered.bidTrace.ProposerFeeRecipient)
}

func TestGloasExchangeBidRejectsUnauthorizedPayout(t *testing.T) {
	for _, tc := range []struct {
		name       string
		snapshot   bool
		disabled   bool
		hasCurrent bool
		buyer      string
	}{
		{name: "wrong payout without an override"},
		{name: "persisted override ignored when markets disabled", snapshot: true, disabled: true},
		{name: "current buyer supersedes stale persisted override", snapshot: true, hasCurrent: true, buyer: (bellatrix.ExecutionAddress{0x44}).String()},
		{name: "empty current response revokes stale persisted override", snapshot: true, hasCurrent: true},
		{name: "malformed current exchange recipient", snapshot: true, hasCurrent: true, buyer: "not-an-address"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buyer := testAddress2
			var snapshot *bellatrix.ExecutionAddress
			if tc.snapshot {
				snapshot = &buyer
			}
			f := newGloasExchangeBidFixture(t, buyer, snapshot)
			disableEthgasMarketAPI = tc.disabled
			if tc.hasCurrent {
				slot := testSlot
				f.cacheBuyer(tc.buyer, &slot)
			}
			for _, contentType := range []string{common.ApplicationJSON, common.ApplicationOctetStream} {
				rr := f.request(t, contentType)
				require.Equal(t, http.StatusNoContent, rr.Code, rr.Body.String())
				require.Empty(t, rr.Body.Bytes())
			}
			require.Empty(t, f.backend.relay.gloasSelectedPayloads, "an unauthorized payout must not select a bid")
			f.requireUnchanged(t)
		})
	}
}

func TestGloasExchangeBidRejectsMalformedOverrideSnapshot(t *testing.T) {
	for _, mismatch := range []string{"cached payout", "bid trace payout"} {
		t.Run(mismatch, func(t *testing.T) {
			buyer := testAddress2
			f := newGloasExchangeBidFixture(t, buyer, &buyer)
			entry := new(common.GloasPayloadCacheEntry)
			require.NoError(t, json.Unmarshal(f.cacheJSON, entry))
			if mismatch == "cached payout" {
				entry.FeeRecipient = testAddress
			} else {
				entry.BidTrace.ProposerFeeRecipient = testAddress
			}
			_, err := gloasPayloadContextFromCache(entry)
			require.Error(t, err, "an override snapshot must match both cached actual payout and original bid trace")
			require.NoError(t, f.backend.redis.SaveGloasPayload(entry))
			for _, contentType := range []string{common.ApplicationJSON, common.ApplicationOctetStream} {
				rr := f.request(t, contentType)
				require.Equal(t, http.StatusInternalServerError, rr.Code, rr.Body.String())
				require.Contains(t, rr.Body.String(), "invalid cached Gloas reveal payload")
			}
			require.Empty(t, f.backend.relay.gloasSelectedPayloads, "invalid snapshots must not select a bid")
		})
	}
}
