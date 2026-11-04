package api

import (
	"encoding/json"
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
	"github.com/attestantio/go-eth2-client/spec/capella"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/flashbots/go-boost-utils/bls"
	"github.com/flashbots/go-boost-utils/ssz"
	"github.com/flashbots/go-boost-utils/utils"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

const gloasExchangeProposerIndex = uint64(17)

type gloasExchangeSimulation struct {
	Method string `json:"method"`
	Params []struct {
		common.GloasSubmitBlockRequest
		RegisteredGasLimit uint64 `json:"registered_gas_limit,string"`
	} `json:"params"`
}

type gloasExchangeSubmissionFixture struct {
	backend    *testBackend
	request    *common.GloasSubmitBlockRequest
	preference *common.SignedProposerPreferences
	builderSK  *bls.SecretKey
	simulated  chan gloasExchangeSimulation
}

func newGloasExchangeSubmissionFixture(t *testing.T, storePreferences bool) *gloasExchangeSubmissionFixture {
	t.Helper()
	backend := func() *testBackend {
		// Authentication is already supplied by the stub. Do not start an
		// unbounded background login worker that would outlive this test.
		previousDisabled := disableEthgasMarketAPI
		disableEthgasMarketAPI = true
		defer func() { disableEthgasMarketAPI = previousDisabled }()
		return newTestBackend(t, 1)
	}()
	backend.relay.headSlot.Store(testSlot - 1)
	backend.relay.capellaEpoch = 0
	backend.relay.denebEpoch = 0
	backend.relay.electraEpoch = 0
	backend.relay.fuluEpoch = 0
	backend.relay.ffOptimisticAllSlots = false
	backend.relay.ffDisableLowPrioBuilders = false
	// Put requests safely after exchange finalization without relying on the
	// test machine's date or a subsecond position within the current slot.
	backend.relay.genesisInfo.Data.GenesisTime = uint64(time.Now().Unix()) - testSlot*common.SecondsPerSlot - 2

	builderSK, builderBLS, err := bls.GenerateNewKeypair()
	require.NoError(t, err)
	builder, err := utils.BlsPublicKeyToPublicKey(builderBLS)
	require.NoError(t, err)
	proposerSK, proposerBLS, err := bls.GenerateNewKeypair()
	require.NoError(t, err)
	proposer, err := utils.BlsPublicKeyToPublicKey(proposerBLS)
	require.NoError(t, err)
	versioned, _, _ := common.CreateTestBlockSubmission(t, builder.String(), uint256.NewInt(1), &common.CreateTestBlockSubmissionOpts{
		Slot:           testSlot,
		ParentHash:     testParentHash,
		ProposerPubkey: proposer.String(),
		Version:        spec.DataVersionFulu,
	})
	versioned.Fulu.Message.ProposerFeeRecipient = testAddress
	versioned.Fulu.Message.GasLimit = testGasLimit
	versioned.Fulu.ExecutionPayload.GasLimit = testGasLimit
	// Execution coinbase is not the signed proposer payout recipient.
	versioned.Fulu.ExecutionPayload.FeeRecipient = bellatrix.ExecutionAddress{0x03}
	versioned.Fulu.ExecutionPayload.Timestamp = backend.relay.genesisInfo.Data.GenesisTime + testSlot*common.SecondsPerSlot
	versioned.Fulu.ExecutionPayload.Transactions = []bellatrix.Transaction{}
	versioned.Fulu.ExecutionPayload.Withdrawals = []*capella.Withdrawal{}
	prevRandao, err := utils.HexToHash(testPrevRandao)
	require.NoError(t, err)
	versioned.Fulu.ExecutionPayload.PrevRandao = prevRandao
	gloasPayload, err := common.NewExecutionPayloadGloas(versioned.Fulu.ExecutionPayload, []byte{0xc0}, testSlot)
	require.NoError(t, err)
	request := &common.GloasSubmitBlockRequest{
		Message:           versioned.Fulu.Message,
		ExecutionPayload:  gloasPayload,
		BlobsBundle:       versioned.Fulu.BlobsBundle,
		ExecutionRequests: common.NewExecutionRequestsGloas(versioned.Fulu.ExecutionRequests),
	}
	// Exercise the native five-list V6 representation, not just Fulu's lists.
	request.ExecutionRequests.BuilderDeposits = []*common.BuilderDepositRequestGloas{{Amount: 1}}
	request.ExecutionRequests.BuilderExits = []*common.BuilderExitRequestGloas{{}}

	withdrawalsRoot, err := ComputeWithdrawalsRoot(versioned.Fulu.ExecutionPayload.Withdrawals)
	require.NoError(t, err)
	backend.relay.payloadAttributes = map[string]payloadAttributesHelper{
		getPayloadAttributesKey(testParentHash, testSlot): {
			slot:            testSlot,
			proposerIndex:   gloasExchangeProposerIndex,
			parentHash:      testParentHash,
			withdrawalsRoot: withdrawalsRoot,
			payloadAttributes: beaconclient.PayloadAttributes{
				PrevRandao: testPrevRandao,
			},
		},
	}
	// Gloas must work without a legacy validator registration or proposer duty.
	backend.relay.proposerDutiesMap = map[uint64]*common.BuilderGetValidatorsResponseEntry{}
	backend.datastore.SetKnownValidator(common.NewPubkeyHex(proposer.String()), gloasExchangeProposerIndex)
	backend.relay.opts.EthNetDetails.GloasForkVersionHex = "0x80000038"
	domain, err := common.ComputeDomain(common.DomainTypeProposerPreferences, "0x80000038", backend.relay.opts.EthNetDetails.GenesisValidatorsRootHex)
	require.NoError(t, err)
	backend.relay.opts.EthNetDetails.DomainProposerPreferences = domain
	preference := signedGloasProposerPreferences(t, domain, proposerSK, testSlot, gloasExchangeProposerIndex, phase0.Root{0x99}, testAddress, testGasLimit)
	if storePreferences {
		backend.relay.processGloasProposerPreferences(beaconclient.ProposerPreferencesEvent{Version: common.ForkVersionStringGloas, Data: preference})
	}

	simulated := make(chan gloasExchangeSimulation, 4)
	simulator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var captured gloasExchangeSimulation
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode V6 simulation request: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		simulated <- captured
		w.Header().Set(HeaderContentType, ApplicationJSON)
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":"1","result":{}}`))
	}))
	t.Cleanup(simulator.Close)
	backend.relay.blockSimRateLimiter = NewBlockSimulationRateLimiter(simulator.URL)
	// Drain request-owned persistence before restoring globals or closing stubs.
	t.Cleanup(backend.relay.payloadSavesWG.Wait)
	return &gloasExchangeSubmissionFixture{backend: backend, request: request, preference: preference, builderSK: builderSK, simulated: simulated}
}

func (fixture *gloasExchangeSubmissionFixture) submit(t *testing.T, encoding string) *httptest.ResponseRecorder {
	t.Helper()
	signature, err := ssz.SignMessage(fixture.request.Message, fixture.backend.relay.opts.EthNetDetails.DomainBuilder, fixture.builderSK)
	require.NoError(t, err)
	fixture.request.Signature = signature
	var body []byte
	contentType := ApplicationJSON
	if encoding == "SSZ" {
		body, err = fixture.request.MarshalSSZ()
		contentType = ApplicationOctetStream
	} else {
		body, err = json.Marshal(fixture.request)
	}
	require.NoError(t, err)
	return fixture.backend.requestBytes(http.MethodPost, pathSubmitNewBlock, body, map[string]string{
		HeaderContentType:         contentType,
		HeaderEthConsensusVersion: common.ForkVersionStringGloas,
	})
}

func (fixture *gloasExchangeSubmissionFixture) requireAccepted(t *testing.T, encoding string) {
	t.Helper()
	rr := fixture.submit(t, encoding)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	select {
	case captured := <-fixture.simulated:
		require.Equal(t, "flashbots_validateBuilderSubmissionV6", captured.Method)
		require.Len(t, captured.Params, 1)
		require.Equal(t, fixture.request, &captured.Params[0].GloasSubmitBlockRequest, "V6 must preserve the builder-signed bid trace and complete Gloas payload")
		require.Equal(t, uint64(fixture.preference.Message.TargetGasLimit), captured.Params[0].RegisteredGasLimit)
	default:
		t.Fatal("accepted submission never reached V6 simulation")
	}
	cached, err := fixture.backend.redis.GetGloasPayload(testSlot, fixture.request.ExecutionPayload.BlockHash.String())
	require.NoError(t, err)
	require.NotNil(t, cached, "accepted Gloas payload must remain revealable from Redis")
	require.Equal(t, fixture.request.Message.ProposerFeeRecipient, cached.FeeRecipient)
	require.Equal(t, *fixture.request.Message, cached.BidTrace.BidTrace)
	require.Equal(t, fixture.request.ExecutionPayload, cached.Contents.ExecutionPayload)
	require.Equal(t, fixture.request.ExecutionRequests, cached.Contents.ExecutionRequests)
	fixture.backend.relay.gloasPayloadsLock.RLock()
	memoryPayload := fixture.backend.relay.gloasPayloads[gloasPayloadKey(testSlot, fixture.request.ExecutionPayload.BlockHash)]
	fixture.backend.relay.gloasPayloadsLock.RUnlock()
	require.NotNil(t, memoryPayload)
	require.Equal(t, fixture.request.Message.ProposerFeeRecipient, memoryPayload.feeRecipient)
	if fixture.request.Message.ProposerFeeRecipient != fixture.preference.Message.FeeRecipient {
		require.NotNil(t, cached.ExchangeFeeRecipient, "validated buyer payout must survive cache eviction and split relay deployments")
		require.Equal(t, fixture.request.Message.ProposerFeeRecipient, *cached.ExchangeFeeRecipient)
		require.Equal(t, cached.ExchangeFeeRecipient, memoryPayload.exchangeFeeRecipient)
	} else {
		require.Nil(t, cached.ExchangeFeeRecipient, "fallback admission must not fabricate exchange payout policy")
	}

	preferences, err := fixture.backend.redis.GetGloasSignedProposerPreferencesForDuty(testSlot, gloasExchangeProposerIndex)
	require.NoError(t, err)
	require.Equal(t, []*common.SignedProposerPreferences{fixture.preference}, preferences, "exchange routing must not rewrite signed proposer preferences")
	verified, err := ssz.VerifySignature(preferences[0].Message, fixture.backend.relay.opts.EthNetDetails.DomainProposerPreferences, fixture.request.Message.ProposerPubkey[:], preferences[0].Signature[:])
	require.NoError(t, err)
	require.True(t, verified)
}

type gloasExchangeStub struct {
	builderCalls  atomic.Int32
	bundleCalls   atomic.Int32
	builderStatus atomic.Int32
	bundleStatus  atomic.Int32
}

func stubGloasSubmissionExchange(t *testing.T, assignment BuilderResponse, bundles PreconfBundles) *gloasExchangeStub {
	t.Helper()
	stub := new(gloasExchangeStub)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data any
		var status int32
		switch r.URL.Path {
		case fmt.Sprintf("/api/v1/p/builder/%d", testSlot):
			stub.builderCalls.Add(1)
			status = stub.builderStatus.Load()
			data = assignment
		case "/api/v1/slot/bundles":
			stub.bundleCalls.Add(1)
			status = stub.bundleStatus.Load()
			if r.URL.Query().Get("slot") != fmt.Sprint(testSlot) || r.Header.Get("Authorization") != "Bearer gloas-exchange-test-token" {
				t.Error("bundle lookup must use the submission slot and authenticated exchange client")
				http.Error(w, "incorrect bundle lookup", http.StatusUnauthorized)
				return
			}
			data = bundles
		default:
			t.Errorf("unexpected exchange request: %s", r.URL)
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodGet {
			t.Errorf("exchange lookup method = %s, want GET", r.Method)
		}
		if status != 0 {
			w.Header().Set("Retry-After", "60")
			http.Error(w, "exchange unavailable", int(status))
			return
		}
		w.Header().Set(HeaderContentType, ApplicationJSON)
		_ = json.NewEncoder(w).Encode(struct {
			Success bool `json:"success"`
			Data    any  `json:"data"`
		}{Success: true, Data: data})
	}))
	t.Cleanup(server.Close)
	exchangeAPIURL = server.URL
	client = &ApiClient{APIURL: server.URL, Client: server.Client(), AccessToken: "gloas-exchange-test-token"}
	return stub
}

func TestBuilderSubmitBlockGloasExchange(t *testing.T) {
	// Package tests deliberately run serially because exchange settings are
	// process-wide. Each fixture has its own assignment and bundle cache.
	previousDisabled, previousURL, previousClient := disableEthgasMarketAPI, exchangeAPIURL, client
	previousFinalized, previousClose := getExchangeFinalizedCutoffMs, getExchangeMarketCloseCutoffMs
	previousPreconfCheck, previousSpaceCheck := skipPreconfCheck, skipEmptySpaceCheck
	previousNamespace := simRPCNamespace
	t.Cleanup(func() {
		disableEthgasMarketAPI, exchangeAPIURL, client = previousDisabled, previousURL, previousClient
		getExchangeFinalizedCutoffMs, getExchangeMarketCloseCutoffMs = previousFinalized, previousClose
		skipPreconfCheck, skipEmptySpaceCheck = previousPreconfCheck, previousSpaceCheck
		simRPCNamespace = previousNamespace
	})
	getExchangeFinalizedCutoffMs, getExchangeMarketCloseCutoffMs = -2000, -4000
	skipPreconfCheck, skipEmptySpaceCheck = false, false
	simRPCNamespace = "flashbots"

	for _, encoding := range []string{"JSON", "SSZ"} {
		t.Run(encoding, func(t *testing.T) {
			for _, name := range []string{
				"buyer recipient accepted",
				"validator recipient rejected when buyer differs",
				"nondelegated builder rejected",
				"markets disabled avoid exchange",
				"empty exchange recipient falls back to signed preferences",
				"cached assignment and bundles avoid fetch",
				"cross-slot bundles rejected without caching",
				"cross-slot assignment rejected without caching",
				"rate-limited assignment rejected with cooldown",
				"rate-limited bundles rejected with cooldown",
				"exchange recipient does not bypass missing preferences",
				"exchange recipient does not bypass signed gas target",
			} {
				t.Run(name, func(t *testing.T) {
					disableEthgasMarketAPI = false
					fixture := newGloasExchangeSubmissionFixture(t, name != "exchange recipient does not bypass missing preferences")
					assignment := BuilderResponse{Slot: testSlot, Builders: []string{fixture.request.Message.BuilderPubkey.String()}}
					bundleSlot := testSlot
					bundles := PreconfBundles{Slot: &bundleSlot, FeeRecipient: testAddress2.String()}
					fixture.request.Message.ProposerFeeRecipient = testAddress2
					switch name {
					case "validator recipient rejected when buyer differs", "markets disabled avoid exchange", "empty exchange recipient falls back to signed preferences", "cross-slot bundles rejected without caching":
						fixture.request.Message.ProposerFeeRecipient = testAddress
					}
					switch name {
					case "nondelegated builder rejected":
						assignment.Builders = []string{testBuilderPubkey}
					case "markets disabled avoid exchange":
						disableEthgasMarketAPI = true
						assignment.Builders = []string{testBuilderPubkey}
					case "empty exchange recipient falls back to signed preferences":
						bundles.FeeRecipient = ""
					case "cross-slot bundles rejected without caching":
						bundleSlot++
					case "cross-slot assignment rejected without caching":
						assignment.Slot++
						assignment.Builders = []string{testBuilderPubkey}
					case "exchange recipient does not bypass signed gas target":
						fixture.backend.relay.gloasPayloads["known-parent"] = &gloasPayloadContext{payload: &common.ExecutionPayloadGloas{
							BlockHash: fixture.request.Message.ParentHash,
							GasLimit:  common.Uint64String(testGasLimit),
						}}
						fixture.request.Message.GasLimit++
						fixture.request.ExecutionPayload.GasLimit++
					}
					exchange := stubGloasSubmissionExchange(t, assignment, bundles)
					switch name {
					case "rate-limited assignment rejected with cooldown":
						exchange.builderStatus.Store(http.StatusTooManyRequests)
					case "rate-limited bundles rejected with cooldown":
						exchange.bundleStatus.Store(http.StatusTooManyRequests)
					}
					if name == "cached assignment and bundles avoid fetch" {
						_, err := fixture.backend.relay.exchangeLookups.save(exchangeLookupKey{apiURL: exchangeAPIURL, endpoint: "builders", slot: testSlot}, &assignment, nil)
						require.NoError(t, err)
						_, err = fixture.backend.relay.exchangeLookups.save(exchangeLookupKey{apiURL: exchangeAPIURL, endpoint: "bundles", slot: testSlot}, &bundles, nil)
						require.NoError(t, err)
					}
					var rejected string
					rejectedStatus := http.StatusBadRequest
					switch name {
					case "cross-slot assignment rejected without caching", "rate-limited assignment rejected with cooldown":
						rejected, rejectedStatus = "builder assignment unavailable", http.StatusServiceUnavailable
					case "cross-slot bundles rejected without caching", "rate-limited bundles rejected with cooldown":
						rejected, rejectedStatus = "slot bundles unavailable", http.StatusServiceUnavailable
					case "validator recipient rejected when buyer differs":
						rejected = "fee recipient does not match"
					case "nondelegated builder rejected":
						rejected = "builder pubkey"
					case "exchange recipient does not bypass missing preferences":
						rejected = "no signed proposer preferences"
					case "exchange recipient does not bypass signed gas target":
						rejected = "gas limit"
					}
					if rejected != "" {
						rr := fixture.submit(t, encoding)
						require.Equal(t, rejectedStatus, rr.Code, rr.Body.String())
						require.Contains(t, rr.Body.String(), rejected)
						require.Empty(t, fixture.simulated, "rejected submission must not be simulated")
						cached, err := fixture.backend.redis.GetGloasPayload(testSlot, fixture.request.ExecutionPayload.BlockHash.String())
						require.NoError(t, err)
						require.Nil(t, cached, "rejected submission must not become revealable")
					} else {
						fixture.requireAccepted(t, encoding)
					}
					switch name {
					case "markets disabled avoid exchange", "cached assignment and bundles avoid fetch":
						require.Zero(t, exchange.builderCalls.Load())
						require.Zero(t, exchange.bundleCalls.Load())
					case "nondelegated builder rejected", "cross-slot assignment rejected without caching", "rate-limited assignment rejected with cooldown":
						require.EqualValues(t, 1, exchange.builderCalls.Load())
						require.Zero(t, exchange.bundleCalls.Load())
					default:
						require.EqualValues(t, 1, exchange.builderCalls.Load())
						require.EqualValues(t, 1, exchange.bundleCalls.Load())
					}
					if rejectedStatus == http.StatusServiceUnavailable {
						endpoint := "bundles"
						if rejected == "builder assignment unavailable" {
							endpoint = "builders"
						}
						cache := &fixture.backend.relay.exchangeLookups
						cache.mu.Lock()
						_, cached := cache.entries[exchangeLookupKey{apiURL: exchangeAPIURL, endpoint: endpoint, slot: testSlot}]
						cache.mu.Unlock()
						require.False(t, cached, "an unavailable or cross-slot response must not become a successful cached response")
					}
					if name == "rate-limited assignment rejected with cooldown" || name == "rate-limited bundles rejected with cooldown" {
						builderCalls, bundleCalls := exchange.builderCalls.Load(), exchange.bundleCalls.Load()
						rr := fixture.submit(t, encoding)
						require.Equal(t, http.StatusServiceUnavailable, rr.Code, rr.Body.String())
						require.Contains(t, rr.Body.String(), rejected)
						require.Equal(t, builderCalls, exchange.builderCalls.Load(), "retry must not refetch during cooldown")
						require.Equal(t, bundleCalls, exchange.bundleCalls.Load(), "retry must not refetch during cooldown")
						require.Empty(t, fixture.simulated)
					}
				})
			}
		})
	}
}
