package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"bitbucket.org/infinity-exchange/mev-boost-relay/beaconclient"
	"bitbucket.org/infinity-exchange/mev-boost-relay/common"
	"bitbucket.org/infinity-exchange/mev-boost-relay/datastore"
	"bitbucket.org/infinity-exchange/mev-boost-relay/metrics"
	"github.com/alicebob/miniredis/v2"
	builderDeneb "github.com/attestantio/go-builder-client/api/deneb"
	builderElectra "github.com/attestantio/go-builder-client/api/electra"
	builderSpec "github.com/attestantio/go-builder-client/spec"
	"github.com/attestantio/go-eth2-client/spec"
	"github.com/attestantio/go-eth2-client/spec/bellatrix"
	"github.com/attestantio/go-eth2-client/spec/deneb"
	"github.com/attestantio/go-eth2-client/spec/electra"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/flashbots/go-boost-utils/bls"
	"github.com/flashbots/go-boost-utils/ssz"
	"github.com/gorilla/mux"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
)

// Exercise the complete getHeader response, including fork-specific fields
// that must be retained in the message covered by the relay signature.
func TestGetHeaderPrefetchedMarketAcrossForks(t *testing.T) {
	oldClient, oldDisabled, oldMultiplier := client, disableEthgasMarketAPI, realTimeBidMultiplier
	oldCutoff, oldDelay, oldFinalized := getHeaderRequestCutoffMs, delayGetHeader, getExchangeFinalizedCutoffMs
	oldHistogram := metrics.GetHeaderLatencyHistogram
	t.Cleanup(func() {
		client, disableEthgasMarketAPI, realTimeBidMultiplier = oldClient, oldDisabled, oldMultiplier
		getHeaderRequestCutoffMs, delayGetHeader, getExchangeFinalizedCutoffMs = oldCutoff, oldDelay, oldFinalized
		metrics.GetHeaderLatencyHistogram = oldHistogram
	})
	disableEthgasMarketAPI, realTimeBidMultiplier = false, "100"
	getHeaderRequestCutoffMs, delayGetHeader, getExchangeFinalizedCutoffMs = 0, 0, -2000
	metrics.GetHeaderLatencyHistogram, _ = noop.NewMeterProvider().Meter("market-fork-test").Float64Histogram("latency")

	for _, version := range []spec.DataVersion{spec.DataVersionDeneb, spec.DataVersionElectra, spec.DataVersionFulu} {
		for _, mediaType := range []string{common.ApplicationJSON, common.ApplicationOctetStream} {
			t.Run(version.String()+"/"+mediaType, func(t *testing.T) {
				redisServer := miniredis.RunT(t)
				cache, err := datastore.NewRedisCache("", redisServer.Addr(), "")
				require.NoError(t, err)
				sk, pk, err := bls.GenerateNewKeypair()
				require.NoError(t, err)
				var publicKey phase0.BLSPubKey
				copy(publicKey[:], bls.PublicKeyToBytes(pk))
				network, err := common.NewEthNetworkDetails(common.EthNetworkMainnet)
				require.NoError(t, err)
				parent, proposer := (phase0.Hash32{}).String(), (phase0.BLSPubKey{}).String()
				payload, getPayload, _ := common.CreateTestBlockSubmission(t, (phase0.BLSPubKey{1}).String(), uint256.NewInt(99), &common.CreateTestBlockSubmissionOpts{Slot: 42, Version: version})
				commitments := []deneb.KZGCommitment{{1, 2, 3}}
				requests := &electra.ExecutionRequests{
					Deposits:       []*electra.DepositRequest{{Pubkey: phase0.BLSPubKey{1}, WithdrawalCredentials: make([]byte, 32), Amount: 32000000000, Signature: phase0.BLSSignature{2}, Index: 3}},
					Withdrawals:    []*electra.WithdrawalRequest{{SourceAddress: bellatrix.ExecutionAddress{4}, ValidatorPubkey: phase0.BLSPubKey{5}, Amount: 6}},
					Consolidations: []*electra.ConsolidationRequest{{SourceAddress: bellatrix.ExecutionAddress{7}, SourcePubkey: phase0.BLSPubKey{8}, TargetPubkey: phase0.BLSPubKey{9}}},
				}
				switch version { //nolint:exhaustive
				case spec.DataVersionDeneb:
					payload.Deneb.BlobsBundle.Commitments = commitments
				case spec.DataVersionElectra:
					payload.Electra.BlobsBundle.Commitments = commitments
					payload.Electra.ExecutionRequests = requests
				case spec.DataVersionFulu:
					payload.Fulu.BlobsBundle.Commitments = commitments
					payload.Fulu.ExecutionRequests = requests
				}
				bid, err := common.BuildGetHeaderResponse(payload, sk, &publicKey, network.DomainBuilder)
				require.NoError(t, err)
				originalJSON, err := json.Marshal(bid)
				require.NoError(t, err)
				trace, err := payload.BidTrace()
				require.NoError(t, err)
				_, err = cache.SaveBidAndUpdateTopBid(t.Context(), cache.NewPipeline(), &common.BidTraceV2WithBlobFields{BidTrace: *trace}, payload, getPayload, bid, time.Now(), false, nil, true)
				require.NoError(t, err)
				db := &marketModeDB{saved: make(chan struct{}, 1)}
				api := &RelayAPI{
					opts: RelayAPIOpts{EthNetDetails: *network},
					log:  common.TestLog, redis: cache, db: db, blsSk: sk, publicKey: &publicKey,
					genesisInfo: &beaconclient.GetGenesisResponse{Data: beaconclient.GetGenesisResponseData{GenesisTime: uint64(time.Now().Unix()) - 42*common.SecondsPerSlot}},
				}
				var marketCalls atomic.Int32
				client = &ApiClient{AccessToken: "test-token", Client: &http.Client{Transport: exchangeTestTransport(func(r *http.Request) (*http.Response, error) {
					marketCalls.Add(1)
					return marketModeTransport{status: http.StatusOK, body: `{"success":true,"data":{"markets":{"slot":42,"multiRelay":true,"realtime":true}}}`}.RoundTrip(r)
				})}}
				require.NoError(t, api.prefetchMarket(t.Context(), client, exchangeAPIURL, 42))
				require.EqualValues(t, 1, marketCalls.Load())

				for attempt := 0; attempt < 3; attempt++ {
					wantValue := "9900"
					if attempt == 2 {
						api.marketCache.Delete(uint64(42))
						wantValue = "99"
					}
					req := mux.SetURLVars(httptest.NewRequest(http.MethodGet, "/eth/v1/builder/header/42/"+parent+"/"+proposer, nil), map[string]string{"slot": "42", "parent_hash": parent, "pubkey": proposer})
					req.Header.Set("Accept", mediaType)
					rr := httptest.NewRecorder()
					api.handleGetHeader(rr, req)
					require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
					require.EqualValues(t, 1, marketCalls.Load(), "neither cached nor missing market data may trigger request-time HTTP")
					response := decodePrefetchedForkBid(t, version, mediaType, rr)
					value, err := response.Value()
					require.NoError(t, err)
					require.Equal(t, wantValue, value.Dec(), "repeated requests must not compound the multiplier")
					message, signature := prefetchedForkBidMessage(t, response)
					valid, err := ssz.VerifySignature(message, network.DomainBuilder, publicKey[:], signature[:])
					require.NoError(t, err)
					require.True(t, valid, "signature must cover blobs and execution requests in the returned message")

					// Compare every field to the original bid, allowing only the
					// advertised value and its corresponding signature to change.
					var expected builderSpec.VersionedSignedBuilderBid
					require.NoError(t, json.Unmarshal(originalJSON, &expected))
					switch version { //nolint:exhaustive
					case spec.DataVersionDeneb:
						expected.Deneb.Message.Value, expected.Deneb.Signature = value, signature
					case spec.DataVersionElectra:
						expected.Electra.Message.Value, expected.Electra.Signature = value, signature
					case spec.DataVersionFulu:
						expected.Fulu.Message.Value, expected.Fulu.Signature = value, signature
					}
					expectedJSON, err := json.Marshal(&expected)
					require.NoError(t, err)
					responseJSON, err := json.Marshal(response)
					require.NoError(t, err)
					require.JSONEq(t, string(expectedJSON), string(responseJSON))
					cachedBid, err := cache.GetBestBid(42, parent, proposer)
					require.NoError(t, err)
					cachedJSON, err := json.Marshal(cachedBid)
					require.NoError(t, err)
					require.JSONEq(t, string(originalJSON), string(cachedJSON), "serving an adjusted bid must not mutate the original Redis bid")
					if attempt == 2 {
						require.JSONEq(t, string(originalJSON), string(responseJSON), "cache miss preserves the original signed bid")
					}
					select {
					case <-db.saved:
					case <-time.After(time.Second):
						t.Fatal("header telemetry did not finish")
					}
				}
			})
		}
	}
}

func decodePrefetchedForkBid(t *testing.T, version spec.DataVersion, mediaType string, rr *httptest.ResponseRecorder) *builderSpec.VersionedSignedBuilderBid {
	t.Helper()
	response := &builderSpec.VersionedSignedBuilderBid{Version: version}
	if mediaType == common.ApplicationJSON {
		require.NoError(t, json.Unmarshal(rr.Body.Bytes(), response))
		require.Equal(t, version, response.Version)
		return response
	}
	require.Equal(t, common.ApplicationOctetStream, rr.Header().Get(HeaderContentType))
	require.Equal(t, version.String(), rr.Header().Get(HeaderEthConsensusVersion))
	switch version { //nolint:exhaustive
	case spec.DataVersionDeneb:
		response.Deneb = new(builderDeneb.SignedBuilderBid)
		require.NoError(t, response.Deneb.UnmarshalSSZ(rr.Body.Bytes()))
	case spec.DataVersionElectra:
		response.Electra = new(builderElectra.SignedBuilderBid)
		require.NoError(t, response.Electra.UnmarshalSSZ(rr.Body.Bytes()))
	case spec.DataVersionFulu:
		response.Fulu = new(builderElectra.SignedBuilderBid)
		require.NoError(t, response.Fulu.UnmarshalSSZ(rr.Body.Bytes()))
	default:
		t.Fatalf("unsupported test fork %s", version)
	}
	return response
}

func prefetchedForkBidMessage(t *testing.T, bid *builderSpec.VersionedSignedBuilderBid) (ssz.ObjWithHashTreeRoot, phase0.BLSSignature) {
	t.Helper()
	switch bid.Version { //nolint:exhaustive
	case spec.DataVersionDeneb:
		return bid.Deneb.Message, bid.Deneb.Signature
	case spec.DataVersionElectra:
		return bid.Electra.Message, bid.Electra.Signature
	case spec.DataVersionFulu:
		return bid.Fulu.Message, bid.Fulu.Signature
	default:
		t.Fatalf("unsupported test fork %s", bid.Version)
		return nil, phase0.BLSSignature{}
	}
}
