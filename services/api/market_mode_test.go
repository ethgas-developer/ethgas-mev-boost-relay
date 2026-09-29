package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"bitbucket.org/infinity-exchange/mev-boost-relay/beaconclient"
	"bitbucket.org/infinity-exchange/mev-boost-relay/common"
	"bitbucket.org/infinity-exchange/mev-boost-relay/database"
	"bitbucket.org/infinity-exchange/mev-boost-relay/datastore"
	"bitbucket.org/infinity-exchange/mev-boost-relay/metrics"
	"github.com/alicebob/miniredis/v2"
	builderSpec "github.com/attestantio/go-builder-client/spec"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/flashbots/go-boost-utils/bls"
	"github.com/flashbots/go-boost-utils/ssz"
	"github.com/gorilla/mux"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
)

type marketModeTransport struct {
	status int
	body   string
}

func (m marketModeTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: m.status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(m.body))}, nil
}

type marketModeDB struct {
	database.IDatabaseService
	saved chan struct{}
}

func (db *marketModeDB) InsertGetPayload(uint64, string, string, uint64, uint64, uint64, uint64) error {
	db.saved <- struct{}{}
	return nil
}

func TestGetHeaderMarketMode(t *testing.T) {
	oldClient, oldDisabled, oldMultiplier := client, disableEthgasMarketAPI, realTimeBidMultiplier
	oldCutoff, oldDelay, oldFinalized := getHeaderRequestCutoffMs, delayGetHeader, getExchangeFinalizedCutoffMs
	oldHistogram := metrics.GetHeaderLatencyHistogram
	t.Cleanup(func() {
		client, disableEthgasMarketAPI, realTimeBidMultiplier = oldClient, oldDisabled, oldMultiplier
		getHeaderRequestCutoffMs, delayGetHeader, getExchangeFinalizedCutoffMs = oldCutoff, oldDelay, oldFinalized
		metrics.GetHeaderLatencyHistogram = oldHistogram
	})
	disableEthgasMarketAPI, realTimeBidMultiplier = false, "2"
	getHeaderRequestCutoffMs, delayGetHeader, getExchangeFinalizedCutoffMs = 0, 0, -2000
	metrics.GetHeaderLatencyHistogram, _ = noop.NewMeterProvider().Meter("market-mode-test").Float64Histogram("latency")
	explicitTrue, explicitFalse := true, false

	for _, tc := range []struct {
		name         string
		body         string
		status       int
		cachedMarket *WholeBlockMarket
		wantValue    string
		missingToken bool
		transport    http.RoundTripper
	}{
		{name: "no exchange token", missingToken: true, transport: exchangeTestTransport(func(*http.Request) (*http.Response, error) { panic("request path must not attempt login") }), wantValue: "99"},
		{name: "expired exchange token", status: http.StatusUnauthorized, body: `{}`, wantValue: "99"},
		{name: "exchange connection refused", transport: exchangeTestTransport(func(*http.Request) (*http.Response, error) { return nil, errors.New("connection refused") }), wantValue: "99"},
		{name: "exchange response stalls", transport: exchangeTestTransport(func(r *http.Request) (*http.Response, error) { <-r.Context().Done(); return nil, r.Context().Err() }), wantValue: "99"},
		{name: "missing mode", body: `{"success":true,"data":{"markets":{"slot":42}}}`, wantValue: "99"},
		{name: "null mode", body: `{"success":true,"data":{"markets":{"slot":42,"multiRelay":null,"realtime":true}}}`, wantValue: "99"},
		{name: "unknown mode with realtime", body: `{"success":true,"data":{"markets":{"slot":42,"realtime":true}}}`, wantValue: "99"},
		{name: "single relay missing realtime", body: `{"success":true,"data":{"markets":{"slot":42,"multiRelay":false}}}`, wantValue: "99"},
		{name: "single relay null realtime", body: `{"success":true,"data":{"markets":{"slot":42,"multiRelay":false,"realtime":null}}}`, wantValue: "99"},
		{name: "multi relay missing realtime", body: `{"success":true,"data":{"markets":{"slot":42,"multiRelay":true}}}`, wantValue: "99"},
		{name: "multi relay null realtime", body: `{"success":true,"data":{"markets":{"slot":42,"multiRelay":true,"realtime":null}}}`, wantValue: "99"},
		{name: "invalid realtime type", body: `{"success":true,"data":{"markets":{"slot":42,"multiRelay":false,"realtime":"false"}}}`, wantValue: "99"},
		{name: "invalid mode type", body: `{"success":true,"data":{"markets":{"slot":42,"multiRelay":"false"}}}`, wantValue: "99"},
		{name: "absent market", body: `{"success":true,"data":{"markets":null}}`, wantValue: "99"},
		{name: "wrong slot", body: `{"success":true,"data":{"markets":{"slot":43,"multiRelay":false,"realtime":false}}}`, wantValue: "99"},
		{name: "not found", status: http.StatusNotFound, body: `{}`, wantValue: "99"},
		{name: "server error", status: http.StatusInternalServerError, body: `{}`, wantValue: "99"},
		{name: "cached unknown mode", cachedMarket: &WholeBlockMarket{Slot: 42, RealTime: &explicitTrue}, wantValue: "99"},
		{name: "cached unknown realtime", cachedMarket: &WholeBlockMarket{Slot: 42, MultiRelay: &explicitFalse}, wantValue: "99"},
		{name: "explicit single relay", body: `{"success":true,"data":{"markets":{"slot":42,"multiRelay":false,"realtime":false}}}`, wantValue: "11000000000000000000099"},
		{name: "explicit realtime single relay", body: `{"success":true,"data":{"markets":{"slot":42,"multiRelay":false,"realtime":true}}}`, wantValue: "11000000000000000000099"},
		{name: "explicit multi relay", body: `{"success":true,"data":{"markets":{"slot":42,"multiRelay":true,"realtime":false}}}`, wantValue: "99"},
		{name: "explicit realtime multi relay", body: `{"success":true,"data":{"markets":{"slot":42,"multiRelay":true,"realtime":true}}}`, wantValue: "198"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status := tc.status
			if status == 0 {
				status = http.StatusOK
			}
			transport := tc.transport
			if transport == nil {
				transport = marketModeTransport{status: status, body: tc.body}
			}
			client = &ApiClient{AccessToken: "test-token", Client: &http.Client{Transport: transport}}
			if tc.missingToken {
				client.AccessToken = ""
			}
			redisServer := miniredis.RunT(t)
			cache, err := datastore.NewRedisCache("", redisServer.Addr(), "")
			require.NoError(t, err)
			sk, pk, err := bls.GenerateNewKeypair()
			require.NoError(t, err)
			var publicKey phase0.BLSPubKey
			copy(publicKey[:], bls.PublicKeyToBytes(pk))
			parent, proposer := (phase0.Hash32{}).String(), (phase0.BLSPubKey{}).String()
			payload, getPayload, _ := common.CreateTestBlockSubmission(t, (phase0.BLSPubKey{1}).String(), uint256.NewInt(99), &common.CreateTestBlockSubmissionOpts{Slot: 42})
			bid, err := common.BuildGetHeaderResponse(payload, sk, &publicKey, phase0.Domain{})
			require.NoError(t, err)
			trace, err := payload.BidTrace()
			require.NoError(t, err)
			_, err = cache.SaveBidAndUpdateTopBid(t.Context(), cache.NewPipeline(), &common.BidTraceV2WithBlobFields{BidTrace: *trace}, payload, getPayload, bid, time.Now(), false, nil, true)
			require.NoError(t, err)
			db := &marketModeDB{saved: make(chan struct{}, 1)}
			api := &RelayAPI{log: common.TestLog, redis: cache, db: db, blsSk: sk, publicKey: &publicKey,
				genesisInfo: &beaconclient.GetGenesisResponse{Data: beaconclient.GetGenesisResponseData{
					GenesisTime: uint64(time.Now().Unix()) - 42*common.SecondsPerSlot,
				}},
			}
			if tc.cachedMarket != nil {
				api.marketCache.Store(uint64(42), &marketCacheEntry{value: tc.cachedMarket, expiration: time.Now().Add(time.Minute)})
			}
			req := mux.SetURLVars(httptest.NewRequest(http.MethodGet, "/eth/v1/builder/header/42/"+parent+"/"+proposer, nil), map[string]string{"slot": "42", "parent_hash": parent, "pubkey": proposer})
			req.Header.Set("Accept", common.ApplicationJSON)
			rr := httptest.NewRecorder()
			start := time.Now()
			api.handleGetHeader(rr, req)
			require.Less(t, time.Since(start), 750*time.Millisecond, "exchange outages must not exhaust the proposer deadline")
			require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
			var response builderSpec.VersionedSignedBuilderBid
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &response))
			require.Equal(t, tc.wantValue, response.Capella.Message.Value.Dec())
			if tc.wantValue == "99" {
				originalJSON, err := json.Marshal(bid)
				require.NoError(t, err)
				require.JSONEq(t, string(originalJSON), rr.Body.String(), "preserve the entire original signed bid")
			}
			valid, err := ssz.VerifySignature(response.Capella.Message, phase0.Domain{}, publicKey[:], response.Capella.Signature[:])
			require.NoError(t, err)
			require.True(t, valid)
			select {
			case <-db.saved:
			case <-time.After(time.Second):
				t.Fatal("header telemetry did not finish")
			}
		})
	}
}
