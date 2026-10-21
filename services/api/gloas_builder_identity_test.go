package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"bitbucket.org/infinity-exchange/mev-boost-relay/beaconclient"
	"bitbucket.org/infinity-exchange/mev-boost-relay/common"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/stretchr/testify/require"
)

type identityTestBeacon struct {
	beaconclient.IMultiBeaconClient
	fetch func(context.Context, string, phase0.BLSPubKey) (*beaconclient.GetStateBuildersResponse, error)
}

func (b *identityTestBeacon) GetStateBuilders(ctx context.Context, stateID string, pubkey phase0.BLSPubKey) (*beaconclient.GetStateBuildersResponse, error) {
	return b.fetch(ctx, stateID, pubkey)
}

func identityRegistryResponse(t *testing.T, api *RelayAPI, index uint64, status string) *beaconclient.GetStateBuildersResponse {
	t.Helper()
	encoded := fmt.Sprintf(`{"execution_optimistic":false,"finalized":false,"data":[{"index":"%d","status":%q,"builder":{"pubkey":%q,"version":"0","execution_address":"0x0000000000000000000000000000000000000000","balance":"1","deposit_epoch":"1","withdrawable_epoch":"18446744073709551615"}}]}`,
		index, status, api.publicKey.String())
	response := new(beaconclient.GetStateBuildersResponse)
	require.NoError(t, json.Unmarshal([]byte(encoded), response))
	return response
}

func TestGloasBuilderIdentityRegistryChecks(t *testing.T) {
	for _, name := range []string{"auto_index_zero", "explicit_index", "wrong_index", "pending", "exited", "exit_initiated", "wrong_pubkey", "wrong_version", "self_build_index", "optimistic", "not_registered", "duplicate", "nil_response", "lookup_error", "unsupported_client"} {
		t.Run(name, func(t *testing.T) {
			backend := newTestBackend(t, 1)
			api := backend.relay
			response := identityRegistryResponse(t, api, 0, "active")
			var lookupErr error
			expectedActive := false
			switch name {
			case "auto_index_zero":
				expectedActive = true
			case "explicit_index":
				index := uint64(0)
				api.opts.GloasBuilderIndex = &index
				expectedActive = true
			case "wrong_index":
				index := uint64(7)
				api.opts.GloasBuilderIndex = &index
			case "pending", "exited":
				response.Data[0].Status = name
			case "exit_initiated":
				response.Data[0].Builder.WithdrawableEpoch = 7
			case "wrong_pubkey":
				response.Data[0].Builder.Pubkey = phase0.BLSPubKey{1}
			case "wrong_version":
				response.Data[0].Builder.Version = 1
			case "self_build_index":
				response.Data[0].Index = math.MaxUint64
			case "optimistic":
				response.ExecutionOptimistic = true
			case "not_registered":
				response.Data = nil
			case "duplicate":
				response.Data = append(response.Data, response.Data[0])
			case "nil_response":
				response = nil
			case "lookup_error":
				lookupErr = errors.New("beacon unavailable")
			}
			calls := 0
			base := api.beaconClient
			api.beaconClient = &identityTestBeacon{IMultiBeaconClient: base, fetch: func(_ context.Context, stateID string, pubkey phase0.BLSPubKey) (*beaconclient.GetStateBuildersResponse, error) {
				calls++
				require.Equal(t, beaconclient.StateIDHead, stateID)
				require.Equal(t, *api.publicKey, pubkey)
				return response, lookupErr
			}}
			if name == "unsupported_client" {
				api.beaconClient = beaconclient.NewMockMultiBeaconClient()
			}
			api.setGloasBuilderIdentity(8, true, "active")
			err := api.refreshGloasBuilderIdentity(context.Background())
			identity, active := api.currentGloasBuilderIdentity()
			require.Equal(t, expectedActive, active)
			if expectedActive {
				require.NoError(t, err)
				require.Zero(t, identity.index)
			} else {
				require.Error(t, err)
			}
			if name != "unsupported_client" {
				require.Equal(t, 1, calls)
			}
		})
	}
}

func TestGloasBuilderIdentityTransitionsAndStaleness(t *testing.T) {
	api := newTestBackend(t, 1).relay
	response := identityRegistryResponse(t, api, 0, "active")
	var lookupErr error
	var calls atomic.Int32
	api.beaconClient = &identityTestBeacon{IMultiBeaconClient: api.beaconClient, fetch: func(context.Context, string, phase0.BLSPubKey) (*beaconclient.GetStateBuildersResponse, error) {
		calls.Add(1)
		return response, lookupErr
	}}
	require.NoError(t, api.refreshGloasBuilderIdentity(context.Background()))
	original, active := api.currentGloasBuilderIdentity()
	require.True(t, active)
	require.NoError(t, api.refreshGloasBuilderIdentity(context.Background()))
	require.True(t, api.gloasBuilderIdentityStillCurrent(original), "healthy refresh must not invalidate an in-flight bid")
	lookupErr = errors.New("temporary outage")
	require.Error(t, api.refreshGloasBuilderIdentity(context.Background()))
	require.False(t, api.gloasBuilderIdentityStillCurrent(original))
	lookupErr = nil
	response = identityRegistryResponse(t, api, 9, "active")
	require.NoError(t, api.refreshGloasBuilderIdentity(context.Background()))
	current, active := api.currentGloasBuilderIdentity()
	require.True(t, active)
	require.Equal(t, uint64(9), current.index, "registry indices can be reused")
	for range 10 {
		_, active = api.currentGloasBuilderIdentity()
		require.True(t, active)
	}
	require.Equal(t, int32(4), calls.Load(), "bid-time reads must not perform registry HTTP calls")
	api.gloasBuilderIdentityMu.Lock()
	api.gloasBuilderIdentity.checkedAt = time.Now().Add(-gloasBuilderIdentityMaxAge)
	api.gloasBuilderIdentityMu.Unlock()
	_, active = api.currentGloasBuilderIdentity()
	require.False(t, active, "stale verification must fail closed")
	require.NoError(t, api.refreshGloasBuilderIdentity(context.Background()))
	api.srvShutdown.Store(true)
	_, active = api.currentGloasBuilderIdentity()
	require.False(t, active, "shutdown must stop new bids")
}

func TestGloasBuilderIdentityUpdatesCancel(t *testing.T) {
	api := newTestBackend(t, 1).relay
	started := make(chan struct{})
	api.beaconClient = &identityTestBeacon{IMultiBeaconClient: api.beaconClient, fetch: func(ctx context.Context, _ string, _ phase0.BLSPubKey) (*beaconclient.GetStateBuildersResponse, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	done := make(chan struct{})
	go func() {
		api.startGloasBuilderIdentityUpdates()
		api.gloasBuilderIdentityWG.Wait()
		close(done)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("registry lookup did not start")
	}
	api.stopGloasBuilderIdentityUpdates()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancelled registry lookup did not stop")
	}
}

func TestGloasBuilderIdentityConcurrentRefresh(t *testing.T) {
	api := newTestBackend(t, 1).relay
	response := identityRegistryResponse(t, api, 7, "active")
	api.beaconClient = &identityTestBeacon{IMultiBeaconClient: api.beaconClient, fetch: func(context.Context, string, phase0.BLSPubKey) (*beaconclient.GetStateBuildersResponse, error) {
		return response, nil
	}}
	var workers sync.WaitGroup
	for range 4 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for range 50 {
				_ = api.refreshGloasBuilderIdentity(context.Background())
				_, _ = api.currentGloasBuilderIdentity()
			}
		}()
	}
	workers.Wait()
	_, active := api.currentGloasBuilderIdentity()
	require.True(t, active)
}

func TestGloasBuilderIdentityGatesCachedBids(t *testing.T) {
	f := newGloasExchangeBidFixture(t, testAddress, nil)
	api := f.backend.relay
	api.setGloasBuilderIdentity(7, false, "pending")
	rr := f.request(t, ApplicationJSON)
	require.Equal(t, http.StatusNoContent, rr.Code, rr.Body.String())
	api.setGloasBuilderIdentity(7, true, "active")
	rr = f.request(t, ApplicationJSON)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
}

func TestGloasBuilderIdentityBeaconHTTPIntegration(t *testing.T) {
	f := newGloasExchangeBidFixture(t, testAddress, nil)
	api := f.backend.relay
	api.opts.GloasBuilderIndex = nil
	api.setGloasBuilderIdentity(0, false, "unverified")
	var pending atomic.Bool
	var lookups atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/eth/v1/beacon/genesis":
			_, _ = w.Write([]byte(`{"data":{"genesis_time":"0","genesis_validators_root":"0x0000000000000000000000000000000000000000000000000000000000000000","genesis_fork_version":"0x00000000"}}`))
		case "/eth/v1/node/syncing":
			_, _ = w.Write([]byte(`{"data":{"head_slot":"42","is_syncing":false,"is_optimistic":false,"el_offline":false}}`))
		case "/eth/v1/beacon/states/head/builders":
			if r.Method != http.MethodPost {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			var request struct {
				IDs []string `json:"ids"`
			}
			if json.NewDecoder(r.Body).Decode(&request) != nil || len(request.IDs) != 1 || request.IDs[0] != api.publicKey.String() {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			lookups.Add(1)
			status := "active"
			if pending.Load() {
				status = "pending"
			}
			_, _ = fmt.Fprintf(w, `{"execution_optimistic":false,"finalized":false,"data":[{"index":"0","status":%q,"builder":{"pubkey":%q,"version":"0","execution_address":"0x0000000000000000000000000000000000000000","balance":"1","deposit_epoch":"1","withdrawable_epoch":"18446744073709551615"}}]}`, status, api.publicKey.String())
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	instance := beaconclient.NewProdBeaconInstance(api.log, server.URL, server.URL)
	api.beaconClient = beaconclient.NewMultiBeaconClient(api.log, []beaconclient.IBeaconInstance{instance})
	require.Equal(t, http.StatusNoContent, f.request(t, ApplicationJSON).Code)
	require.NoError(t, api.refreshGloasBuilderIdentity(t.Context()))
	for _, encoding := range []string{ApplicationJSON, "application/octet-stream"} {
		response := f.request(t, encoding)
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		bid := new(common.SignedExecutionPayloadBid)
		if encoding == ApplicationJSON {
			var wrapped common.VersionedSignedExecutionPayloadBidResponse
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &wrapped))
			bid = wrapped.Data
		} else {
			require.NoError(t, bid.UnmarshalSSZ(response.Body.Bytes()))
		}
		require.Zero(t, bid.Message.BuilderIndex)
	}
	require.Equal(t, int32(1), lookups.Load(), "bid requests must use cached verification")
	pending.Store(true)
	require.Error(t, api.refreshGloasBuilderIdentity(t.Context()))
	require.Equal(t, http.StatusNoContent, f.request(t, ApplicationJSON).Code)
}
