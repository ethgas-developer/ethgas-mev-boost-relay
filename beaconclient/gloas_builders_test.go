package beaconclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"bitbucket.org/infinity-exchange/mev-boost-relay/common"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/stretchr/testify/require"
)

func registryTestPubkey(t *testing.T) phase0.BLSPubKey {
	t.Helper()
	var pubkey phase0.BLSPubKey
	require.NoError(t, json.Unmarshal([]byte(`"`+testPubKey+`"`), &pubkey))
	return pubkey
}

func registryResponseJSON() string {
	return fmt.Sprintf(`{"execution_optimistic":false,"finalized":false,"data":[{"index":"0","status":"active","builder":{"pubkey":%q,"version":"0","execution_address":"0x0000000000000000000000000000000000000000","balance":"50000000000","deposit_epoch":"0","withdrawable_epoch":"18446744073709551615"}}]}`, testPubKey)
}

func registryTestInstance(t *testing.T, handler http.HandlerFunc) *ProdBeaconInstance {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return &ProdBeaconInstance{beaconURI: server.URL, log: common.TestLog}
}

func TestGetStateBuildersRequestAndDecode(t *testing.T) {
	pubkey := registryTestPubkey(t)
	instance := registryTestInstance(t, func(w http.ResponseWriter, req *http.Request) {
		require.Equal(t, http.MethodPost, req.Method)
		require.Equal(t, "/eth/v1/beacon/states/head/builders", req.URL.Path)
		require.Empty(t, req.URL.RawQuery)
		require.Equal(t, "application/json", req.Header.Get("Accept"))
		require.Equal(t, "application/json", req.Header.Get("Content-Type"))
		body, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		require.JSONEq(t, `{"ids":["`+testPubKey+`"]}`, string(body))
		_, err = io.WriteString(w, registryResponseJSON())
		require.NoError(t, err)
	})
	response, err := instance.GetStateBuilders(context.Background(), "head", pubkey)
	require.NoError(t, err)
	require.False(t, response.ExecutionOptimistic)
	require.False(t, response.Finalized)
	require.Len(t, response.Data, 1)
	entry := response.Data[0]
	require.Zero(t, entry.Index)
	require.Equal(t, "active", entry.Status)
	require.Equal(t, pubkey, entry.Builder.Pubkey)
	require.Zero(t, entry.Builder.Version)
	require.Equal(t, uint64(50_000_000_000), entry.Builder.Balance)
	require.Zero(t, entry.Builder.DepositEpoch)
	require.Equal(t, ^uint64(0), entry.Builder.WithdrawableEpoch)
}

func TestGetStateBuildersEmptyIsNotFound(t *testing.T) {
	instance := registryTestInstance(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"execution_optimistic":false,"finalized":true,"data":[]}`)
	})
	response, err := instance.GetStateBuilders(context.Background(), "finalized", registryTestPubkey(t))
	require.NoError(t, err)
	require.NotNil(t, response.Data)
	require.Empty(t, response.Data)
	require.True(t, response.Finalized)
}

func TestGetStateBuildersPreservesInactiveStatusesAndZeroBalance(t *testing.T) {
	for _, status := range []string{"pending", "active", "exited"} {
		t.Run(status, func(t *testing.T) {
			body := strings.Replace(registryResponseJSON(), `"status":"active"`, `"status":"`+status+`"`, 1)
			body = strings.Replace(body, `"balance":"50000000000"`, `"balance":"0"`, 1)
			response, err := decodeBuilderRegistryResponse([]byte(body), registryTestPubkey(t))
			require.NoError(t, err)
			require.Equal(t, status, response.Data[0].Status)
			require.Zero(t, response.Data[0].Builder.Balance)
		})
	}
}

func TestGetStateBuildersRejectsMalformedAndUnsafeResponses(t *testing.T) {
	valid := registryResponseJSON()
	tests := map[string]string{
		"invalid JSON":          `not JSON`,
		"trailing JSON":         valid + `{}`,
		"null":                  `null`,
		"missing metadata":      `{"data":[]}`,
		"null data":             `{"execution_optimistic":false,"finalized":false,"data":null}`,
		"missing index":         strings.Replace(valid, `"index":"0",`, "", 1),
		"numeric index":         strings.Replace(valid, `"index":"0"`, `"index":0`, 1),
		"index overflow":        strings.Replace(valid, `"index":"0"`, `"index":"18446744073709551616"`, 1),
		"negative balance":      strings.Replace(valid, `"balance":"50000000000"`, `"balance":"-1"`, 1),
		"missing epoch":         strings.Replace(valid, `"deposit_epoch":"0",`, "", 1),
		"version overflow":      strings.Replace(valid, `"version":"0"`, `"version":"256"`, 1),
		"missing version":       strings.Replace(valid, `"version":"0",`, "", 1),
		"unknown status":        strings.Replace(valid, `"status":"active"`, `"status":"unknown"`, 1),
		"null builder":          `{"execution_optimistic":false,"finalized":false,"data":[{"index":"0","status":"active","builder":null}]}`,
		"malformed public key":  strings.Replace(valid, testPubKey, "0x01", 1),
		"unexpected public key": strings.Replace(valid, testPubKey, "0x"+strings.Repeat("11", 48), 1),
		"malformed address":     strings.Replace(valid, "0x0000000000000000000000000000000000000000", "0x01", 1),
		"duplicate entries":     strings.Replace(valid, `"data":[`, `"data":[{},`, 1),
		"oversized":             strings.Repeat(" ", maxBuilderRegistryResponse+1),
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			instance := registryTestInstance(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, body) })
			response, err := instance.GetStateBuilders(context.Background(), "head", registryTestPubkey(t))
			require.Error(t, err)
			require.Nil(t, response)
		})
	}
	t.Run("optimistic", func(t *testing.T) {
		instance := registryTestInstance(t, func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, strings.Replace(valid, `"execution_optimistic":false`, `"execution_optimistic":true`, 1))
		})
		_, err := instance.GetStateBuilders(context.Background(), "head", registryTestPubkey(t))
		require.ErrorIs(t, err, ErrBuilderRegistryOptimistic)
	})
}

func TestGetStateBuildersStateIDsAndZeroKey(t *testing.T) {
	for _, stateID := range []string{"head", "genesis", "finalized", "justified", "0", "18446744073709551615", "0x" + strings.Repeat("aa", 32)} {
		require.NoError(t, validateBuilderStateID(stateID), stateID)
	}
	instance := &ProdBeaconInstance{beaconURI: "http://must-not-be-called.invalid"}
	for _, stateID := range []string{"", "../head", "head?ids=all", "head/builders", "head#fragment", "%68ead", "-1", "+1", "18446744073709551616", "0x01", "0x" + strings.Repeat("zz", 32)} {
		_, err := instance.GetStateBuilders(context.Background(), stateID, registryTestPubkey(t))
		require.ErrorContains(t, err, "state ID", stateID)
	}
	_, err := instance.GetStateBuilders(context.Background(), "head", phase0.BLSPubKey{})
	require.ErrorContains(t, err, "public key is zero")
}

func TestGetStateBuildersHTTPAndContext(t *testing.T) {
	for _, code := range []int{http.StatusBadRequest, http.StatusNotFound, http.StatusInternalServerError, http.StatusFound, http.StatusNoContent} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			instance := registryTestInstance(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code) })
			_, err := instance.GetStateBuilders(context.Background(), "head", registryTestPubkey(t))
			require.ErrorIs(t, err, ErrHTTPErrorResponse)
			require.ErrorContains(t, err, fmt.Sprint(code))
		})
	}
	t.Run("caller deadline cancels body read", func(t *testing.T) {
		instance := registryTestInstance(t, func(w http.ResponseWriter, req *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			<-req.Context().Done()
		})
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		_, err := instance.GetStateBuilders(ctx, "head", registryTestPubkey(t))
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
	t.Run("cancelled before request", func(t *testing.T) {
		instance := registryTestInstance(t, func(_ http.ResponseWriter, _ *http.Request) { t.Error("cancelled request reached server") })
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := instance.GetStateBuilders(ctx, "head", registryTestPubkey(t))
		require.ErrorIs(t, err, context.Canceled)
	})
}

type registryMockInstance struct {
	*MockBeaconInstance
	uri      string
	response *GetStateBuildersResponse
	err      error
	calls    int
	syncFn   func(context.Context) (*SyncStatusPayloadData, error)
	readFn   func(context.Context) (*GetStateBuildersResponse, error)
}

func (m *registryMockInstance) GetURI() string { return m.uri }

func (m *registryMockInstance) BuilderRegistrySyncStatus(ctx context.Context) (*SyncStatusPayloadData, error) {
	if m.syncFn != nil {
		return m.syncFn(ctx)
	}
	return m.MockSyncStatus, m.MockSyncStatusErr
}

func (m *registryMockInstance) GetStateBuilders(ctx context.Context, _ string, _ phase0.BLSPubKey) (*GetStateBuildersResponse, error) {
	m.calls++
	if m.readFn != nil {
		return m.readFn(ctx)
	}
	return m.response, m.err
}

func TestMultiBeaconGetStateBuildersFallbackAndOrdering(t *testing.T) {
	pubkey := registryTestPubkey(t)
	success := &GetStateBuildersResponse{Data: []BuilderResponseEntry{{Index: 7, Status: "active", Builder: BuilderResponseBuilderData{Pubkey: pubkey}}}}
	bad := &registryMockInstance{MockBeaconInstance: NewMockBeaconInstance(), uri: "bad", err: errors.New("bad response")}
	good := &registryMockInstance{MockBeaconInstance: NewMockBeaconInstance(), uri: "good", response: success}
	client := NewMultiBeaconClient(common.TestLog, []IBeaconInstance{NewMockBeaconInstance(), bad, good})
	client.bestBeaconIndex.Store(1)
	response, err := client.GetStateBuilders(context.Background(), "head", pubkey)
	require.NoError(t, err)
	require.Same(t, success, response)
	require.Equal(t, 1, bad.calls)
	require.Equal(t, 1, good.calls)
	require.Equal(t, int64(2), client.bestBeaconIndex.Load(), "store the original index, not the reordered loop offset")
	_, err = client.GetStateBuilders(context.Background(), "head", pubkey)
	require.NoError(t, err)
	require.Equal(t, 1, bad.calls)
	require.Equal(t, 2, good.calls)
}

func TestMultiBeaconGetStateBuildersEmptyDoesNotFallBack(t *testing.T) {
	empty := &registryMockInstance{MockBeaconInstance: NewMockBeaconInstance(), response: &GetStateBuildersResponse{Data: []BuilderResponseEntry{}}}
	other := &registryMockInstance{MockBeaconInstance: NewMockBeaconInstance(), err: errors.New("must not be called")}
	client := NewMultiBeaconClient(common.TestLog, []IBeaconInstance{empty, other})
	response, err := client.GetStateBuilders(context.Background(), "head", registryTestPubkey(t))
	require.NoError(t, err)
	require.Empty(t, response.Data)
	require.Zero(t, other.calls)
}

func TestMultiBeaconGetStateBuildersRejectsUnsafeAndUnavailable(t *testing.T) {
	for _, name := range []string{"syncing", "sync error", "nil status", "optimistic registry", "nil registry"} {
		t.Run(name, func(t *testing.T) {
			first := &registryMockInstance{MockBeaconInstance: NewMockBeaconInstance(), response: &GetStateBuildersResponse{}}
			switch name {
			case "syncing":
				first.MockSyncStatus.IsSyncing = true
			case "sync error":
				first.MockSyncStatusErr = errors.New("unavailable")
			case "nil status":
				first.MockSyncStatus = nil
			case "optimistic registry":
				first.response.ExecutionOptimistic = true
			case "nil registry":
				first.response = nil
			}
			second := &registryMockInstance{MockBeaconInstance: NewMockBeaconInstance(), response: &GetStateBuildersResponse{Data: []BuilderResponseEntry{}}}
			client := NewMultiBeaconClient(common.TestLog, []IBeaconInstance{first, second})
			client.ffAllowSyncingBeaconNode = true
			response, err := client.GetStateBuilders(context.Background(), "head", registryTestPubkey(t))
			require.NoError(t, err)
			require.Same(t, second.response, response)
			require.Equal(t, int64(1), client.bestBeaconIndex.Load())
		})
	}
	t.Run("unsupported", func(t *testing.T) {
		client := NewMultiBeaconClient(common.TestLog, []IBeaconInstance{NewMockBeaconInstance()})
		_, err := client.GetStateBuilders(context.Background(), "head", registryTestPubkey(t))
		require.ErrorIs(t, err, ErrBuilderRegistryUnsupported)
	})
	t.Run("all fail", func(t *testing.T) {
		cause := errors.New("registry unavailable")
		bad := &registryMockInstance{MockBeaconInstance: NewMockBeaconInstance(), err: cause}
		client := NewMultiBeaconClient(common.TestLog, []IBeaconInstance{bad})
		_, err := client.GetStateBuilders(context.Background(), "head", registryTestPubkey(t))
		require.ErrorIs(t, err, ErrBeaconNodesUnavailable)
		require.ErrorIs(t, err, cause)
	})
	t.Run("deadline", func(t *testing.T) {
		bad := &registryMockInstance{MockBeaconInstance: NewMockBeaconInstance(), syncFn: func(ctx context.Context) (*SyncStatusPayloadData, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		}}
		client := NewMultiBeaconClient(common.TestLog, []IBeaconInstance{bad})
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		_, err := client.GetStateBuilders(ctx, "head", registryTestPubkey(t))
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Zero(t, bad.calls)
	})
}

func TestMultiBeaconGetStateBuildersProductionSyncChecks(t *testing.T) {
	for _, syncJSON := range []string{
		`{"data":{"head_slot":"1","is_syncing":true,"is_optimistic":false}}`,
		`{"data":{"head_slot":"1","is_syncing":false,"is_optimistic":true}}`,
		`{"data":{"head_slot":"1","is_syncing":false,"is_optimistic":false,"el_offline":true}}`,
		`{"data":{"head_slot":"1","is_syncing":false}}`,
		`not JSON`,
	} {
		t.Run(syncJSON, func(t *testing.T) {
			unsafe := registryTestInstance(t, func(w http.ResponseWriter, req *http.Request) {
				require.Equal(t, "/eth/v1/node/syncing", req.URL.Path, "unsafe sync status must not reach registry")
				_, _ = io.WriteString(w, syncJSON)
			})
			safe := registryTestInstance(t, func(w http.ResponseWriter, req *http.Request) {
				if req.URL.Path == "/eth/v1/node/syncing" {
					_, _ = io.WriteString(w, `{"data":{"head_slot":"2","is_syncing":false,"is_optimistic":false}}`)
					return
				}
				require.Equal(t, http.MethodPost, req.Method)
				_, _ = io.WriteString(w, registryResponseJSON())
			})
			client := NewMultiBeaconClient(common.TestLog, []IBeaconInstance{unsafe, safe})
			response, err := client.GetStateBuilders(context.Background(), "head", registryTestPubkey(t))
			require.NoError(t, err)
			require.Len(t, response.Data, 1)
			require.Equal(t, int64(1), client.bestBeaconIndex.Load())
		})
	}
}

func TestMultiBeaconGetStateBuildersFallsBackAfterTimedOutBody(t *testing.T) {
	slow := registryTestInstance(t, func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/eth/v1/node/syncing" {
			_, _ = io.WriteString(w, `{"data":{"head_slot":"1","is_syncing":false,"is_optimistic":false}}`)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-req.Context().Done()
	})
	safe := &registryMockInstance{MockBeaconInstance: NewMockBeaconInstance(), response: &GetStateBuildersResponse{Data: []BuilderResponseEntry{}}}
	client := NewMultiBeaconClient(common.TestLog, []IBeaconInstance{slow, safe})
	response, err := client.GetStateBuilders(context.Background(), "head", registryTestPubkey(t))
	require.NoError(t, err)
	require.Same(t, safe.response, response)
	require.Equal(t, int64(1), client.bestBeaconIndex.Load())
}

type registryUnboundedSyncMock struct {
	*MockBeaconInstance
	calls int
}

func (m *registryUnboundedSyncMock) GetStateBuilders(context.Context, string, phase0.BLSPubKey) (*GetStateBuildersResponse, error) {
	m.calls++
	return &GetStateBuildersResponse{}, nil
}

func TestMultiBeaconGetStateBuildersRequiresBoundedCustomSync(t *testing.T) {
	instance := &registryUnboundedSyncMock{MockBeaconInstance: NewMockBeaconInstance()}
	client := NewMultiBeaconClient(common.TestLog, []IBeaconInstance{instance})
	_, err := client.GetStateBuilders(context.Background(), "head", registryTestPubkey(t))
	require.ErrorIs(t, err, ErrBeaconNodesUnavailable)
	require.ErrorContains(t, err, "context-bounded")
	require.Zero(t, instance.calls)
}
