package api

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"bitbucket.org/infinity-exchange/mev-boost-relay/common"
	"github.com/stretchr/testify/require"
)

func newExchangePrefetchTestAPI(t *testing.T, transport http.RoundTripper) *RelayAPI {
	t.Helper()
	previousClient, previousHTTP, previousURL := client, http.DefaultClient, exchangeAPIURL
	previousDisabled, previousCutoff := disableEthgasMarketAPI, getExchangeFinalizedCutoffMs
	t.Cleanup(func() {
		client, http.DefaultClient, exchangeAPIURL = previousClient, previousHTTP, previousURL
		disableEthgasMarketAPI, getExchangeFinalizedCutoffMs = previousDisabled, previousCutoff
	})
	disableEthgasMarketAPI, getExchangeFinalizedCutoffMs = false, -2000
	exchangeAPIURL = "http://exchange-prefetch.test"
	http.DefaultClient = &http.Client{Transport: transport}
	client = &ApiClient{APIURL: exchangeAPIURL, Client: http.DefaultClient, AccessToken: "test-token"}
	api := newMarketPrefetchTestAPI()
	api.opts.ProposerAPI, api.opts.BlockBuilderAPI = false, true
	t.Cleanup(api.stopExchangePrefetch)
	return api
}

func exchangePrefetchRequest(r *http.Request) (string, uint64, error) {
	if strings.HasPrefix(r.URL.Path, "/api/v1/p/builder/") {
		slot, err := strconv.ParseUint(strings.TrimPrefix(r.URL.Path, "/api/v1/p/builder/"), 10, 64)
		return "builders", slot, err
	}
	if r.URL.Path != "/api/v1/slot/bundles" || r.Header.Get("Authorization") != "Bearer test-token" {
		return "", 0, fmt.Errorf("unexpected exchange request: %s", r.URL)
	}
	slot, err := strconv.ParseUint(r.URL.Query().Get("slot"), 10, 64)
	return "bundles", slot, err
}

func exchangePrefetchResponse(r *http.Request) (*http.Response, error) {
	endpoint, slot, err := exchangePrefetchRequest(r)
	if err != nil {
		return nil, err
	}
	var body string
	if endpoint == "builders" {
		body = fmt.Sprintf(`{"success":true,"data":{"slot":%d,"builders":[%q]}}`, slot, testBuilderPubkey)
	} else {
		body = fmt.Sprintf(`{"success":true,"data":{"slot":%d,"feeRecipient":%q,"emptySpace":21000,"bundles":[]}}`, slot, testAddress2.String())
	}
	return marketModeTransport{status: http.StatusOK, body: body}.RoundTrip(r)
}

func exchangePrefetchRoundTime(api *RelayAPI) time.Time {
	return time.Unix(int64(api.genesisInfo.Data.GenesisTime+41*common.SecondsPerSlot+1), 0)
}

func TestExchangePrefetchFinalizationBoundary(t *testing.T) {
	for _, cutoff := range []int{-4000, -2000, 500} {
		t.Run(strconv.Itoa(cutoff), func(t *testing.T) {
			var upcomingCalls atomic.Int32
			api := newExchangePrefetchTestAPI(t, exchangeTestTransport(func(r *http.Request) (*http.Response, error) {
				_, slot, err := exchangePrefetchRequest(r)
				if err != nil {
					return nil, err
				}
				if slot == 42 {
					upcomingCalls.Add(1)
				}
				return exchangePrefetchResponse(r)
			}))
			getExchangeFinalizedCutoffMs = cutoff
			finalized := time.Unix(int64(api.genesisInfo.Data.GenesisTime+42*common.SecondsPerSlot), 0).Add(time.Duration(cutoff) * time.Millisecond)
			lastResults := make(map[exchangePrefetchKey]string)
			api.prefetchExchangeWindow(context.Background(), finalized.Add(-time.Nanosecond), lastResults)
			require.Zero(t, upcomingCalls.Load(), "upcoming policy must not be cached before finalization")
			api.prefetchExchangeWindow(context.Background(), finalized, lastResults)
			require.EqualValues(t, 2, upcomingCalls.Load(), "both endpoints become eligible at the configured cutoff")
			assignment, err := api.getBuilderAssignment(context.Background(), 42)
			require.NoError(t, err)
			require.Equal(t, uint64(42), assignment.Slot)
			bundles, err := api.getSlotBundles(context.Background(), 42)
			require.NoError(t, err)
			require.Equal(t, uint64(42), *bundles.Slot)
			require.EqualValues(t, 2, upcomingCalls.Load(), "submission lookups must reuse the prefetched policy")
		})
	}
}

func TestExchangePrefetchParallelAndSharedWithSubmissions(t *testing.T) {
	started, release := make(chan string, 2), make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	var requests atomic.Int32
	api := newExchangePrefetchTestAPI(t, exchangeTestTransport(func(r *http.Request) (*http.Response, error) {
		endpoint, _, err := exchangePrefetchRequest(r)
		if err != nil {
			return nil, err
		}
		requests.Add(1)
		started <- endpoint
		select {
		case <-release:
			return exchangePrefetchResponse(r)
		case <-r.Context().Done():
			return nil, r.Context().Err()
		}
	}))
	done := make(chan struct{})
	go func() {
		api.prefetchExchangeWindow(context.Background(), exchangePrefetchRoundTime(api), make(map[exchangePrefetchKey]string))
		close(done)
	}()
	var endpoints []string
	for range 2 {
		select {
		case endpoint := <-started:
			endpoints = append(endpoints, endpoint)
		case <-time.After(time.Second):
			t.Fatal("both requests must start without waiting for the other endpoint")
		}
	}
	require.ElementsMatch(t, []string{"builders", "bundles"}, endpoints)
	var callers sync.WaitGroup
	errors := make(chan error, 40)
	for range 20 {
		callers.Add(2)
		go func() {
			defer callers.Done()
			_, err := api.getBuilderAssignment(context.Background(), 41)
			errors <- err
		}()
		go func() {
			defer callers.Done()
			bundles, err := api.getSlotBundles(context.Background(), 41)
			if err == nil && (bundles.FeeRecipient != testAddress2.String() || bundles.EmptySpace != 21000) {
				err = fmt.Errorf("prefetched submission constraints were lost")
			}
			errors <- err
		}()
	}
	releaseOnce.Do(func() { close(release) })
	callers.Wait()
	for range 40 {
		require.NoError(t, <-errors)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("prefetch did not finish")
	}
	require.EqualValues(t, 2, requests.Load(), "background and concurrent submission lookups must share their requests")
}

func TestExchangePrefetchInvalidResponseRecovery(t *testing.T) {
	var requests atomic.Int32
	var recovered atomic.Bool
	api := newExchangePrefetchTestAPI(t, exchangeTestTransport(func(r *http.Request) (*http.Response, error) {
		requests.Add(1)
		if !recovered.Load() {
			return marketModeTransport{status: http.StatusOK, body: `{"success":true,"data":{}}`}.RoundTrip(r)
		}
		return exchangePrefetchResponse(r)
	}))
	now, lastResults := exchangePrefetchRoundTime(api), make(map[exchangePrefetchKey]string)
	api.prefetchExchangeWindow(context.Background(), now, lastResults)
	require.EqualValues(t, 2, requests.Load())
	require.Empty(t, api.exchangeLookups.entries, "malformed responses must not become permissive cached policy")
	api.prefetchExchangeWindow(context.Background(), now, lastResults)
	require.EqualValues(t, 2, requests.Load(), "failure cooldown must prevent immediate retry bursts")
	recovered.Store(true)
	api.exchangeLookups.mu.Lock()
	for key, failure := range api.exchangeLookups.failures {
		failure.expiration = time.Now().Add(-time.Second)
		api.exchangeLookups.failures[key] = failure
	}
	api.exchangeLookups.mu.Unlock()
	api.prefetchExchangeWindow(context.Background(), now, lastResults)
	require.EqualValues(t, 4, requests.Load())
	_, err := api.getBuilderAssignment(context.Background(), 41)
	require.NoError(t, err)
	_, err = api.getSlotBundles(context.Background(), 41)
	require.NoError(t, err)
	require.EqualValues(t, 4, requests.Load())
}

func TestExchangePrefetchRespectsSharedRateLimit(t *testing.T) {
	var requests atomic.Int32
	api := newExchangePrefetchTestAPI(t, exchangeTestTransport(func(r *http.Request) (*http.Response, error) {
		requests.Add(1)
		response, err := (marketModeTransport{status: http.StatusTooManyRequests, body: `{}`}).RoundTrip(r)
		response.Header.Set("Retry-After", "60")
		return response, err
	}))
	_, err := api.getBuilderAssignment(context.Background(), 40)
	require.ErrorContains(t, err, "HTTP 429")
	api.prefetchExchangeWindow(context.Background(), exchangePrefetchRoundTime(api), make(map[exchangePrefetchKey]string))
	require.EqualValues(t, 1, requests.Load(), "429 must suppress both endpoints and other slots for the same API")
	_, err = api.getSlotBundles(context.Background(), 41)
	require.ErrorContains(t, err, "cooling down")
	require.EqualValues(t, 1, requests.Load())
}

func TestExchangePrefetchWorkerLifecycle(t *testing.T) {
	var requests atomic.Int32
	api := newExchangePrefetchTestAPI(t, exchangeTestTransport(func(r *http.Request) (*http.Response, error) {
		requests.Add(1)
		return exchangePrefetchResponse(r)
	}))
	api.startExchangePrefetch()
	api.startExchangePrefetch()
	require.EqualValues(t, 2, requests.Load(), "builder-only instances warm both endpoints before any submission")
	_, err := api.getBuilderAssignment(context.Background(), 41)
	require.NoError(t, err)
	_, err = api.getSlotBundles(context.Background(), 41)
	require.NoError(t, err)
	require.EqualValues(t, 2, requests.Load())
	api.stopExchangePrefetch()
	api.stopExchangePrefetch()
	select {
	case <-api.exchangePrefetchDone:
	default:
		t.Fatal("shutdown must drain the worker")
	}
}

func TestExchangePrefetchWorkerUsesWallClock(t *testing.T) {
	var nextSlotRequests atomic.Int32
	var finalizedAt time.Time
	api := newExchangePrefetchTestAPI(t, exchangeTestTransport(func(r *http.Request) (*http.Response, error) {
		_, slot, err := exchangePrefetchRequest(r)
		if err != nil {
			return nil, err
		}
		if slot == 42 {
			if time.Now().Before(finalizedAt) {
				t.Error("worker fetched the upcoming policy before finalization")
			}
			nextSlotRequests.Add(1)
		}
		return exchangePrefetchResponse(r)
	}))
	// The next slot finalizes in one to two seconds. Beacon head events
	// deliberately never advance, so only the independent timer can warm it.
	api.genesisInfo.Data.GenesisTime = uint64(time.Now().Unix()) - 42*common.SecondsPerSlot + 4
	finalizedAt = time.Unix(int64(api.genesisInfo.Data.GenesisTime+42*common.SecondsPerSlot), 0).Add(time.Duration(getExchangeFinalizedCutoffMs) * time.Millisecond)
	api.startExchangePrefetch()
	require.Zero(t, nextSlotRequests.Load())
	require.Eventually(t, func() bool { return nextSlotRequests.Load() == 2 }, 4*time.Second, 10*time.Millisecond)
	api.stopExchangePrefetch()
	require.Less(t, uint64(time.Now().Unix()), api.genesisInfo.Data.GenesisTime+42*common.SecondsPerSlot, "both endpoints must be warm before the proposal slot begins")
	_, err := api.getBuilderAssignment(context.Background(), 42)
	require.NoError(t, err)
	_, err = api.getSlotBundles(context.Background(), 42)
	require.NoError(t, err)
	require.EqualValues(t, 2, nextSlotRequests.Load())
}

func TestExchangePrefetchIndependentOfMarketPolling(t *testing.T) {
	marketStarted := make(chan struct{}, 2)
	var constraints atomic.Int32
	api := newExchangePrefetchTestAPI(t, exchangeTestTransport(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/api/v1/p/wholeblock/market" {
			marketStarted <- struct{}{}
			<-r.Context().Done()
			return nil, r.Context().Err()
		}
		constraints.Add(1)
		return exchangePrefetchResponse(r)
	}))
	api.opts.ProposerAPI = true
	t.Cleanup(api.stopMarketPrefetch)
	marketWarmup := make(chan struct{})
	go func() { api.startMarketPrefetch(); close(marketWarmup) }()
	for range 2 {
		select {
		case <-marketStarted:
		case <-time.After(time.Second):
			t.Fatal("market polling did not start")
		}
	}
	api.startExchangePrefetch()
	require.EqualValues(t, 2, constraints.Load(), "a stalled market API must not hold up finalized policy warmup")
	api.stopMarketPrefetch()
	<-marketWarmup
}

func TestExchangePrefetchDisabled(t *testing.T) {
	for _, name := range []string{"exchange disabled", "proposer only", "missing genesis", "shutdown"} {
		t.Run(name, func(t *testing.T) {
			var requests atomic.Int32
			api := newExchangePrefetchTestAPI(t, exchangeTestTransport(func(r *http.Request) (*http.Response, error) {
				requests.Add(1)
				return exchangePrefetchResponse(r)
			}))
			switch name {
			case "exchange disabled":
				disableEthgasMarketAPI = true
			case "proposer only":
				api.opts.BlockBuilderAPI, api.opts.ProposerAPI = false, true
			case "missing genesis":
				api.genesisInfo = nil
			case "shutdown":
				api.srvShutdown.Store(true)
			}
			api.startExchangePrefetch()
			require.Nil(t, api.exchangePrefetchDone)
			require.Zero(t, requests.Load())
		})
	}
}

func TestExchangePrefetchShutdownDrainsSharedFetches(t *testing.T) {
	started := make(chan struct{}, 2)
	var requests, completed atomic.Int32
	api := newExchangePrefetchTestAPI(t, exchangeTestTransport(func(r *http.Request) (*http.Response, error) {
		requests.Add(1)
		started <- struct{}{}
		<-r.Context().Done()
		completed.Add(1)
		return nil, r.Context().Err()
	}))
	warmup := make(chan struct{})
	go func() { api.startExchangePrefetch(); close(warmup) }()
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("warmup did not start both endpoint requests")
		}
	}
	stopped := make(chan struct{})
	go func() { api.stopExchangePrefetch(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("shutdown exceeded the bounded shared lookup deadline")
	}
	<-warmup
	require.EqualValues(t, 2, completed.Load(), "shutdown must finish active shared fetches before returning")
	require.EqualValues(t, 2, requests.Load(), "shutdown must prevent another prefetch round")
}
