package api

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"bitbucket.org/infinity-exchange/mev-boost-relay/beaconclient"
	"bitbucket.org/infinity-exchange/mev-boost-relay/common"
	"github.com/stretchr/testify/require"
)

func newMarketPrefetchTestAPI() *RelayAPI {
	return &RelayAPI{
		log:  common.TestLog,
		opts: RelayAPIOpts{ProposerAPI: true},
		genesisInfo: &beaconclient.GetGenesisResponse{Data: beaconclient.GetGenesisResponseData{
			GenesisTime: uint64(time.Now().Unix()) - 41*common.SecondsPerSlot,
		}},
	}
}

func marketPrefetchResponse(r *http.Request, realtime bool) (*http.Response, error) {
	return marketModeTransport{status: http.StatusOK, body: fmt.Sprintf(
		`{"success":true,"data":{"markets":{"slot":%s,"multiRelay":true,"realtime":%t}}}`,
		r.URL.Query().Get("slot"), realtime,
	)}.RoundTrip(r)
}

func TestMarketPrefetchRetriesBeforeProposal(t *testing.T) {
	for _, failure := range []string{"absent", "http error", "invalid mode", "timeout", "no token"} {
		t.Run(failure, func(t *testing.T) {
			oldClient, oldDisabled := client, disableEthgasMarketAPI
			defer func() { client, disableEthgasMarketAPI = oldClient, oldDisabled }()
			disableEthgasMarketAPI = false
			var recovered atomic.Bool
			var requests atomic.Int32
			client = &ApiClient{AccessToken: "test-token", Client: &http.Client{Transport: exchangeTestTransport(func(r *http.Request) (*http.Response, error) {
				requests.Add(1)
				if !recovered.Load() {
					switch failure {
					case "absent":
						return marketModeTransport{status: http.StatusOK, body: `{"success":true,"data":{"markets":null}}`}.RoundTrip(r)
					case "http error":
						return marketModeTransport{status: http.StatusServiceUnavailable, body: "unavailable"}.RoundTrip(r)
					case "invalid mode":
						return marketModeTransport{status: http.StatusOK, body: fmt.Sprintf(`{"success":true,"data":{"markets":{"slot":%s,"multiRelay":true}}}`, r.URL.Query().Get("slot"))}.RoundTrip(r)
					case "timeout":
						<-r.Context().Done()
						return nil, r.Context().Err()
					}
				}
				// Longer than the original 200ms deadline, still before proposal.
				select {
				case <-time.After(300 * time.Millisecond):
					return marketPrefetchResponse(r, true)
				case <-r.Context().Done():
					return nil, r.Context().Err()
				}
			})}}
			if failure == "no token" {
				client.setTokens("", "")
			}
			api := newMarketPrefetchTestAPI()
			var readers sync.WaitGroup
			for range 50 {
				readers.Add(1)
				go func() {
					defer readers.Done()
					_ = api.cachedMarketForSlot(42)
				}()
			}
			readers.Wait()
			require.Zero(t, requests.Load(), "cache misses must never initiate market requests")
			api.startMarketPrefetch()
			defer api.stopMarketPrefetch()
			_, cached := api.marketCache.Load(uint64(42))
			require.False(t, cached, "a failed attempt must not create a negative cache entry")
			recovered.Store(true)
			client.setTokens("recovered-token", "")
			require.Eventually(t, func() bool {
				market := api.cachedMarketForSlot(42)
				return market != nil && market.RealTime != nil && *market.RealTime
			}, 5*time.Second, 10*time.Millisecond, "retry failure before the proposal, instead of caching it for 12 seconds")
			require.Less(t, uint64(time.Now().Unix()), api.genesisInfo.Data.GenesisTime+42*common.SecondsPerSlot)
		})
	}
}

func TestMarketPrefetchRefreshAndSlotRollover(t *testing.T) {
	oldClient, oldDisabled := client, disableEthgasMarketAPI
	defer func() { client, disableEthgasMarketAPI = oldClient, oldDisabled }()
	disableEthgasMarketAPI = false
	var unavailable, changedMode atomic.Bool
	var requestedSlots sync.Map
	client = &ApiClient{AccessToken: "test-token", Client: &http.Client{Transport: exchangeTestTransport(func(r *http.Request) (*http.Response, error) {
		slot, err := strconv.ParseUint(r.URL.Query().Get("slot"), 10, 64)
		if err != nil {
			return nil, err
		}
		requestedSlots.Store(slot, true)
		if unavailable.Load() {
			return marketModeTransport{status: http.StatusBadGateway, body: "unavailable"}.RoundTrip(r)
		}
		return marketPrefetchResponse(r, !changedMode.Load())
	})}}
	api := newMarketPrefetchTestAPI()
	// Start shortly before a real slot boundary. Genesis stays immutable once
	// the worker starts; this requires neither synctest nor a slot-length override.
	api.genesisInfo.Data.GenesisTime = uint64(time.Now().Unix()) - 42*common.SecondsPerSlot + 2
	api.startMarketPrefetch()
	defer api.stopMarketPrefetch()
	initial := api.cachedMarketForSlot(42)
	require.NotNil(t, initial)
	require.True(t, *initial.RealTime)
	require.NotNil(t, api.cachedMarketForSlot(41))
	cached, ok := api.marketCache.Load(uint64(42))
	require.True(t, ok)
	entry := cached.(*marketCacheEntry)
	expectedExpiry := time.Unix(int64(api.genesisInfo.Data.GenesisTime+43*common.SecondsPerSlot), 0)
	require.Equal(t, expectedExpiry, entry.expiration, "a prefetched slot remains cached until its end")
	require.Greater(t, time.Until(entry.expiration), time.Duration(common.SecondsPerSlot)*time.Second, "expiry must not be 12 seconds after an early prefetch")
	unavailable.Store(true)
	require.Eventually(t, func() bool {
		_, nextRequested := requestedSlots.Load(uint64(43))
		return nextRequested && api.cachedMarketForSlot(41) == nil
	}, 4*time.Second, 10*time.Millisecond, "wall time must advance the window without beacon head events")
	require.Same(t, initial, api.cachedMarketForSlot(42), "an outage across the boundary must retain the upcoming slot's successful metadata")
	changedMode.Store(true)
	unavailable.Store(false)
	require.Eventually(t, func() bool {
		market := api.cachedMarketForSlot(42)
		return market != nil && market.RealTime != nil && !*market.RealTime
	}, 3*time.Second, 10*time.Millisecond, "successful refreshes must observe mode changes")
}

func TestMarketPrefetchLateFailurePreservesSuccess(t *testing.T) {
	api := newMarketPrefetchTestAPI()
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	slow := &ApiClient{AccessToken: "test-token", Client: &http.Client{Transport: exchangeTestTransport(func(r *http.Request) (*http.Response, error) {
		close(started)
		select {
		case <-release:
			return marketModeTransport{status: http.StatusOK, body: `{"success":true,"data":{"markets":null}}`}.RoundTrip(r)
		case <-r.Context().Done():
			return nil, r.Context().Err()
		}
	})}}
	go func() { done <- api.prefetchMarket(t.Context(), slow, "http://exchange.test", 42) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first request did not start")
	}
	fast := &ApiClient{AccessToken: "test-token", Client: &http.Client{Transport: exchangeTestTransport(func(r *http.Request) (*http.Response, error) {
		return marketPrefetchResponse(r, true)
	})}}
	require.NoError(t, api.prefetchMarket(t.Context(), fast, "http://exchange.test", 42))
	initial := api.cachedMarketForSlot(42)
	require.NotNil(t, initial)
	releaseOnce.Do(func() { close(release) })
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("first request did not finish")
	}
	require.Same(t, initial, api.cachedMarketForSlot(42))
}

func TestMarketPrefetchShutdownCancelsRequests(t *testing.T) {
	oldClient, oldDisabled := client, disableEthgasMarketAPI
	defer func() { client, disableEthgasMarketAPI = oldClient, oldDisabled }()
	disableEthgasMarketAPI = false
	var requests, canceled atomic.Int32
	startedRequests := make(chan uint64, 2)
	client = &ApiClient{AccessToken: "test-token", Client: &http.Client{Transport: exchangeTestTransport(func(r *http.Request) (*http.Response, error) {
		requests.Add(1)
		slot, err := strconv.ParseUint(r.URL.Query().Get("slot"), 10, 64)
		if err != nil {
			return nil, err
		}
		startedRequests <- slot
		<-r.Context().Done()
		if r.Context().Err() == context.Canceled {
			canceled.Add(1)
		}
		return nil, r.Context().Err()
	})}}
	api := newMarketPrefetchTestAPI()
	defer api.stopMarketPrefetch()
	started := make(chan struct{})
	go func() {
		api.startMarketPrefetch()
		close(started)
	}()
	var slots []uint64
	for range 2 {
		select {
		case slot := <-startedRequests:
			slots = append(slots, slot)
		case <-time.After(time.Second):
			t.Fatal("warmup requests did not start")
		}
	}
	require.ElementsMatch(t, []uint64{41, 42}, slots)
	api.startMarketPrefetch() // Repeated starts cannot create another worker.
	require.Equal(t, int32(2), requests.Load())
	stopped := make(chan struct{})
	go func() {
		api.stopMarketPrefetch()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not cancel requests promptly")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not release startup warmup")
	}
	require.Equal(t, int32(2), canceled.Load())
	api.stopMarketPrefetch() // Shutdown is idempotent.
	require.Equal(t, int32(2), requests.Load())
	select {
	case <-api.marketPrefetchDone:
	default:
		t.Fatal("shutdown must wait for the worker to finish")
	}
}

func TestMarketPrefetchRejectedToken(t *testing.T) {
	for _, refreshed := range []bool{false, true} {
		t.Run(fmt.Sprintf("refreshed=%t", refreshed), func(t *testing.T) {
			api := newMarketPrefetchTestAPI()
			started, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
			var releaseOnce sync.Once
			defer releaseOnce.Do(func() { close(release) })
			var requests atomic.Int32
			c := &ApiClient{AccessToken: "old-token", RefreshToken: "refresh-token", Client: &http.Client{Transport: exchangeTestTransport(func(r *http.Request) (*http.Response, error) {
				requests.Add(1)
				if r.Header.Get("Authorization") == "Bearer new-token" {
					return marketPrefetchResponse(r, true)
				}
				close(started)
				select {
				case <-release:
					return marketModeTransport{status: http.StatusUnauthorized, body: `{}`}.RoundTrip(r)
				case <-r.Context().Done():
					return nil, r.Context().Err()
				}
			})}}
			go func() { done <- api.prefetchMarket(t.Context(), c, "http://exchange.test", 42) }()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("authenticated request did not start")
			}
			if refreshed {
				c.setTokens("new-token", "refresh-token")
			}
			releaseOnce.Do(func() { close(release) })
			select {
			case err := <-done:
				require.Error(t, err)
			case <-time.After(time.Second):
				t.Fatal("rejected request did not finish")
			}
			accessToken, refreshToken := c.tokens()
			require.Equal(t, "refresh-token", refreshToken)
			if refreshed {
				require.Equal(t, "new-token", accessToken, "an old request cannot invalidate a token refreshed in flight")
				require.NoError(t, api.prefetchMarket(t.Context(), c, "http://exchange.test", 42))
				require.NotNil(t, api.cachedMarketForSlot(42))
				require.Equal(t, int32(2), requests.Load())
			} else {
				require.Empty(t, accessToken)
				require.Error(t, api.prefetchMarket(t.Context(), c, "http://exchange.test", 42))
				require.Equal(t, int32(1), requests.Load(), "missing tokens must defer to background authentication")
			}
		})
	}
}

func TestMarketPrefetchExpiredResponseNotCached(t *testing.T) {
	api := newMarketPrefetchTestAPI()
	c := &ApiClient{AccessToken: "test-token", Client: &http.Client{Transport: exchangeTestTransport(func(r *http.Request) (*http.Response, error) {
		return marketPrefetchResponse(r, true)
	})}}
	require.NoError(t, api.prefetchMarket(t.Context(), c, "http://exchange.test", 40))
	_, cached := api.marketCache.Load(uint64(40))
	require.False(t, cached, "a response received after the slot ends must not enter the cache")
}

func TestMarketPrefetchDisabled(t *testing.T) {
	oldDisabled := disableEthgasMarketAPI
	defer func() { disableEthgasMarketAPI = oldDisabled }()
	api := newMarketPrefetchTestAPI()
	disableEthgasMarketAPI = true
	api.startMarketPrefetch()
	require.Nil(t, api.marketPrefetchDone)
	disableEthgasMarketAPI = false
	api.opts.ProposerAPI = false
	api.startMarketPrefetch()
	require.Nil(t, api.marketPrefetchDone)
	api.opts.ProposerAPI = true
	api.srvShutdown.Store(true)
	api.startMarketPrefetch()
	require.Nil(t, api.marketPrefetchDone)
	api.stopMarketPrefetch()
}

func TestMarketPrefetchPreservesRefreshedToken(t *testing.T) {
	api := newMarketPrefetchTestAPI()
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	c := &ApiClient{AccessToken: "old-token", Client: &http.Client{Transport: exchangeTestTransport(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("Authorization") == "Bearer old-token" {
			close(started)
			select {
			case <-release:
				return marketModeTransport{status: http.StatusUnauthorized, body: `{}`}.RoundTrip(r)
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
		}
		return marketPrefetchResponse(r, true)
	})}}
	go func() { done <- api.prefetchMarket(t.Context(), c, "http://exchange.test", 42) }()
	<-started
	c.setTokens("new-token", "new-refresh-token")
	require.NoError(t, api.prefetchMarket(t.Context(), c, "http://exchange.test", 42))
	close(release)
	require.ErrorContains(t, <-done, "status 401")
	accessToken, refreshToken := c.tokens()
	require.Equal(t, "new-token", accessToken, "a late 401 must not invalidate the refreshed token")
	require.Equal(t, "new-refresh-token", refreshToken)
	market := api.cachedMarketForSlot(42)
	require.NotNil(t, market, "a late 401 must not overwrite valid prefetched data")
	require.True(t, *market.RealTime)
}
