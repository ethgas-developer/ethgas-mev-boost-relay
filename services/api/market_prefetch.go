package api

import (
	"context"
	"sync"
	"time"

	"bitbucket.org/infinity-exchange/mev-boost-relay/common"
	"github.com/sirupsen/logrus"
)

const (
	marketRequestTimeout   = 2 * time.Second
	marketPrefetchInterval = time.Second
)

// Prefetch the current and next slots using wall time, so a late/missed head
// event cannot delay the next proposal's market lookup. Each round has at most
// two concurrent requests and completes before another round can start.
func (api *RelayAPI) startMarketPrefetch() {
	if disableEthgasMarketAPI || !api.opts.ProposerAPI || api.genesisInfo == nil {
		return
	}
	api.marketPrefetchMu.Lock()
	if api.marketPrefetchCancel != nil || api.srvShutdown.Load() {
		api.marketPrefetchMu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	api.marketPrefetchCancel = cancel
	api.marketPrefetchDone = make(chan struct{})
	done, ready := api.marketPrefetchDone, make(chan struct{})
	c, apiURL := client, exchangeAPIURL
	api.marketPrefetchMu.Unlock()

	go func() {
		defer close(done)
		ticker := time.NewTicker(marketPrefetchInterval)
		defer ticker.Stop()
		lastErrors := make(map[uint64]string)
		api.prefetchMarketWindow(ctx, c, apiURL, lastErrors)
		close(ready)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				api.prefetchMarketWindow(ctx, c, apiURL, lastErrors)
			}
		}
	}()
	// Warm the cache before accepting bids. An unavailable exchange only delays
	// startup by the bounded request timeout; subsequent attempts run in the background.
	<-ready
}

func (api *RelayAPI) stopMarketPrefetch() {
	api.marketPrefetchMu.Lock()
	cancel, done := api.marketPrefetchCancel, api.marketPrefetchDone
	api.marketPrefetchMu.Unlock()
	if cancel != nil {
		cancel()
		<-done
	}
}

func (api *RelayAPI) prefetchMarketWindow(ctx context.Context, c *ApiClient, apiURL string, lastErrors map[uint64]string) {
	now := time.Now().Unix()
	genesis := api.genesisInfo.Data.GenesisTime
	if ctx.Err() != nil || now < 0 || uint64(now) < genesis {
		return
	}
	slot := (uint64(now) - genesis) / common.SecondsPerSlot
	for oldSlot := range lastErrors {
		if oldSlot < slot {
			delete(lastErrors, oldSlot)
		}
	}
	var wg sync.WaitGroup
	var errs [2]error
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = api.prefetchMarket(ctx, c, apiURL, slot+uint64(i))
		}()
	}
	wg.Wait()
	if ctx.Err() != nil {
		return
	}
	for i, err := range errs {
		marketSlot := slot + uint64(i)
		if err == nil {
			delete(lastErrors, marketSlot)
		} else if lastErrors[marketSlot] != err.Error() {
			api.log.WithError(err).WithField("slot", marketSlot).Warn("market prefetch failed; will retry")
			lastErrors[marketSlot] = err.Error()
		}
	}
}

func (api *RelayAPI) prefetchMarket(ctx context.Context, c *ApiClient, apiURL string, slot uint64) error {
	started := time.Now()
	market, err := c.fetchWholeBlockMarket(ctx, apiURL, slot)
	if err != nil || market == nil {
		// Absence and transient failures never overwrite the last valid result.
		// There is no negative cache; the next round will retry.
		return err
	}
	expiration := time.Unix(int64(api.genesisInfo.Data.GenesisTime+(slot+1)*common.SecondsPerSlot), 0) //nolint:gosec
	if ctx.Err() != nil || !time.Now().Before(expiration) {
		return ctx.Err()
	}
	previous := api.cachedMarketForSlot(slot)
	api.marketCache.Store(slot, &marketCacheEntry{value: market, expiration: expiration})
	if previous == nil || previous.MultiRelay == nil || previous.RealTime == nil ||
		*previous.MultiRelay != *market.MultiRelay || *previous.RealTime != *market.RealTime {
		api.log.WithFields(logrus.Fields{
			"slot": slot, "multiRelay": *market.MultiRelay, "realTime": *market.RealTime,
			"durationMs": time.Since(started).Milliseconds(),
		}).Info("market prefetched")
	}
	return nil
}
