package api

import (
	"context"
	"errors"
	"sync"
	"time"

	"bitbucket.org/infinity-exchange/mev-boost-relay/common"
	"github.com/sirupsen/logrus"
)

const exchangePrefetchInterval = 250 * time.Millisecond

type exchangePrefetchKey struct {
	slot     uint64
	endpoint string
}

type exchangePrefetchResult struct {
	key      exchangePrefetchKey
	duration time.Duration
	err      error
}

// Warm submission constraints independently of market-mode polling. A slow
// market response must not hold up the finalized assignment or bundle lookup.
func (api *RelayAPI) startExchangePrefetch() {
	if disableEthgasMarketAPI || !api.opts.BlockBuilderAPI || api.genesisInfo == nil {
		return
	}
	api.exchangePrefetchMu.Lock()
	if api.exchangePrefetchCancel != nil || api.srvShutdown.Load() {
		api.exchangePrefetchMu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	api.exchangePrefetchCancel = cancel
	api.exchangePrefetchDone = make(chan struct{})
	done, ready := api.exchangePrefetchDone, make(chan struct{})
	api.exchangePrefetchMu.Unlock()

	go func() {
		defer close(done)
		ticker := time.NewTicker(exchangePrefetchInterval)
		defer ticker.Stop()
		lastResults := make(map[exchangePrefetchKey]string)
		api.prefetchExchangeWindow(ctx, time.Now(), lastResults)
		close(ready)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				api.prefetchExchangeWindow(ctx, time.Now(), lastResults)
			}
		}
	}()
	// The first round is bounded by the shared lookup's 500ms timeout. Warm
	// the current slot before accepting submissions; retries remain background work.
	<-ready
}

func (api *RelayAPI) stopExchangePrefetch() {
	api.exchangePrefetchMu.Lock()
	cancel, done := api.exchangePrefetchCancel, api.exchangePrefetchDone
	api.exchangePrefetchMu.Unlock()
	if cancel != nil {
		cancel()
		<-done
	}
}

func (api *RelayAPI) prefetchExchangeWindow(ctx context.Context, now time.Time, lastResults map[exchangePrefetchKey]string) {
	genesis := api.genesisInfo.Data.GenesisTime
	if ctx.Err() != nil || now.Unix() < 0 || uint64(now.Unix()) < genesis {
		return
	}
	slot := (uint64(now.Unix()) - genesis) / common.SecondsPerSlot
	for key := range lastResults {
		if key.slot < slot {
			delete(lastResults, key)
		}
	}

	results := make(chan exchangePrefetchResult, 4)
	var wg sync.WaitGroup
	for _, target := range []uint64{slot, slot + 1} {
		slotStart := time.Unix(int64(genesis+target*common.SecondsPerSlot), 0) //nolint:gosec
		finalizedAt := slotStart.Add(time.Duration(getExchangeFinalizedCutoffMs) * time.Millisecond)
		if now.Before(finalizedAt) {
			// Before finalization even a structurally valid empty response can
			// change. Never let it become the slot's cached admission policy.
			continue
		}
		for _, endpoint := range []string{"builders", "bundles"} {
			if ctx.Err() != nil {
				break
			}
			key := exchangePrefetchKey{slot: target, endpoint: endpoint}
			wg.Add(1)
			go func() {
				defer wg.Done()
				started := time.Now()
				var err error
				// A submission may join this same singleflight request. Drain
				// its bounded fetch on shutdown instead of canceling that submission.
				if key.endpoint == "builders" {
					_, err = api.getBuilderAssignment(context.Background(), key.slot)
				} else {
					_, err = api.getSlotBundles(context.Background(), key.slot)
				}
				results <- exchangePrefetchResult{key: key, duration: time.Since(started), err: err}
			}()
		}
	}
	wg.Wait()
	close(results)
	if ctx.Err() != nil {
		return
	}
	for result := range results {
		previous, known := lastResults[result.key]
		fields := logrus.Fields{
			"slot": result.key.slot, "endpoint": result.key.endpoint,
			"durationMs": result.duration.Milliseconds(),
		}
		if result.err != nil {
			message := result.err.Error()
			var cooldown *exchangeLookupCooldownError
			if errors.As(result.err, &cooldown) {
				// The retry deadline changes across attempts; keep one warning
				// for the same underlying failure until it changes or recovers.
				message = cooldown.cause.Error()
			}
			if !known || previous != message {
				api.log.WithFields(fields).WithError(result.err).Warn("exchange prefetch failed; will retry")
			}
			lastResults[result.key] = message
		} else {
			if !known || previous != "" {
				api.log.WithFields(fields).Info("exchange data prefetched")
			}
			lastResults[result.key] = ""
		}
	}
}
