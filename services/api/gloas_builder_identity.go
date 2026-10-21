package api

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"bitbucket.org/infinity-exchange/mev-boost-relay/beaconclient"
)

const (
	gloasBuilderRefreshInterval = 5 * time.Second
	gloasBuilderLookupTimeout   = 5 * time.Second
	gloasBuilderIdentityMaxAge  = 30 * time.Second
)

type gloasBuilderIdentity struct {
	index      uint64
	generation uint64
	active     bool
	checkedAt  time.Time
	reason     string
}

func (api *RelayAPI) gloasBuilderIdentityEnabled() bool {
	return api.opts.GloasBuilderIndex != nil || len(api.opts.GloasRequestAuthData) > 0
}

// Keep the configured index immutable: it is an optional operator assertion,
// not proof that this signing key owns an active registry entry.
func (api *RelayAPI) refreshGloasBuilderIdentity(ctx context.Context) error {
	if !api.gloasBuilderIdentityEnabled() {
		return nil
	}
	if !api.gloasBuilderRefreshMu.TryLock() {
		return nil
	}
	defer api.gloasBuilderRefreshMu.Unlock()

	index, err := api.lookupGloasBuilderIdentity(ctx)
	reason := "active"
	if err != nil {
		reason = err.Error()
	}
	api.setGloasBuilderIdentity(index, err == nil, reason)
	return err
}

func (api *RelayAPI) lookupGloasBuilderIdentity(ctx context.Context) (uint64, error) {
	if api.publicKey == nil || api.blsSk == nil || api.opts.EthNetDetails.GloasForkVersionHex == "" {
		return 0, errors.New("Gloas signing identity is not configured")
	}
	reader, ok := api.beaconClient.(beaconclient.IBuilderRegistryReader)
	if !ok {
		return 0, errors.New("beacon client does not support the Gloas builder registry")
	}
	response, err := reader.GetStateBuilders(ctx, beaconclient.StateIDHead, *api.publicKey)
	if err != nil {
		return 0, fmt.Errorf("Gloas builder registry lookup failed: %w", err)
	}
	if response == nil || response.ExecutionOptimistic {
		return 0, errors.New("Gloas builder registry is unavailable or execution optimistic")
	}
	if len(response.Data) == 0 {
		return 0, errors.New("relay key is not registered as a Gloas builder")
	}
	if len(response.Data) != 1 {
		return 0, errors.New("Gloas builder registry returned multiple identities")
	}
	builder := response.Data[0]
	if builder.Builder.Pubkey != *api.publicKey {
		return 0, errors.New("Gloas builder registry returned a different public key")
	}
	if builder.Index == math.MaxUint64 || builder.Builder.Version != 0 {
		return 0, errors.New("registry entry is not a Gloas payload builder")
	}
	if expected := api.opts.GloasBuilderIndex; expected != nil && *expected != builder.Index {
		return 0, fmt.Errorf("Gloas builder index mismatch: configured=%d registered=%d", *expected, builder.Index)
	}
	if builder.Status != "active" || builder.Builder.WithdrawableEpoch != math.MaxUint64 {
		return 0, fmt.Errorf("relay's Gloas builder registration is not active (status=%s)", builder.Status)
	}
	return builder.Index, nil
}

func (api *RelayAPI) setGloasBuilderIdentity(index uint64, active bool, reason string) {
	api.gloasBuilderIdentityMu.Lock()
	previous := api.gloasBuilderIdentity
	generation := previous.generation
	if previous.active != active || previous.index != index {
		generation++
	}
	api.gloasBuilderIdentity = gloasBuilderIdentity{
		index: index, active: active, checkedAt: time.Now(), reason: reason, generation: generation,
	}
	api.gloasBuilderIdentityMu.Unlock()

	if previous.reason == reason && previous.active == active && previous.index == index {
		return
	}
	log := api.log.WithField("builderIndex", index)
	if active {
		log.Info("relay's Gloas builder identity is registered and active")
	} else {
		log.WithField("reason", reason).Warn("Gloas bidding disabled until builder identity is verified active")
	}
}

func (api *RelayAPI) currentGloasBuilderIdentity() (gloasBuilderIdentity, bool) {
	api.gloasBuilderIdentityMu.RLock()
	identity := api.gloasBuilderIdentity
	api.gloasBuilderIdentityMu.RUnlock()
	valid := api.gloasBuilderIdentityEnabled() && api.blsSk != nil &&
		api.opts.EthNetDetails.GloasForkVersionHex != "" && !api.srvShutdown.Load() &&
		identity.active && !identity.checkedAt.IsZero() && time.Since(identity.checkedAt) < gloasBuilderIdentityMaxAge
	return identity, valid
}

func (api *RelayAPI) gloasBuilderIdentityStillCurrent(identity gloasBuilderIdentity) bool {
	current, active := api.currentGloasBuilderIdentity()
	return active && current.index == identity.index && current.generation == identity.generation
}

func (api *RelayAPI) startGloasBuilderIdentityUpdates() {
	if !api.gloasBuilderIdentityEnabled() {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	api.gloasBuilderIdentityMu.Lock()
	api.gloasBuilderIdentityCancel = cancel
	api.gloasBuilderIdentityMu.Unlock()

	lookup := func() {
		lookupCtx, lookupCancel := context.WithTimeout(ctx, gloasBuilderLookupTimeout)
		defer lookupCancel()
		_ = api.refreshGloasBuilderIdentity(lookupCtx)
	}
	lookup()
	api.gloasBuilderIdentityWG.Add(1)
	go func() {
		defer api.gloasBuilderIdentityWG.Done()
		ticker := time.NewTicker(gloasBuilderRefreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				lookup()
			}
		}
	}()
}

func (api *RelayAPI) stopGloasBuilderIdentityUpdates() {
	api.gloasBuilderIdentityMu.Lock()
	cancel := api.gloasBuilderIdentityCancel
	api.gloasBuilderIdentityMu.Unlock()
	if cancel != nil {
		cancel()
	}
}
