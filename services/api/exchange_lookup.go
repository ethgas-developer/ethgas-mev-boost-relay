package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

const (
	exchangeLookupTTL             = time.Minute
	exchangeLookupRequestTimeout  = 500 * time.Millisecond
	exchangeLookupFailureCooldown = 250 * time.Millisecond
	exchangeRateLimitInitialDelay = time.Second
	exchangeRateLimitMaxDelay     = 8 * time.Second
	exchangeRateLimitJitter       = 250 * time.Millisecond
)

// exchangeRateLimitError retains Retry-After independently of the HTTP body, so
// both exchange endpoints can apply one cooldown for their shared API URL.
type exchangeRateLimitError struct {
	retryAt time.Time
}

func (*exchangeRateLimitError) Error() string { return "exchange API returned HTTP 429" }

func newExchangeRateLimitError(retryAfter string) error {
	limited := new(exchangeRateLimitError)
	retryAfter = strings.TrimSpace(retryAfter)
	if seconds, err := strconv.ParseInt(retryAfter, 10, 64); err == nil && seconds >= 0 {
		if seconds > math.MaxInt64/int64(time.Second) {
			limited.retryAt = time.Now().Add(time.Duration(math.MaxInt64))
		} else {
			limited.retryAt = time.Now().Add(time.Duration(seconds) * time.Second)
		}
	} else if date, err := http.ParseTime(retryAfter); err == nil {
		limited.retryAt = date
	}
	return limited
}

type exchangeLookupKey struct {
	apiURL   string
	endpoint string
	slot     uint64
}

type exchangeLookupEntry struct {
	value      any
	expiration time.Time
}

type exchangeLookupFailure struct {
	err        error
	expiration time.Time
}

type exchangeRateLimitState struct {
	until      time.Time
	expiration time.Time
	attempts   uint
}

type exchangeLookupCooldownError struct {
	until time.Time
	cause error
}

func (e *exchangeLookupCooldownError) Error() string {
	return fmt.Sprintf("exchange lookup cooling down until %s: %v", e.until.Format(time.RFC3339Nano), e.cause)
}

func (e *exchangeLookupCooldownError) Unwrap() error { return e.cause }

// The zero value is ready for use. Successful responses and temporary failures
// are separate: an error can never become an empty, permissive assignment.
type exchangeLookupCache struct {
	mu       sync.Mutex
	flights  singleflight.Group
	entries  map[exchangeLookupKey]exchangeLookupEntry
	failures map[exchangeLookupKey]exchangeLookupFailure
	limits   map[string]exchangeRateLimitState
}

func (c *exchangeLookupCache) purgeLocked(now time.Time) {
	for key, entry := range c.entries {
		if !now.Before(entry.expiration) {
			delete(c.entries, key)
		}
	}
	for key, failure := range c.failures {
		if !now.Before(failure.expiration) {
			delete(c.failures, key)
		}
	}
	for apiURL, limit := range c.limits {
		if !now.Before(limit.expiration) {
			delete(c.limits, apiURL)
		}
	}
}

// Cached success wins over cooldowns, including a 429 from another endpoint.
func (c *exchangeLookupCache) load(key exchangeLookupKey) (any, error, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	c.purgeLocked(now)
	if entry, ok := c.entries[key]; ok {
		return entry.value, nil, true
	}
	if limit, ok := c.limits[key.apiURL]; ok && now.Before(limit.until) {
		return nil, &exchangeLookupCooldownError{
			until: limit.until,
			cause: &exchangeRateLimitError{retryAt: limit.until},
		}, true
	}
	if failure, ok := c.failures[key]; ok {
		return nil, &exchangeLookupCooldownError{until: failure.expiration, cause: failure.err}, true
	}
	return nil, nil, false
}

func (c *exchangeLookupCache) save(key exchangeLookupKey, value any, fetchErr error) (any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	c.purgeLocked(now)
	if fetchErr == nil {
		if c.entries == nil {
			c.entries = make(map[exchangeLookupKey]exchangeLookupEntry)
		}
		c.entries[key] = exchangeLookupEntry{value: value, expiration: now.Add(exchangeLookupTTL)}
		delete(c.failures, key)
		// A concurrent successful request must not undo an active 429 cooldown.
		if limit, ok := c.limits[key.apiURL]; ok {
			if now.Before(limit.until) {
				limit.attempts = 0
				c.limits[key.apiURL] = limit
			} else {
				delete(c.limits, key.apiURL)
			}
		}
		return value, nil
	}

	var limited *exchangeRateLimitError
	if errors.As(fetchErr, &limited) {
		if c.limits == nil {
			c.limits = make(map[string]exchangeRateLimitState)
		}
		limit := c.limits[key.apiURL]
		until := limited.retryAt
		if until.IsZero() {
			delay := exchangeRateLimitInitialDelay
			for attempt := uint(0); attempt < limit.attempts && delay < exchangeRateLimitMaxDelay; attempt++ {
				delay *= 2
			}
			delay = min(delay, exchangeRateLimitMaxDelay)
			delay += time.Duration(rand.Int64N(int64(exchangeRateLimitJitter)))
			until = now.Add(delay)
		}
		// Even Retry-After: 0 must not cause a burst of immediate retries.
		until = maxTime(until, now.Add(exchangeLookupFailureCooldown))
		limit.until = maxTime(limit.until, until)
		limit.expiration = maxTime(limit.until, now.Add(exchangeLookupTTL))
		if limit.attempts < 4 {
			limit.attempts++
		}
		c.limits[key.apiURL] = limit
	} else {
		if c.failures == nil {
			c.failures = make(map[exchangeLookupKey]exchangeLookupFailure)
		}
		c.failures[key] = exchangeLookupFailure{err: fetchErr, expiration: now.Add(exchangeLookupFailureCooldown)}
	}
	// Recheck after a failure: a valid result saved while the request was in
	// flight remains authoritative for this slot and must never be discarded.
	if entry, ok := c.entries[key]; ok {
		return entry.value, nil
	}
	return nil, fetchErr
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func (c *exchangeLookupCache) get(ctx context.Context, apiURL, endpoint string, slot uint64, fetch func(context.Context) (any, error)) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key := exchangeLookupKey{apiURL: strings.TrimRight(apiURL, "/"), endpoint: endpoint, slot: slot}
	if value, err, done := c.load(key); done {
		return value, err
	}
	flightKey := key.apiURL + "\x00" + endpoint + "\x00" + strconv.FormatUint(slot, 10)
	result := c.flights.DoChan(flightKey, func() (any, error) {
		if value, err, done := c.load(key); done {
			return value, err
		}
		// One caller disconnecting must not cancel the shared lookup for others.
		fetchCtx, cancel := context.WithTimeout(context.Background(), exchangeLookupRequestTimeout)
		defer cancel()
		value, err := fetch(fetchCtx)
		if err == nil && value == nil {
			err = errors.New("exchange lookup returned no response")
		}
		return c.save(key, value, err)
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case response := <-result:
		return response.Val, response.Err
	}
}

func (api *RelayAPI) getBuilderAssignment(ctx context.Context, slot uint64) (*BuilderResponse, error) {
	apiURL := strings.TrimRight(exchangeAPIURL, "/")
	value, err := api.exchangeLookups.get(ctx, apiURL, "builders", slot, func(fetchCtx context.Context) (any, error) {
		return FetchBuilderPubKey(fetchCtx, apiURL, slot)
	})
	if err != nil {
		return nil, err
	}
	return value.(*BuilderResponse), nil
}

func (api *RelayAPI) getSlotBundles(ctx context.Context, slot uint64) (*PreconfBundles, error) {
	c, apiURL := client, exchangeAPIURL
	if c != nil && c.APIURL != "" {
		apiURL = c.APIURL
	}
	apiURL = strings.TrimRight(apiURL, "/")
	value, err := api.exchangeLookups.get(ctx, apiURL, "bundles", slot, func(fetchCtx context.Context) (any, error) {
		return fetchSlotBundles(fetchCtx, c, apiURL, slot)
	})
	if err != nil {
		return nil, err
	}
	return value.(*PreconfBundles), nil
}

func fetchSlotBundles(ctx context.Context, c *ApiClient, apiURL string, slot uint64) (*PreconfBundles, error) {
	if c == nil || c.Client == nil {
		return nil, errors.New("exchange HTTP client unavailable")
	}
	accessToken, _ := c.tokens()
	if accessToken == "" {
		return nil, errors.New("exchange authentication unavailable; background login pending")
	}
	url := fmt.Sprintf("%s/api/v1/slot/bundles?slot=%d", apiURL, slot)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create slot bundles request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	resp, err := c.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch slot bundles: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized {
		c.invalidateAccessToken(accessToken)
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, newExchangeRateLimitError(resp.Header.Get("Retry-After"))
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("slot bundles returned HTTP %d", resp.StatusCode)
	}
	var response ApiResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to decode slot bundles response: %w", err)
	}
	if !response.Success {
		return nil, errors.New("slot bundles response indicates failure")
	}
	var bundles *PreconfBundles
	if err := json.Unmarshal(response.Data, &bundles); err != nil {
		return nil, fmt.Errorf("failed to decode slot bundles data: %w", err)
	}
	if bundles == nil || bundles.Slot == nil {
		return nil, errors.New("slot bundles response has no slot")
	}
	if *bundles.Slot != slot {
		return nil, fmt.Errorf("slot bundles response slot %d does not match requested slot %d", *bundles.Slot, slot)
	}
	return bundles, nil
}
