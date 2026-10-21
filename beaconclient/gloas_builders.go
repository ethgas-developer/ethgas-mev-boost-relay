package beaconclient

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/attestantio/go-eth2-client/spec/bellatrix"
	"github.com/attestantio/go-eth2-client/spec/phase0"
)

// IBuilderRegistryReader is optional so existing beacon implementations do not
// need to support a post-Gloas endpoint to continue serving pre-Gloas traffic.
// Custom IBeaconInstance adapters used through MultiBeaconClient must also
// provide BuilderRegistrySyncStatus(context.Context) with bounded cancellation.
type IBuilderRegistryReader interface {
	GetStateBuilders(ctx context.Context, stateID string, pubkey phase0.BLSPubKey) (*GetStateBuildersResponse, error)
}

var (
	ErrBuilderRegistryUnsupported = errors.New("no beacon instance supports the Gloas builder registry")
	ErrBuilderRegistryOptimistic  = errors.New("builder registry response is execution optimistic")
)

const (
	builderRegistryTimeout         = 5 * time.Second
	builderRegistryInstanceTimeout = 2 * time.Second
	maxBuilderRegistryResponse     = 64 * 1024
)

var builderRegistryClient = &http.Client{
	Timeout: builderRegistryTimeout,
	CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

// GetStateBuildersResponse follows the standard Beacon API getStateBuilders
// response. An empty Data slice is a successful, authoritative not-found result.
// https://github.com/ethereum/beacon-APIs/blob/master/apis/beacon/states/builders.yaml
type GetStateBuildersResponse struct {
	ExecutionOptimistic bool                   `json:"execution_optimistic"`
	Finalized           bool                   `json:"finalized"`
	Data                []BuilderResponseEntry `json:"data"`
}

type BuilderResponseEntry struct {
	Index   uint64                     `json:"index,string"`
	Status  string                     `json:"status"`
	Builder BuilderResponseBuilderData `json:"builder"`
}

type BuilderResponseBuilderData struct {
	Pubkey            phase0.BLSPubKey           `json:"pubkey"`
	Version           uint8                      `json:"version,string"`
	ExecutionAddress  bellatrix.ExecutionAddress `json:"execution_address"`
	Balance           uint64                     `json:"balance,string"`
	DepositEpoch      uint64                     `json:"deposit_epoch,string"`
	WithdrawableEpoch uint64                     `json:"withdrawable_epoch,string"`
}

// GetStateBuilders reads only the requested key, bounding both time and body
// size. Optimistic or incomplete state must never activate the relay identity.
func (c *ProdBeaconInstance) GetStateBuilders(ctx context.Context, stateID string, pubkey phase0.BLSPubKey) (*GetStateBuildersResponse, error) {
	if err := validateBuilderStateID(stateID); err != nil {
		return nil, err
	}
	if pubkey == (phase0.BLSPubKey{}) {
		return nil, errors.New("builder public key is zero")
	}
	ctx, cancel := context.WithTimeout(ctx, builderRegistryTimeout)
	defer cancel()
	payload, err := json.Marshal(struct {
		IDs []string `json:"ids"`
	}{IDs: []string{pubkey.String()}})
	if err != nil {
		return nil, fmt.Errorf("encode builder registry request: %w", err)
	}
	uri := fmt.Sprintf("%s/eth/v1/beacon/states/%s/builders", strings.TrimRight(c.beaconURI, "/"), stateID)
	body, err := fetchBuilderRegistryJSON(ctx, http.MethodPost, uri, payload)
	if err != nil {
		return nil, err
	}
	return decodeBuilderRegistryResponse(body, pubkey)
}

func validateBuilderStateID(stateID string) error {
	switch stateID {
	case "head", "genesis", "finalized", "justified":
		return nil
	}
	if strings.HasPrefix(stateID, "0x") && len(stateID) == 66 {
		if _, err := hex.DecodeString(stateID[2:]); err == nil {
			return nil
		}
	}
	if stateID != "" && strings.IndexFunc(stateID, func(r rune) bool { return r < '0' || r > '9' }) == -1 {
		if _, err := strconv.ParseUint(stateID, 10, 64); err == nil {
			return nil
		}
	}
	return fmt.Errorf("invalid builder registry state ID %q", stateID)
}

func fetchBuilderRegistryJSON(ctx context.Context, method, uri string, payload []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, uri, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("create builder registry request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := builderRegistryClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request builder registry: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: builder registry HTTP status %d", ErrHTTPErrorResponse, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBuilderRegistryResponse+1))
	if err != nil {
		return nil, fmt.Errorf("read builder registry response: %w", err)
	}
	if len(body) > maxBuilderRegistryResponse {
		return nil, errors.New("builder registry response exceeds size limit")
	}
	return body, nil
}

func decodeBuilderRegistryResponse(body []byte, pubkey phase0.BLSPubKey) (*GetStateBuildersResponse, error) {
	// Pointers distinguish a real zero (valid for index/version/balance/epoch)
	// from an omitted or null required field. Allow unknown fields for API evolution.
	var wire struct {
		ExecutionOptimistic *bool              `json:"execution_optimistic"`
		Finalized           *bool              `json:"finalized"`
		Data                *[]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &wire); err != nil {
		return nil, fmt.Errorf("decode builder registry response: %w", err)
	}
	if wire.ExecutionOptimistic == nil || wire.Finalized == nil || wire.Data == nil {
		return nil, errors.New("builder registry response is missing required metadata or data")
	}
	if *wire.ExecutionOptimistic {
		return nil, ErrBuilderRegistryOptimistic
	}
	if len(*wire.Data) > 1 {
		return nil, errors.New("filtered builder registry response contains multiple entries")
	}
	result := &GetStateBuildersResponse{Finalized: *wire.Finalized, Data: make([]BuilderResponseEntry, 0, len(*wire.Data))}
	for _, raw := range *wire.Data {
		var entry struct {
			Index   *uint64 `json:"index,string"`
			Status  *string `json:"status"`
			Builder *struct {
				Pubkey            *phase0.BLSPubKey           `json:"pubkey"`
				Version           *uint8                      `json:"version,string"`
				ExecutionAddress  *bellatrix.ExecutionAddress `json:"execution_address"`
				Balance           *uint64                     `json:"balance,string"`
				DepositEpoch      *uint64                     `json:"deposit_epoch,string"`
				WithdrawableEpoch *uint64                     `json:"withdrawable_epoch,string"`
			} `json:"builder"`
		}
		if err := json.Unmarshal(raw, &entry); err != nil {
			return nil, fmt.Errorf("decode builder registry entry: %w", err)
		}
		b := entry.Builder
		if entry.Index == nil || entry.Status == nil || b == nil || b.Pubkey == nil || b.Version == nil || b.ExecutionAddress == nil || b.Balance == nil || b.DepositEpoch == nil || b.WithdrawableEpoch == nil {
			return nil, errors.New("builder registry entry is missing required fields")
		}
		if *entry.Status != "pending" && *entry.Status != "active" && *entry.Status != "exited" {
			return nil, fmt.Errorf("unknown builder registry status %q", *entry.Status)
		}
		if *b.Pubkey != pubkey {
			return nil, errors.New("filtered builder registry response has an unexpected public key")
		}
		result.Data = append(result.Data, BuilderResponseEntry{
			Index: *entry.Index, Status: *entry.Status,
			Builder: BuilderResponseBuilderData{Pubkey: *b.Pubkey, Version: *b.Version, ExecutionAddress: *b.ExecutionAddress, Balance: *b.Balance, DepositEpoch: *b.DepositEpoch, WithdrawableEpoch: *b.WithdrawableEpoch},
		})
	}
	return result, nil
}

// GetStateBuilders prefers the last successful beacon, skipping syncing and
// optimistic nodes even when ALLOW_SYNCING_BEACON_NODE is enabled. Each instance
// gets a bounded share of the overall request deadline, so failures can fall back.
func (c *MultiBeaconClient) GetStateBuilders(ctx context.Context, stateID string, pubkey phase0.BLSPubKey) (*GetStateBuildersResponse, error) {
	if err := validateBuilderStateID(stateID); err != nil {
		return nil, err
	}
	if pubkey == (phase0.BLSPubKey{}) {
		return nil, errors.New("builder public key is zero")
	}
	ctx, cancel := context.WithTimeout(ctx, builderRegistryTimeout)
	defer cancel()
	indices := make([]int, 0, len(c.beaconInstances))
	best := int(c.bestBeaconIndex.Load())
	if best >= 0 && best < len(c.beaconInstances) {
		indices = append(indices, best)
	}
	for index := range c.beaconInstances {
		if index != best {
			indices = append(indices, index)
		}
	}
	supported := false
	var failures []error
	for _, index := range indices {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		instance := c.beaconInstances[index]
		reader, ok := instance.(IBuilderRegistryReader)
		if !ok {
			continue
		}
		supported = true
		instanceCtx, instanceCancel := context.WithTimeout(ctx, builderRegistryInstanceTimeout)
		err := checkBuilderRegistrySyncStatus(instanceCtx, instance)
		var response *GetStateBuildersResponse
		if err == nil {
			response, err = reader.GetStateBuilders(instanceCtx, stateID, pubkey)
			if err == nil && response == nil {
				err = errors.New("builder registry returned a nil response")
			}
			if err == nil && response.ExecutionOptimistic {
				err = ErrBuilderRegistryOptimistic
			}
		}
		instanceCancel()
		if err != nil {
			c.log.WithField("uri", instance.GetURI()).WithError(err).Warn("failed to fetch safe builder registry")
			failures = append(failures, err)
			continue
		}
		c.bestBeaconIndex.Store(int64(index))
		return response, nil
	}
	if !supported {
		return nil, ErrBuilderRegistryUnsupported
	}
	return nil, fmt.Errorf("%w: builder registry: %w", ErrBeaconNodesUnavailable, errors.Join(failures...))
}

func checkBuilderRegistrySyncStatus(ctx context.Context, instance IBeaconInstance) error {
	if prod, ok := instance.(*ProdBeaconInstance); ok {
		body, err := fetchBuilderRegistryJSON(ctx, http.MethodGet, strings.TrimRight(prod.beaconURI, "/")+"/eth/v1/node/syncing", nil)
		if err != nil {
			return err
		}
		var wire struct {
			Data *struct {
				HeadSlot     *uint64 `json:"head_slot,string"`
				IsSyncing    *bool   `json:"is_syncing"`
				IsOptimistic *bool   `json:"is_optimistic"`
				ELOffline    bool    `json:"el_offline"`
			} `json:"data"`
		}
		if err := json.Unmarshal(body, &wire); err != nil {
			return fmt.Errorf("decode builder registry sync status: %w", err)
		}
		if wire.Data == nil || wire.Data.HeadSlot == nil || wire.Data.IsSyncing == nil || wire.Data.IsOptimistic == nil {
			return errors.New("builder registry sync status is missing required fields")
		}
		if *wire.Data.IsSyncing || *wire.Data.IsOptimistic || wire.Data.ELOffline {
			return ErrBeaconNodeSyncing
		}
		return nil
	}
	// The old SyncStatus interface cannot cancel its request. Custom adapters
	// must explicitly provide a context-bounded implementation rather than
	// spawning potentially unbounded legacy requests on every refresh.
	reader, ok := instance.(interface {
		BuilderRegistrySyncStatus(context.Context) (*SyncStatusPayloadData, error)
	})
	if !ok {
		return errors.New("beacon instance lacks a context-bounded builder registry sync check")
	}
	status, err := reader.BuilderRegistrySyncStatus(ctx)
	if err != nil {
		return err
	}
	if status == nil || status.IsSyncing {
		return ErrBeaconNodeSyncing
	}
	return nil
}
