package api

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

	"bitbucket.org/infinity-exchange/mev-boost-relay/beaconclient"
	"bitbucket.org/infinity-exchange/mev-boost-relay/common"
	"bitbucket.org/infinity-exchange/mev-boost-relay/datastore"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/flashbots/go-boost-utils/ssz"
)

var (
	errMissingGloasProposerPreferences   = errors.New("no signed proposer preferences for Gloas duty")
	errAmbiguousGloasProposerPreferences = errors.New("conflicting signed proposer preferences across dependent roots")
)

const maxGloasProposerPreferenceLookaheadEpochs = uint64(1)

// processGloasProposerPreferences consumes only beacon-node gossip-validated
// events, independently verifies the validator signature, and persists the
// complete signed object for split relay deployments.
func (api *RelayAPI) processGloasProposerPreferences(event beaconclient.ProposerPreferencesEvent) {
	if !strings.EqualFold(event.Version, common.ForkVersionStringGloas) || event.Data == nil || event.Data.Message == nil {
		api.log.Warn("ignoring malformed or non-Gloas proposer preferences event")
		return
	}
	preference := event.Data
	message := preference.Message
	slot := uint64(message.ProposalSlot)
	headSlot := api.headSlot.Load()
	if slot <= headSlot {
		api.log.WithFields(map[string]any{"proposalSlot": slot, "headSlot": headSlot}).Warn("ignoring stale Gloas proposer preferences")
		return
	}
	if slot/common.SlotsPerEpoch > headSlot/common.SlotsPerEpoch+maxGloasProposerPreferenceLookaheadEpochs {
		api.log.WithFields(map[string]any{"proposalSlot": slot, "headSlot": headSlot}).Warn("ignoring Gloas proposer preferences beyond proposer lookahead")
		return
	}
	if message.DependentRoot == (phase0.Root{}) {
		api.log.WithField("proposalSlot", slot).Warn("ignoring Gloas proposer preferences with zero dependent root")
		return
	}

	proposerPubkey, found := api.datastore.GetKnownValidatorPubkeyByIndex(uint64(message.ValidatorIndex))
	if !found {
		api.log.WithFields(map[string]any{
			"proposalSlot":   slot,
			"validatorIndex": uint64(message.ValidatorIndex),
		}).Warn("ignoring Gloas proposer preferences for unknown validator index")
		return
	}
	pubkey, err := proposerPubkey.ToPubkey()
	if err != nil {
		api.log.WithError(err).Warn("invalid known-validator pubkey for Gloas proposer preferences")
		return
	}
	verified, err := ssz.VerifySignature(message, api.opts.EthNetDetails.DomainProposerPreferences, pubkey[:], preference.Signature[:])
	if err != nil || !verified {
		api.log.WithError(err).WithFields(map[string]any{
			"proposalSlot":   slot,
			"validatorIndex": uint64(message.ValidatorIndex),
		}).Warn("ignoring Gloas proposer preferences with invalid signature")
		return
	}

	if err := api.redis.SaveGloasSignedProposerPreferences(preference, 3*common.DurationPerEpoch); err != nil {
		fields := map[string]any{
			"proposalSlot":   slot,
			"validatorIndex": uint64(message.ValidatorIndex),
			"dependentRoot":  message.DependentRoot.String(),
		}
		if errors.Is(err, datastore.ErrGloasProposerPreferencesConflict) {
			api.log.WithError(err).WithFields(fields).Error("validator equivocated Gloas proposer preferences")
		} else {
			api.log.WithError(err).WithFields(fields).Error("failed to persist Gloas proposer preferences")
		}
		return
	}
	api.log.WithFields(map[string]any{
		"proposalSlot":   slot,
		"validatorIndex": uint64(message.ValidatorIndex),
		"dependentRoot":  message.DependentRoot.String(),
		"feeRecipient":   message.FeeRecipient.String(),
		"targetGasLimit": uint64(message.TargetGasLimit),
	}).Info("Gloas proposer preferences stored")
}

// resolveGloasProposerPreferences validates the exact proposal duty and every
// branch-scoped preference again after loading it from shared Redis. A relay
// does not have a fork-choice store with which to derive the dependent root
// from the requested parent. If multiple BN-validated roots disagree on the
// fields that affect the bid, it fails closed; identical values are safe to
// use and the exact roots remain stored for audit/reorg handling.
func (api *RelayAPI) resolveGloasProposerPreferences(slot uint64, validatorIndex uint64, proposerPubkey phase0.BLSPubKey) (*common.SignedProposerPreferences, error) {
	knownPubkey, found := api.datastore.GetKnownValidatorPubkeyByIndex(validatorIndex)
	if !found || !strings.EqualFold(knownPubkey.String(), proposerPubkey.String()) {
		return nil, fmt.Errorf("proposer pubkey does not match payload-attributes validator index %d", validatorIndex)
	}

	candidates, err := api.redis.GetGloasSignedProposerPreferencesForDuty(slot, validatorIndex)
	if err != nil {
		return nil, fmt.Errorf("load signed proposer preferences: %w", err)
	}
	if len(candidates) == 0 {
		return nil, errMissingGloasProposerPreferences
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i] == nil || candidates[i].Message == nil {
			return false
		}
		if candidates[j] == nil || candidates[j].Message == nil {
			return true
		}
		return candidates[i].Message.DependentRoot.String() < candidates[j].Message.DependentRoot.String()
	})
	var selected *common.SignedProposerPreferences
	for _, candidate := range candidates {
		if candidate == nil || candidate.Message == nil {
			return nil, errors.New("stored signed proposer preferences are incomplete")
		}
		message := candidate.Message
		if uint64(message.ProposalSlot) != slot || uint64(message.ValidatorIndex) != validatorIndex || message.DependentRoot == (phase0.Root{}) {
			return nil, errors.New("stored signed proposer preferences do not match requested duty")
		}
		verified, verifyErr := ssz.VerifySignature(message, api.opts.EthNetDetails.DomainProposerPreferences, proposerPubkey[:], candidate.Signature[:])
		if verifyErr != nil || !verified {
			return nil, errors.New("stored signed proposer preferences have invalid signature")
		}
		if selected == nil {
			selected = candidate
			continue
		}
		if selected.Message.FeeRecipient != message.FeeRecipient || selected.Message.TargetGasLimit != message.TargetGasLimit {
			return nil, errAmbiguousGloasProposerPreferences
		}
	}
	return selected, nil
}

// isGasLimitTargetCompatible is the Gloas consensus helper. The V6 simulator
// receives target_gas_limit as registered_gas_limit and performs this check
// against the canonical parent. The relay also applies it locally whenever it
// has the parent payload in its reveal cache.
func isGasLimitTargetCompatible(parentGasLimit, gasLimit, targetGasLimit uint64) bool {
	gasLimitDifference := parentGasLimit / 1024
	if gasLimitDifference < 1 {
		gasLimitDifference = 1
	}
	gasLimitDifference--
	minGasLimit := parentGasLimit - min(parentGasLimit, gasLimitDifference)
	maxGasLimit := parentGasLimit + gasLimitDifference
	if maxGasLimit < parentGasLimit {
		maxGasLimit = math.MaxUint64
	}
	if targetGasLimit >= minGasLimit && targetGasLimit <= maxGasLimit {
		return gasLimit == targetGasLimit
	}
	if targetGasLimit > maxGasLimit {
		return gasLimit == maxGasLimit
	}
	return gasLimit == minGasLimit
}
