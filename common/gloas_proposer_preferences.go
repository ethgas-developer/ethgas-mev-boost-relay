package common

import (
	"encoding/binary"
	"fmt"

	"github.com/attestantio/go-eth2-client/spec/bellatrix"
	"github.com/attestantio/go-eth2-client/spec/phase0"
)

const (
	proposerPreferencesSSZSize       = 76
	signedProposerPreferencesSSZSize = proposerPreferencesSSZSize + 96
)

// DomainTypeProposerPreferences is DOMAIN_PROPOSER_PREFERENCES from the
// consensus Gloas specification.
var DomainTypeProposerPreferences = phase0.DomainType{0x0d, 0x00, 0x00, 0x00}

// ProposerPreferences is the consensus-layer, branch-scoped preference a
// validator gossips before its proposal slot. It supersedes the fee recipient
// and target gas limit in ValidatorRegistrationV1 from Gloas onwards.
type ProposerPreferences struct {
	DependentRoot  phase0.Root                `json:"dependent_root"`
	ProposalSlot   phase0.Slot                `json:"proposal_slot"`
	ValidatorIndex phase0.ValidatorIndex      `json:"validator_index"`
	FeeRecipient   bellatrix.ExecutionAddress `json:"fee_recipient"`
	TargetGasLimit Uint64String               `json:"target_gas_limit"`
}

// SignedProposerPreferences is emitted by beacon nodes on the
// proposer_preferences SSE topic after gossip validation. Relays verify the
// signature again before accepting it into shared storage.
type SignedProposerPreferences struct {
	Message   *ProposerPreferences `json:"message"`
	Signature phase0.BLSSignature  `json:"signature"`
}

func (p *ProposerPreferences) MarshalSSZ() ([]byte, error) {
	if p == nil {
		return nil, fmt.Errorf("nil proposer preferences")
	}
	encoded := make([]byte, proposerPreferencesSSZSize)
	copy(encoded[0:32], p.DependentRoot[:])
	binary.LittleEndian.PutUint64(encoded[32:40], uint64(p.ProposalSlot))
	binary.LittleEndian.PutUint64(encoded[40:48], uint64(p.ValidatorIndex))
	copy(encoded[48:68], p.FeeRecipient[:])
	binary.LittleEndian.PutUint64(encoded[68:76], uint64(p.TargetGasLimit))
	return encoded, nil
}

func (p *ProposerPreferences) UnmarshalSSZ(input []byte) error {
	if p == nil {
		return fmt.Errorf("nil proposer preferences")
	}
	if len(input) != proposerPreferencesSSZSize {
		return fmt.Errorf("invalid proposer preferences SSZ length: %d", len(input))
	}
	copy(p.DependentRoot[:], input[0:32])
	p.ProposalSlot = phase0.Slot(binary.LittleEndian.Uint64(input[32:40]))
	p.ValidatorIndex = phase0.ValidatorIndex(binary.LittleEndian.Uint64(input[40:48]))
	copy(p.FeeRecipient[:], input[48:68])
	p.TargetGasLimit = Uint64String(binary.LittleEndian.Uint64(input[68:76]))
	return nil
}

func (p *ProposerPreferences) HashTreeRoot() ([32]byte, error) {
	if p == nil {
		return [32]byte{}, fmt.Errorf("nil proposer preferences")
	}
	var feeRecipientRoot [32]byte
	copy(feeRecipientRoot[:], p.FeeRecipient[:])
	return merkleizeFixed([][32]byte{
		p.DependentRoot,
		uint64Root(uint64(p.ProposalSlot)),
		uint64Root(uint64(p.ValidatorIndex)),
		feeRecipientRoot,
		uint64Root(uint64(p.TargetGasLimit)),
	}, 8), nil
}

func (p *SignedProposerPreferences) MarshalSSZ() ([]byte, error) {
	if p == nil || p.Message == nil {
		return nil, fmt.Errorf("incomplete signed proposer preferences")
	}
	message, err := p.Message.MarshalSSZ()
	if err != nil {
		return nil, err
	}
	encoded := make([]byte, signedProposerPreferencesSSZSize)
	copy(encoded[0:proposerPreferencesSSZSize], message)
	copy(encoded[proposerPreferencesSSZSize:], p.Signature[:])
	return encoded, nil
}

func (p *SignedProposerPreferences) UnmarshalSSZ(input []byte) error {
	if p == nil {
		return fmt.Errorf("nil signed proposer preferences")
	}
	if len(input) != signedProposerPreferencesSSZSize {
		return fmt.Errorf("invalid signed proposer preferences SSZ length: %d", len(input))
	}
	p.Message = new(ProposerPreferences)
	if err := p.Message.UnmarshalSSZ(input[:proposerPreferencesSSZSize]); err != nil {
		return err
	}
	copy(p.Signature[:], input[proposerPreferencesSSZSize:])
	return nil
}
