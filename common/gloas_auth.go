package common

import (
	"encoding/binary"
	"fmt"

	"github.com/attestantio/go-eth2-client/spec/phase0"
)

const (
	MaxBuilderRequestAuthDataSize  = 4096
	requestAuthSSZFixedSize        = 12
	signedRequestAuthSSZFixedSize  = 100
	builderPreferencesSSZSize      = 8
	builderPreferencesReqFixedSize = 12
)

// DomainTypeRequestAuth is DOMAIN_REQUEST_AUTH from builder-specs. It is an
// application domain: the domain is computed with the genesis fork version
// and a zero genesis validators root.
var DomainTypeRequestAuth = phase0.DomainType{0x0b, 0x00, 0x00, 0x01}

// BuilderRequestAuth authorizes a builder API request for one proposal slot.
// Lighthouse currently calls this wire object RequestAuth; the field layout is
// identical to BuilderRequestAuth in the current builder-specs terminology.
type BuilderRequestAuth struct {
	Data HexBytes    `json:"data"`
	Slot phase0.Slot `json:"slot"`
}

// SignedBuilderRequestAuth binds BuilderRequestAuth to the proposer public key
// in the request path.
type SignedBuilderRequestAuth struct {
	Message   *BuilderRequestAuth `json:"message"`
	Signature phase0.BLSSignature `json:"signature"`
}

type BuilderPreferences struct {
	MaxExecutionPayment phase0.Gwei `json:"max_execution_payment"`
}

type BuilderPreferencesRequest struct {
	Preferences *BuilderPreferences       `json:"preferences"`
	Auth        *SignedBuilderRequestAuth `json:"auth"`
}

func (a *BuilderRequestAuth) validate() error {
	if a == nil {
		return fmt.Errorf("missing request auth message")
	}
	if len(a.Data) == 0 {
		return fmt.Errorf("request auth data must not be empty")
	}
	if len(a.Data) > MaxBuilderRequestAuthDataSize {
		return fmt.Errorf("request auth data exceeds %d bytes", MaxBuilderRequestAuthDataSize)
	}
	return nil
}

func (a *BuilderRequestAuth) MarshalSSZ() ([]byte, error) {
	if err := a.validate(); err != nil {
		return nil, err
	}
	encoded := make([]byte, requestAuthSSZFixedSize+len(a.Data))
	binary.LittleEndian.PutUint32(encoded[0:4], requestAuthSSZFixedSize)
	binary.LittleEndian.PutUint64(encoded[4:12], uint64(a.Slot))
	copy(encoded[12:], a.Data)
	return encoded, nil
}

func (a *BuilderRequestAuth) UnmarshalSSZ(input []byte) error {
	if a == nil {
		return fmt.Errorf("nil request auth")
	}
	if len(input) < requestAuthSSZFixedSize {
		return fmt.Errorf("request auth SSZ is too short: %d", len(input))
	}
	dataOffset := int(binary.LittleEndian.Uint32(input[0:4]))
	if dataOffset != requestAuthSSZFixedSize {
		return fmt.Errorf("invalid request auth data offset: %d", dataOffset)
	}
	a.Slot = phase0.Slot(binary.LittleEndian.Uint64(input[4:12]))
	a.Data = append(a.Data[:0], input[dataOffset:]...)
	return a.validate()
}

func (a *BuilderRequestAuth) HashTreeRoot() ([32]byte, error) {
	if err := a.validate(); err != nil {
		return [32]byte{}, err
	}
	return merkleizeFixed([][32]byte{
		boundedByteListRoot(a.Data, MaxBuilderRequestAuthDataSize),
		uint64Root(uint64(a.Slot)),
	}, 2), nil
}

func (a *SignedBuilderRequestAuth) MarshalSSZ() ([]byte, error) {
	if a == nil || a.Message == nil {
		return nil, fmt.Errorf("incomplete signed request auth")
	}
	message, err := a.Message.MarshalSSZ()
	if err != nil {
		return nil, err
	}
	encoded := make([]byte, signedRequestAuthSSZFixedSize+len(message))
	binary.LittleEndian.PutUint32(encoded[0:4], signedRequestAuthSSZFixedSize)
	copy(encoded[4:100], a.Signature[:])
	copy(encoded[100:], message)
	return encoded, nil
}

func (a *SignedBuilderRequestAuth) UnmarshalSSZ(input []byte) error {
	if a == nil {
		return fmt.Errorf("nil signed request auth")
	}
	if len(input) < signedRequestAuthSSZFixedSize+requestAuthSSZFixedSize+1 {
		return fmt.Errorf("signed request auth SSZ is too short: %d", len(input))
	}
	messageOffset := int(binary.LittleEndian.Uint32(input[0:4]))
	if messageOffset != signedRequestAuthSSZFixedSize {
		return fmt.Errorf("invalid signed request auth message offset: %d", messageOffset)
	}
	copy(a.Signature[:], input[4:100])
	a.Message = new(BuilderRequestAuth)
	return a.Message.UnmarshalSSZ(input[messageOffset:])
}

func (p *BuilderPreferences) MarshalSSZ() ([]byte, error) {
	if p == nil {
		return nil, fmt.Errorf("nil builder preferences")
	}
	encoded := make([]byte, builderPreferencesSSZSize)
	binary.LittleEndian.PutUint64(encoded, uint64(p.MaxExecutionPayment))
	return encoded, nil
}

func (p *BuilderPreferences) UnmarshalSSZ(input []byte) error {
	if p == nil {
		return fmt.Errorf("nil builder preferences")
	}
	if len(input) != builderPreferencesSSZSize {
		return fmt.Errorf("invalid builder preferences SSZ length: %d", len(input))
	}
	p.MaxExecutionPayment = phase0.Gwei(binary.LittleEndian.Uint64(input))
	return nil
}

func (p *BuilderPreferencesRequest) MarshalSSZ() ([]byte, error) {
	if p == nil || p.Preferences == nil || p.Auth == nil {
		return nil, fmt.Errorf("incomplete builder preferences request")
	}
	preferences, err := p.Preferences.MarshalSSZ()
	if err != nil {
		return nil, err
	}
	auth, err := p.Auth.MarshalSSZ()
	if err != nil {
		return nil, err
	}
	encoded := make([]byte, builderPreferencesReqFixedSize+len(auth))
	copy(encoded[0:8], preferences)
	binary.LittleEndian.PutUint32(encoded[8:12], builderPreferencesReqFixedSize)
	copy(encoded[12:], auth)
	return encoded, nil
}

func (p *BuilderPreferencesRequest) UnmarshalSSZ(input []byte) error {
	if p == nil {
		return fmt.Errorf("nil builder preferences request")
	}
	if len(input) < builderPreferencesReqFixedSize+signedRequestAuthSSZFixedSize+requestAuthSSZFixedSize+1 {
		return fmt.Errorf("builder preferences request SSZ is too short: %d", len(input))
	}
	authOffset := int(binary.LittleEndian.Uint32(input[8:12]))
	if authOffset != builderPreferencesReqFixedSize {
		return fmt.Errorf("invalid builder preferences auth offset: %d", authOffset)
	}
	p.Preferences = new(BuilderPreferences)
	if err := p.Preferences.UnmarshalSSZ(input[0:8]); err != nil {
		return err
	}
	p.Auth = new(SignedBuilderRequestAuth)
	return p.Auth.UnmarshalSSZ(input[authOffset:])
}
