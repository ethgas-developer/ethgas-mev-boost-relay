package common

import (
	"testing"

	"github.com/attestantio/go-eth2-client/spec/bellatrix"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/flashbots/go-boost-utils/bls"
	"github.com/flashbots/go-boost-utils/ssz"
	"github.com/flashbots/go-boost-utils/utils"
	"github.com/stretchr/testify/require"
)

func TestSignedProposerPreferencesSSZAndSignature(t *testing.T) {
	domain, err := ComputeDomain(
		DomainTypeProposerPreferences,
		"0x80000038",
		"0x4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95",
	)
	require.NoError(t, err)

	sk, blsPubkey, err := bls.GenerateNewKeypair()
	require.NoError(t, err)
	pubkey, err := utils.BlsPublicKeyToPublicKey(blsPubkey)
	require.NoError(t, err)

	message := &ProposerPreferences{
		DependentRoot:  phase0.Root{0x01, 0x02, 0x03},
		ProposalSlot:   64,
		ValidatorIndex: 17,
		FeeRecipient:   bellatrix.ExecutionAddress{0xaa, 0xbb, 0xcc},
		TargetGasLimit: 60_000_000,
	}
	signature, err := ssz.SignMessage(message, domain, sk)
	require.NoError(t, err)
	signed := &SignedProposerPreferences{Message: message, Signature: signature}

	encoded, err := signed.MarshalSSZ()
	require.NoError(t, err)
	require.Len(t, encoded, signedProposerPreferencesSSZSize)

	decoded := new(SignedProposerPreferences)
	require.NoError(t, decoded.UnmarshalSSZ(encoded))
	require.Equal(t, signed, decoded)

	verified, err := ssz.VerifySignature(decoded.Message, domain, pubkey[:], decoded.Signature[:])
	require.NoError(t, err)
	require.True(t, verified)
}

func TestSignedProposerPreferencesRejectsMalformedSSZ(t *testing.T) {
	signed := new(SignedProposerPreferences)
	require.Error(t, signed.UnmarshalSSZ(make([]byte, signedProposerPreferencesSSZSize-1)))
	require.Error(t, signed.UnmarshalSSZ(make([]byte, signedProposerPreferencesSSZSize+1)))
}
