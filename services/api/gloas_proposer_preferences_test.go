package api

import (
	"errors"
	"testing"

	"bitbucket.org/infinity-exchange/mev-boost-relay/beaconclient"
	"bitbucket.org/infinity-exchange/mev-boost-relay/common"
	"bitbucket.org/infinity-exchange/mev-boost-relay/datastore"
	"github.com/attestantio/go-eth2-client/spec/bellatrix"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/flashbots/go-boost-utils/bls"
	"github.com/flashbots/go-boost-utils/ssz"
	"github.com/flashbots/go-boost-utils/utils"
	"github.com/stretchr/testify/require"
)

func signedGloasProposerPreferences(
	t *testing.T,
	domain phase0.Domain,
	sk *bls.SecretKey,
	slot uint64,
	validatorIndex uint64,
	dependentRoot phase0.Root,
	feeRecipient bellatrix.ExecutionAddress,
	targetGasLimit uint64,
) *common.SignedProposerPreferences {
	t.Helper()
	message := &common.ProposerPreferences{
		DependentRoot:  dependentRoot,
		ProposalSlot:   phase0.Slot(slot),
		ValidatorIndex: phase0.ValidatorIndex(validatorIndex),
		FeeRecipient:   feeRecipient,
		TargetGasLimit: common.Uint64String(targetGasLimit),
	}
	signature, err := ssz.SignMessage(message, domain, sk)
	require.NoError(t, err)
	return &common.SignedProposerPreferences{Message: message, Signature: signature}
}

func TestGloasProposerPreferencesValidationAndForkSafety(t *testing.T) {
	backend := newTestBackend(t, 1)
	backend.relay.headSlot.Store(32)
	domain, err := common.ComputeDomain(
		common.DomainTypeProposerPreferences,
		"0x80000038",
		backend.relay.opts.EthNetDetails.GenesisValidatorsRootHex,
	)
	require.NoError(t, err)
	backend.relay.opts.EthNetDetails.DomainProposerPreferences = domain

	sk, blsPubkey, err := bls.GenerateNewKeypair()
	require.NoError(t, err)
	pubkey, err := utils.BlsPublicKeyToPublicKey(blsPubkey)
	require.NoError(t, err)
	const validatorIndex = uint64(17)
	backend.datastore.SetKnownValidator(common.NewPubkeyHex(pubkey.String()), validatorIndex)

	first := signedGloasProposerPreferences(t, domain, sk, testSlot, validatorIndex, phase0.Root{0x01}, testAddress, 60_000_000)
	backend.relay.processGloasProposerPreferences(beaconclient.ProposerPreferencesEvent{Version: "gloas", Data: first})
	resolved, err := backend.relay.resolveGloasProposerPreferences(testSlot, validatorIndex, pubkey)
	require.NoError(t, err)
	require.Equal(t, first, resolved)

	// A second BN-validated fork root with identical bid-affecting values is
	// safe: whichever branch wins, the relay constructs the same bid fields.
	sameValuesOtherRoot := signedGloasProposerPreferences(t, domain, sk, testSlot, validatorIndex, phase0.Root{0x02}, testAddress, 60_000_000)
	backend.relay.processGloasProposerPreferences(beaconclient.ProposerPreferencesEvent{Version: "gloas", Data: sameValuesOtherRoot})
	_, err = backend.relay.resolveGloasProposerPreferences(testSlot, validatorIndex, pubkey)
	require.NoError(t, err)

	// Without a fork-choice store, differing values across dependent roots are
	// ambiguous and must fail closed.
	conflictingRoot := signedGloasProposerPreferences(t, domain, sk, testSlot, validatorIndex, phase0.Root{0x03}, testAddress2, 61_000_000)
	backend.relay.processGloasProposerPreferences(beaconclient.ProposerPreferencesEvent{Version: "gloas", Data: conflictingRoot})
	_, err = backend.relay.resolveGloasProposerPreferences(testSlot, validatorIndex, pubkey)
	require.ErrorIs(t, err, errAmbiguousGloasProposerPreferences)

	// A second valid signature with different contents for an already-seen
	// dependent root is an equivocation. The Redis conflict marker makes all
	// replicas reject the duty, including replicas that stored the first value.
	sameRootEquivocation := signedGloasProposerPreferences(t, domain, sk, testSlot, validatorIndex, phase0.Root{0x01}, testAddress2, 60_000_000)
	backend.relay.processGloasProposerPreferences(beaconclient.ProposerPreferencesEvent{Version: "gloas", Data: sameRootEquivocation})
	_, err = backend.relay.resolveGloasProposerPreferences(testSlot, validatorIndex, pubkey)
	require.ErrorIs(t, err, datastore.ErrGloasProposerPreferencesConflict)

	wrongProposer := pubkey
	wrongProposer[0] ^= 0xff
	_, err = backend.relay.resolveGloasProposerPreferences(testSlot, validatorIndex, wrongProposer)
	require.ErrorContains(t, err, "proposer pubkey does not match")
}

func TestGloasProposerPreferencesRejectInvalidSignatureAndSlot(t *testing.T) {
	backend := newTestBackend(t, 1)
	backend.relay.headSlot.Store(32)
	domain, err := common.ComputeDomain(common.DomainTypeProposerPreferences, "0x80000038", backend.relay.opts.EthNetDetails.GenesisValidatorsRootHex)
	require.NoError(t, err)
	backend.relay.opts.EthNetDetails.DomainProposerPreferences = domain

	sk, blsPubkey, err := bls.GenerateNewKeypair()
	require.NoError(t, err)
	pubkey, err := utils.BlsPublicKeyToPublicKey(blsPubkey)
	require.NoError(t, err)
	backend.datastore.SetKnownValidator(common.NewPubkeyHex(pubkey.String()), 17)

	invalid := signedGloasProposerPreferences(t, domain, sk, testSlot, 17, phase0.Root{0x01}, testAddress, 60_000_000)
	invalid.Signature[0] ^= 0xff
	backend.relay.processGloasProposerPreferences(beaconclient.ProposerPreferencesEvent{Version: "gloas", Data: invalid})
	_, err = backend.relay.resolveGloasProposerPreferences(testSlot, 17, pubkey)
	require.True(t, errors.Is(err, errMissingGloasProposerPreferences))

	stale := signedGloasProposerPreferences(t, domain, sk, 32, 17, phase0.Root{0x02}, testAddress, 60_000_000)
	backend.relay.processGloasProposerPreferences(beaconclient.ProposerPreferencesEvent{Version: "gloas", Data: stale})
	entries, err := backend.redis.GetGloasSignedProposerPreferencesForDuty(32, 17)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestIsGasLimitTargetCompatible(t *testing.T) {
	const parent = uint64(30_000_000)
	delta := parent/1024 - 1
	require.True(t, isGasLimitTargetCompatible(parent, parent, parent))
	require.True(t, isGasLimitTargetCompatible(parent, parent+delta, 60_000_000))
	require.True(t, isGasLimitTargetCompatible(parent, parent-delta, 1))
	require.False(t, isGasLimitTargetCompatible(parent, parent+delta-1, 60_000_000))
	require.False(t, isGasLimitTargetCompatible(parent, parent-delta+1, 1))
}
