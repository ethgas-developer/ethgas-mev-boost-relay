package datastore

import (
	"testing"
	"time"

	"bitbucket.org/infinity-exchange/mev-boost-relay/common"
	"github.com/attestantio/go-eth2-client/spec/bellatrix"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/stretchr/testify/require"
)

func TestGloasSignedProposerPreferencesBranchScopedStorage(t *testing.T) {
	cache := setupTestRedis(t)
	preference := &common.SignedProposerPreferences{
		Message: &common.ProposerPreferences{
			DependentRoot:  phase0.Root{0x01},
			ProposalSlot:   42,
			ValidatorIndex: 17,
			FeeRecipient:   bellatrix.ExecutionAddress{0xaa},
			TargetGasLimit: 60_000_000,
		},
		Signature: phase0.BLSSignature{0xbb},
	}

	require.NoError(t, cache.SaveGloasSignedProposerPreferences(preference, time.Minute))
	require.NoError(t, cache.SaveGloasSignedProposerPreferences(preference, time.Minute))

	stored, err := cache.GetGloasSignedProposerPreferences(42, 17, preference.Message.DependentRoot.String())
	require.NoError(t, err)
	require.Equal(t, preference, stored)

	otherBranch := *preference
	otherBranchMessage := *preference.Message
	otherBranchMessage.DependentRoot = phase0.Root{0x02}
	otherBranch.Message = &otherBranchMessage
	require.NoError(t, cache.SaveGloasSignedProposerPreferences(&otherBranch, time.Minute))

	all, err := cache.GetGloasSignedProposerPreferencesForDuty(42, 17)
	require.NoError(t, err)
	require.Len(t, all, 2)

	conflict := *preference
	conflictMessage := *preference.Message
	conflictMessage.FeeRecipient[0] = 0xcc
	conflict.Message = &conflictMessage
	require.ErrorIs(t, cache.SaveGloasSignedProposerPreferences(&conflict, time.Minute), ErrGloasProposerPreferencesConflict)

	// An exact-root equivocation poisons the whole proposal duty. Every relay
	// replica sees the shared marker and fails closed rather than serving the
	// value it happened to observe first.
	all, err = cache.GetGloasSignedProposerPreferencesForDuty(42, 17)
	require.ErrorIs(t, err, ErrGloasProposerPreferencesConflict)
	require.Empty(t, all)
}
