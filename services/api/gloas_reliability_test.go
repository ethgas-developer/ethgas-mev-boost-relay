package api

import (
	"testing"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/stretchr/testify/require"
)

func TestGloasOnlySigningIdentity(t *testing.T) {
	previousDisabled := disableEthgasMarketAPI
	disableEthgasMarketAPI = true
	t.Cleanup(func() { disableEthgasMarketAPI = previousDisabled })

	backend := newTestBackend(t, 1)
	opts := backend.relay.opts
	builderIndex := uint64(7)
	opts.GloasBuilderIndex = &builderIndex
	opts.EthNetDetails.GloasForkVersionHex = "0x80000038"
	opts.BlockBuilderAPI = false
	opts.ProposerAPI = false

	relay, err := NewRelayAPI(opts)
	require.NoError(t, err)
	require.NotNil(t, relay.publicKey)
	require.NotEqual(t, phase0.BLSPubKey{}, *relay.publicKey)
	require.Equal(t, *backend.relay.publicKey, *relay.publicKey)
}
