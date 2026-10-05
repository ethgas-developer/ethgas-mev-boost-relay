package database

import (
	"encoding/json"
	"testing"
	"time"

	"bitbucket.org/infinity-exchange/mev-boost-relay/common"
	"github.com/attestantio/go-eth2-client/spec"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

const testBuilderPubkey = "0xb872a4f5f596ea7dfd695e45afbe4551b405b10dafba98b2d897c58a5047fc288ef2c1bc4216f906ea05d7fdbed61116"

func TestExecutionPayloadEntryToExecutionPayload(t *testing.T) {
	filename := "../testdata/executionPayloadCapella_Goerli.json.gz"
	payloadBytes := common.LoadGzippedBytes(t, filename)
	entry := &ExecutionPayloadEntry{
		ID:         123,
		Slot:       5552306,
		InsertedAt: time.Unix(1685616301, 0),

		ProposerPubkey: "0x8559727ee65c295279332198029c939557f4d2aba0751fc55f71d0733b8aa17cd0301232a7f21a895f81eacf55c97ec4",
		BlockHash:      "0x1bafdc454116b605005364976b134d761dd736cb4788d25c835783b46daeb121",
		Version:        common.ForkVersionStringCapella,
		Payload:        string(payloadBytes),
	}

	payload, err := ExecutionPayloadEntryToExecutionPayload(entry)
	require.NoError(t, err)
	require.Equal(t, "0x1bafdc454116b605005364976b134d761dd736cb4788d25c835783b46daeb121", payload.Capella.BlockHash.String())
}

func TestExecutionPayloadEntryToExecutionPayloadDeneb(t *testing.T) {
	filename := "../testdata/executionPayloadAndBlobsBundleDeneb_Goerli.json.gz"
	payloadBytes := common.LoadGzippedBytes(t, filename)
	entry := &ExecutionPayloadEntry{
		ID:         123,
		Slot:       7432891,
		InsertedAt: time.Unix(1685616301, 0),

		ProposerPubkey: "0x8559727ee65c295279332198029c939557f4d2aba0751fc55f71d0733b8aa17cd0301232a7f21a895f81eacf55c97ec4",
		BlockHash:      "0xbd1ae4f7edb2315d2df70a8d9881fab8d6763fb1c00533ae729050928c38d05a",
		Version:        common.ForkVersionStringDeneb,
		Payload:        string(payloadBytes),
	}

	payload, err := ExecutionPayloadEntryToExecutionPayload(entry)
	require.NoError(t, err)
	require.Equal(t, "0xbd1ae4f7edb2315d2df70a8d9881fab8d6763fb1c00533ae729050928c38d05a", payload.Deneb.ExecutionPayload.BlockHash.String())
	require.Len(t, payload.Deneb.BlobsBundle.Blobs, 1)
}

func TestGloasPayloadEntryPreservesAmsterdamFields(t *testing.T) {
	payload, _, _ := common.CreateTestBlockSubmission(t, testBuilderPubkey, uint256.NewInt(10), &common.CreateTestBlockSubmissionOpts{
		Slot:    42,
		Version: spec.DataVersionFulu,
	})
	gloasPayload, err := common.NewExecutionPayloadGloas(payload.Fulu.ExecutionPayload, []byte{0xc0, 0x01}, 42)
	require.NoError(t, err)
	executionRequests := common.NewExecutionRequestsGloas(payload.Fulu.ExecutionRequests)
	executionRequests.BuilderDeposits = []*common.BuilderDepositRequestGloas{{Amount: 1}}
	executionRequests.BuilderExits = []*common.BuilderExitRequestGloas{{}}
	payload.Gloas = &common.GloasPayloadContents{
		ExecutionPayload:  gloasPayload,
		BlobsBundle:       payload.Fulu.BlobsBundle,
		ExecutionRequests: executionRequests,
	}

	entry, err := PayloadToExecPayloadEntry(payload)
	require.NoError(t, err)
	require.Equal(t, common.ForkVersionStringGloas, entry.Version)
	require.Contains(t, entry.Payload, `"block_access_list":"0xc001"`)
	require.Contains(t, entry.Payload, `"slot_number":"42"`)
	stored := new(common.GloasPayloadContents)
	require.NoError(t, json.Unmarshal([]byte(entry.Payload), stored))
	require.Equal(t, executionRequests, stored.ExecutionRequests)

	legacyResponse, err := ExecutionPayloadEntryToExecutionPayload(entry)
	require.NoError(t, err)
	require.Equal(t, spec.DataVersionFulu, legacyResponse.Version)
	require.Equal(t, gloasPayload.BlockHash, legacyResponse.Fulu.ExecutionPayload.BlockHash)
}
