package common

import (
	"encoding/binary"
	"encoding/json"
	"testing"

	builderAPIElectra "github.com/attestantio/go-builder-client/api/electra"
	builderAPIFulu "github.com/attestantio/go-builder-client/api/fulu"
	builderAPIV1 "github.com/attestantio/go-builder-client/api/v1"
	"github.com/attestantio/go-eth2-client/spec/bellatrix"
	"github.com/attestantio/go-eth2-client/spec/capella"
	"github.com/attestantio/go-eth2-client/spec/deneb"
	"github.com/attestantio/go-eth2-client/spec/electra"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/flashbots/go-boost-utils/bls"
	"github.com/flashbots/go-boost-utils/ssz"
	"github.com/flashbots/go-boost-utils/utils"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func TestGloasRequestAuthJSONSSZAndSignature(t *testing.T) {
	details, err := NewEthNetworkDetails(EthNetworkMainnet)
	require.NoError(t, err)
	sk, blsPubkey, err := bls.GenerateNewKeypair()
	require.NoError(t, err)
	pubkey, err := utils.BlsPublicKeyToPublicKey(blsPubkey)
	require.NoError(t, err)
	message := &BuilderRequestAuth{
		Data: []byte("http://relay.example:9062"),
		Slot: 42,
	}
	signature, err := ssz.SignMessage(message, details.DomainBuilderRequestAuth, sk)
	require.NoError(t, err)
	original := &SignedBuilderRequestAuth{Message: message, Signature: signature}

	encodedJSON, err := json.Marshal(original)
	require.NoError(t, err)
	decodedJSON := new(SignedBuilderRequestAuth)
	require.NoError(t, json.Unmarshal(encodedJSON, decodedJSON))
	require.Equal(t, original, decodedJSON)

	encodedSSZ, err := original.MarshalSSZ()
	require.NoError(t, err)
	require.Equal(t, uint32(signedRequestAuthSSZFixedSize), binary.LittleEndian.Uint32(encodedSSZ[0:4]))
	decodedSSZ := new(SignedBuilderRequestAuth)
	require.NoError(t, decodedSSZ.UnmarshalSSZ(encodedSSZ))
	require.Equal(t, original, decodedSSZ)
	verified, err := ssz.VerifySignature(decodedSSZ.Message, details.DomainBuilderRequestAuth, pubkey[:], decodedSSZ.Signature[:])
	require.NoError(t, err)
	require.True(t, verified)

	preferences := &BuilderPreferencesRequest{
		Preferences: &BuilderPreferences{MaxExecutionPayment: 1234},
		Auth:        original,
	}
	preferencesSSZ, err := preferences.MarshalSSZ()
	require.NoError(t, err)
	decodedPreferences := new(BuilderPreferencesRequest)
	require.NoError(t, decodedPreferences.UnmarshalSSZ(preferencesSSZ))
	require.Equal(t, preferences, decodedPreferences)
}

func TestGloasRequestAuthRejectsInvalidSSZ(t *testing.T) {
	auth := &BuilderRequestAuth{Data: nil, Slot: 42}
	_, err := auth.MarshalSSZ()
	require.Error(t, err)

	auth.Data = make([]byte, MaxBuilderRequestAuthDataSize+1)
	_, err = auth.MarshalSSZ()
	require.Error(t, err)

	valid := &BuilderRequestAuth{Data: []byte("relay"), Slot: 42}
	encoded, err := valid.MarshalSSZ()
	require.NoError(t, err)
	encoded[0] = 13
	require.Error(t, new(BuilderRequestAuth).UnmarshalSSZ(encoded))

	require.Error(t, json.Unmarshal([]byte(`{"data":"72656c6179","slot":"42"}`), new(BuilderRequestAuth)))
}

func TestNewExecutionPayloadBid(t *testing.T) {
	parentHash := repeatedHash(0x01)
	blockHash := repeatedHash(0x03)
	parentRoot := phase0.Root(repeatedHash(0x02))
	prevRandao := repeatedHash(0x04)
	feeRecipient := bellatrix.ExecutionAddress{}
	for i := range feeRecipient {
		feeRecipient[i] = 0x05
	}
	executionBeneficiary := bellatrix.ExecutionAddress{}
	for i := range executionBeneficiary {
		executionBeneficiary[i] = 0x06
	}

	legacy := &builderAPIElectra.BuilderBid{
		Header: &deneb.ExecutionPayloadHeader{
			ParentHash:   parentHash,
			BlockHash:    blockHash,
			PrevRandao:   prevRandao,
			FeeRecipient: executionBeneficiary,
			GasLimit:     30_000_000,
		},
		BlobKZGCommitments: []deneb.KZGCommitment{},
		ExecutionRequests:  &electra.ExecutionRequests{},
		Value:              uint256.MustFromDecimal("11000000001"),
	}

	bid, err := NewExecutionPayloadBid(legacy, NewExecutionRequestsGloas(legacy.ExecutionRequests), 42, parentRoot, feeRecipient, 7)
	require.NoError(t, err)
	require.Equal(t, parentHash, bid.ParentBlockHash)
	require.Equal(t, parentRoot, bid.ParentBlockRoot)
	require.Equal(t, blockHash, bid.BlockHash)
	require.Equal(t, prevRandao, bid.PrevRandao)
	require.Equal(t, feeRecipient, bid.FeeRecipient)
	require.Equal(t, uint64(30_000_000), uint64(bid.GasLimit))
	require.Equal(t, uint64(7), uint64(bid.BuilderIndex))
	require.Equal(t, phase0.Slot(42), bid.Slot)
	require.Equal(t, phase0.Gwei(0), bid.Value)
	require.Equal(t, phase0.Gwei(11), bid.ExecutionPayment)

	encoded, err := json.Marshal(bid)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"parent_block_hash":"0x0101010101010101010101010101010101010101010101010101010101010101",
		"parent_block_root":"0x0202020202020202020202020202020202020202020202020202020202020202",
		"block_hash":"0x0303030303030303030303030303030303030303030303030303030303030303",
		"prev_randao":"0x0404040404040404040404040404040404040404040404040404040404040404",
		"fee_recipient":"0x0505050505050505050505050505050505050505",
		"gas_limit":"30000000",
		"builder_index":"7",
		"slot":"42",
		"value":"0",
		"execution_payment":"11",
		"blob_kzg_commitments":[],
		"execution_requests_root":"0x87b69a306c8e430d0857f7c4ac5e27cecffa1108d43c2e5df7388056fea7a423"
	}`, string(encoded))

	requestsWithBuilderOps := NewExecutionRequestsGloas(legacy.ExecutionRequests)
	requestsWithBuilderOps.BuilderDeposits = []*BuilderDepositRequestGloas{{
		Pubkey:                repeatedPubkey(0x0a),
		WithdrawalCredentials: phase0.Root(repeatedHash(0x0b)),
		Amount:                1,
		Signature:             repeatedSignature(0x0c),
	}}
	requestsWithBuilderOps.BuilderExits = []*BuilderExitRequestGloas{{
		SourceAddress: repeatedExecutionAddress(0x0d),
		Pubkey:        repeatedPubkey(0x0e),
	}}
	bidWithBuilderOps, err := NewExecutionPayloadBid(legacy, requestsWithBuilderOps, 42, parentRoot, feeRecipient, 7)
	require.NoError(t, err)
	expectedRequestsRoot, err := requestsWithBuilderOps.HashTreeRoot()
	require.NoError(t, err)
	require.Equal(t, phase0.Root(expectedRequestsRoot), bidWithBuilderOps.ExecutionRequestsRoot)
	require.NotEqual(t, bid.ExecutionRequestsRoot, bidWithBuilderOps.ExecutionRequestsRoot)
}

func TestExecutionPayloadBidHashTreeRootMatchesLighthouse(t *testing.T) {
	feeRecipient := bellatrix.ExecutionAddress{}
	for i := range feeRecipient {
		feeRecipient[i] = 0x05
	}
	bid := &ExecutionPayloadBid{
		ParentBlockHash:       repeatedHash(0x01),
		ParentBlockRoot:       phase0.Root(repeatedHash(0x02)),
		BlockHash:             repeatedHash(0x03),
		PrevRandao:            repeatedHash(0x04),
		FeeRecipient:          feeRecipient,
		GasLimit:              30_000_000,
		BuilderIndex:          7,
		Slot:                  42,
		Value:                 11,
		ExecutionPayment:      11,
		BlobKZGCommitments:    []deneb.KZGCommitment{},
		ExecutionRequestsRoot: phase0.Root(repeatedHash(0x06)),
	}

	root, err := bid.HashTreeRoot()
	require.NoError(t, err)
	// Generated independently with Lighthouse's ExecutionPayloadBid<MainnetEthSpec>
	// TreeHash implementation at the Gloas test branch pinned in LOCALTEST.md.
	require.Equal(t, "0x9c8b1fd45cd177a1bd737ecb68f4af38cc81f259a511abf65ac440a55ec7e6d3", phase0.Root(root).String())

	var commitment deneb.KZGCommitment
	for i := range commitment {
		commitment[i] = 0x07
	}
	bid.BlobKZGCommitments = []deneb.KZGCommitment{commitment}
	root, err = bid.HashTreeRoot()
	require.NoError(t, err)
	require.Equal(t, "0xba9032bb34caed046f5ed96907ca74b27d8b6c84d8a4b5cf02e476544d78d34a", phase0.Root(root).String())
}

func TestSignedExecutionPayloadBidSSZRoundTrip(t *testing.T) {
	feeRecipient := bellatrix.ExecutionAddress{}
	for i := range feeRecipient {
		feeRecipient[i] = 0x05
	}
	commitment := deneb.KZGCommitment{}
	for i := range commitment {
		commitment[i] = 0x07
	}
	signature := phase0.BLSSignature{}
	for i := range signature {
		signature[i] = 0x08
	}

	original := &SignedExecutionPayloadBid{
		Message: &ExecutionPayloadBid{
			ParentBlockHash:       repeatedHash(0x01),
			ParentBlockRoot:       phase0.Root(repeatedHash(0x02)),
			BlockHash:             repeatedHash(0x03),
			PrevRandao:            repeatedHash(0x04),
			FeeRecipient:          feeRecipient,
			GasLimit:              30_000_000,
			BuilderIndex:          7,
			Slot:                  42,
			Value:                 11,
			ExecutionPayment:      12,
			BlobKZGCommitments:    []deneb.KZGCommitment{commitment},
			ExecutionRequestsRoot: phase0.Root(repeatedHash(0x06)),
		},
		Signature: signature,
	}

	encoded, err := original.MarshalSSZ()
	require.NoError(t, err)
	require.Len(t, encoded, signedExecutionPayloadBidSSZFixedSize+executionPayloadBidSSZFixedSize+len(commitment))
	require.Equal(t, uint32(signedExecutionPayloadBidSSZFixedSize), binary.LittleEndian.Uint32(encoded[0:4]))
	require.Equal(t, signature[:], encoded[4:100])
	require.Equal(t, uint32(executionPayloadBidSSZFixedSize), binary.LittleEndian.Uint32(encoded[288:292]))

	decoded := new(SignedExecutionPayloadBid)
	require.NoError(t, decoded.UnmarshalSSZ(encoded))
	require.Equal(t, original, decoded)

	_, err = (*SignedExecutionPayloadBid)(nil).MarshalSSZ()
	require.Error(t, err)
	require.Error(t, decoded.UnmarshalSSZ(encoded[:len(encoded)-1]))
	encoded[0] = 99
	require.Error(t, decoded.UnmarshalSSZ(encoded))
}

func TestExecutionPayloadGloasPreservesEmptyWithdrawals(t *testing.T) {
	payload := &deneb.ExecutionPayload{
		BaseFeePerGas: uint256.NewInt(1),
		Transactions:  []bellatrix.Transaction{},
		Withdrawals:   []*capella.Withdrawal{},
	}

	gloasPayload, err := NewExecutionPayloadGloas(payload, []byte{0xc0}, 42)
	require.NoError(t, err)
	require.NotNil(t, gloasPayload.Withdrawals)
	require.Empty(t, gloasPayload.Withdrawals)

	denebPayload, err := gloasPayload.AsDeneb()
	require.NoError(t, err)
	require.NotNil(t, denebPayload.Withdrawals)
	require.Empty(t, denebPayload.Withdrawals)
}

func TestGloasSubmitBlockRequestSSZRoundTrip(t *testing.T) {
	feeRecipient := bellatrix.ExecutionAddress{}
	for i := range feeRecipient {
		feeRecipient[i] = 0x05
	}
	basePayload := &deneb.ExecutionPayload{
		ParentHash:    repeatedHash(0x01),
		FeeRecipient:  feeRecipient,
		StateRoot:     phase0.Root(repeatedHash(0x02)),
		ReceiptsRoot:  phase0.Root(repeatedHash(0x03)),
		PrevRandao:    repeatedHash(0x04),
		BlockNumber:   10,
		GasLimit:      30_000_000,
		GasUsed:       42_000,
		Timestamp:     1234,
		ExtraData:     []byte{0xaa, 0xbb},
		BaseFeePerGas: uint256.NewInt(7),
		BlockHash:     repeatedHash(0x06),
		Transactions:  []bellatrix.Transaction{{0x02, 0x01}, {0x03, 0x02, 0x01}},
		Withdrawals:   []*capella.Withdrawal{{Index: 1, ValidatorIndex: 2, Address: feeRecipient, Amount: 3}},
		BlobGasUsed:   4,
		ExcessBlobGas: 5,
	}
	gloasPayload, err := NewExecutionPayloadGloas(basePayload, []byte{0xc1, 0x80}, 42)
	require.NoError(t, err)

	commitment := deneb.KZGCommitment{}
	proof := deneb.KZGProof{}
	for i := range commitment {
		commitment[i] = 0x07
		proof[i] = 0x08
	}
	signature := phase0.BLSSignature{}
	for i := range signature {
		signature[i] = 0x09
	}
	original := &GloasSubmitBlockRequest{
		Message: &builderAPIV1.BidTrace{
			Slot:                 42,
			ParentHash:           basePayload.ParentHash,
			BlockHash:            basePayload.BlockHash,
			ProposerFeeRecipient: feeRecipient,
			GasLimit:             basePayload.GasLimit,
			GasUsed:              basePayload.GasUsed,
			Value:                uint256.NewInt(11),
		},
		ExecutionPayload: gloasPayload,
		BlobsBundle: &builderAPIFulu.BlobsBundle{
			Commitments: []deneb.KZGCommitment{commitment},
			Proofs:      []deneb.KZGProof{proof},
			Blobs:       []deneb.Blob{},
		},
		ExecutionRequests: &ExecutionRequestsGloas{
			Deposits:       []*electra.DepositRequest{},
			Withdrawals:    []*electra.WithdrawalRequest{},
			Consolidations: []*electra.ConsolidationRequest{},
			BuilderDeposits: []*BuilderDepositRequestGloas{{
				Pubkey:                repeatedPubkey(0x0a),
				WithdrawalCredentials: phase0.Root(repeatedHash(0x0b)),
				Amount:                32_000_000_000,
				Signature:             repeatedSignature(0x0c),
			}},
			BuilderExits: []*BuilderExitRequestGloas{{
				SourceAddress: repeatedExecutionAddress(0x0d),
				Pubkey:        repeatedPubkey(0x0e),
			}},
		},
		Signature: signature,
	}

	encoded, err := original.MarshalSSZ()
	require.NoError(t, err)
	require.True(t, IsGloasSubmitBlockRequestSSZ(encoded))
	require.Equal(t, uint32(gloasSubmitBlockRequestSSZFixedSize), binary.LittleEndian.Uint32(encoded[236:240]))
	require.Equal(t, uint32(gloasExecutionPayloadSSZFixedSize), binary.LittleEndian.Uint32(encoded[gloasSubmitBlockRequestSSZFixedSize+436:gloasSubmitBlockRequestSSZFixedSize+440]))

	decoded := new(GloasSubmitBlockRequest)
	require.NoError(t, decoded.UnmarshalSSZ(encoded))
	require.Equal(t, original, decoded)
	require.Len(t, decoded.ExecutionRequests.BuilderDeposits, 1)
	require.Len(t, decoded.ExecutionRequests.BuilderExits, 1)

	encodedJSON, err := json.Marshal(original)
	require.NoError(t, err)
	decodedJSON := new(GloasSubmitBlockRequest)
	require.NoError(t, json.Unmarshal(encodedJSON, decodedJSON))
	require.Equal(t, original, decodedJSON)

	// A Gloas V6 payload must not silently decode as the Fulu V5 payload.
	fulu := new(builderAPIFulu.SubmitBlockRequest)
	require.Error(t, fulu.UnmarshalSSZ(encoded))
}

func TestGloasSubmitBlockRequestSSZRejectsFuluAndBadOffsets(t *testing.T) {
	fulu := &builderAPIFulu.SubmitBlockRequest{
		Message: &builderAPIV1.BidTrace{Value: uint256.NewInt(1)},
		ExecutionPayload: &deneb.ExecutionPayload{
			BaseFeePerGas: uint256.NewInt(1),
			Transactions:  []bellatrix.Transaction{},
			Withdrawals:   []*capella.Withdrawal{},
		},
		BlobsBundle:       &builderAPIFulu.BlobsBundle{},
		ExecutionRequests: &electra.ExecutionRequests{},
	}
	fuluEncoded, err := fulu.MarshalSSZ()
	require.NoError(t, err)
	require.False(t, IsGloasSubmitBlockRequestSSZ(fuluEncoded))
	require.Error(t, new(GloasSubmitBlockRequest).UnmarshalSSZ(fuluEncoded))

	require.Error(t, new(GloasSubmitBlockRequest).UnmarshalSSZ(make([]byte, gloasSubmitBlockRequestSSZFixedSize-1)))
	badOuterOffset := make([]byte, gloasSubmitBlockRequestSSZFixedSize)
	binary.LittleEndian.PutUint32(badOuterOffset[236:240], gloasSubmitBlockRequestSSZFixedSize+4)
	require.Error(t, new(GloasSubmitBlockRequest).UnmarshalSSZ(badOuterOffset))
}

func TestBuilderBlockValidationRequestGloasKeepsAmsterdamFields(t *testing.T) {
	basePayload := &deneb.ExecutionPayload{
		BaseFeePerGas: uint256.NewInt(1),
		Transactions:  []bellatrix.Transaction{},
		Withdrawals:   []*capella.Withdrawal{},
	}
	gloasPayload, err := NewExecutionPayloadGloas(basePayload, []byte{0xc0}, 42)
	require.NoError(t, err)

	parentRoot := phase0.Root(repeatedHash(0x02))
	req := &BuilderBlockValidationRequest{
		Gloas: &GloasSubmitBlockRequest{
			Message:           &builderAPIV1.BidTrace{},
			ExecutionPayload:  gloasPayload,
			BlobsBundle:       &builderAPIFulu.BlobsBundle{},
			ExecutionRequests: NewExecutionRequestsGloas(nil),
		},
		RegisteredGasLimit:    30_000_000,
		ParentBeaconBlockRoot: &parentRoot,
	}

	encoded, err := json.Marshal(req)
	require.NoError(t, err)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(encoded, &fields))
	require.JSONEq(t, `"30000000"`, string(fields["registered_gas_limit"]))
	require.JSONEq(t, `"0x0202020202020202020202020202020202020202020202020202020202020202"`, string(fields["parent_beacon_block_root"]))

	var payloadFields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(fields["execution_payload"], &payloadFields))
	require.JSONEq(t, `"0xc0"`, string(payloadFields["block_access_list"]))
	require.JSONEq(t, `"42"`, string(payloadFields["slot_number"]))
}

func TestExecutionRequestsGloasBuilderRequestsRoundTripAndRoot(t *testing.T) {
	requests := &ExecutionRequestsGloas{
		Deposits:       []*electra.DepositRequest{},
		Withdrawals:    []*electra.WithdrawalRequest{},
		Consolidations: []*electra.ConsolidationRequest{},
		BuilderDeposits: []*BuilderDepositRequestGloas{{
			Pubkey:                repeatedPubkey(0x11),
			WithdrawalCredentials: phase0.Root(repeatedHash(0x22)),
			Amount:                1_234_567_890,
			Signature:             repeatedSignature(0x33),
		}},
		BuilderExits: []*BuilderExitRequestGloas{{
			SourceAddress: repeatedExecutionAddress(0x44),
			Pubkey:        repeatedPubkey(0x55),
		}},
	}

	encoded, err := requests.MarshalSSZ()
	require.NoError(t, err)
	require.Equal(t, uint32(20), binary.LittleEndian.Uint32(encoded[:4]))
	require.Len(t, encoded, 20+gloasBuilderDepositRequestSize+gloasBuilderExitRequestSize)

	decoded := new(ExecutionRequestsGloas)
	require.NoError(t, decoded.UnmarshalSSZ(encoded))
	require.Equal(t, requests, decoded)

	encodedJSON, err := json.Marshal(requests)
	require.NoError(t, err)
	require.Contains(t, string(encodedJSON), `"builder_deposits"`)
	require.Contains(t, string(encodedJSON), `"builder_exits"`)
	decodedJSON := new(ExecutionRequestsGloas)
	require.NoError(t, json.Unmarshal(encodedJSON, decodedJSON))
	require.Equal(t, requests, decodedJSON)

	root, err := requests.HashTreeRoot()
	require.NoError(t, err)
	require.Equal(t, "0xee8088a695ed4b1c4ac0ed7f97e0b1cce91cf768348a19be67eac18bcab2ba75", phase0.Root(root).String())

	emptyRoot, err := NewExecutionRequestsGloas(nil).HashTreeRoot()
	require.NoError(t, err)
	require.NotEqual(t, emptyRoot, root)
}

func TestExecutionRequestsGloasAcceptsLegacyThreeFieldWireShape(t *testing.T) {
	legacy := &electra.ExecutionRequests{
		Deposits:       []*electra.DepositRequest{},
		Withdrawals:    []*electra.WithdrawalRequest{},
		Consolidations: []*electra.ConsolidationRequest{},
	}

	legacySSZ, err := legacy.MarshalSSZ()
	require.NoError(t, err)
	require.Equal(t, uint32(12), binary.LittleEndian.Uint32(legacySSZ[:4]))
	decodedSSZ := new(ExecutionRequestsGloas)
	require.NoError(t, decodedSSZ.UnmarshalSSZ(legacySSZ))
	require.Equal(t, NewExecutionRequestsGloas(legacy), decodedSSZ)

	legacyJSON, err := json.Marshal(legacy)
	require.NoError(t, err)
	decodedJSON := new(ExecutionRequestsGloas)
	require.NoError(t, json.Unmarshal(legacyJSON, decodedJSON))
	require.Equal(t, NewExecutionRequestsGloas(legacy), decodedJSON)
}

func TestExecutionRequestsGloasRejectsInvalidBuilderLists(t *testing.T) {
	requests := NewExecutionRequestsGloas(nil)
	requests.BuilderDeposits = []*BuilderDepositRequestGloas{nil}
	_, err := requests.MarshalSSZ()
	require.ErrorContains(t, err, "null item")

	require.Error(t, json.Unmarshal([]byte(`{
		"deposits":[],"withdrawals":[],"consolidations":[],
		"builder_deposits":[{}],"builder_exits":[]
	}`), new(ExecutionRequestsGloas)))
	require.Error(t, new(ExecutionRequestsGloas).UnmarshalSSZ(make([]byte, 19)))
}

func TestExecutionRequestsGloasAllowsMoreThanFuluDepositLimit(t *testing.T) {
	const depositCount = 8193
	requests := NewExecutionRequestsGloas(nil)
	requests.Deposits = make([]*electra.DepositRequest, depositCount)
	for i := range requests.Deposits {
		requests.Deposits[i] = &electra.DepositRequest{WithdrawalCredentials: make([]byte, 32)}
	}

	encodedSSZ, err := requests.MarshalSSZ()
	require.NoError(t, err)
	decodedSSZ := new(ExecutionRequestsGloas)
	require.NoError(t, decodedSSZ.UnmarshalSSZ(encodedSSZ))
	require.Len(t, decodedSSZ.Deposits, depositCount)

	encodedJSON, err := json.Marshal(requests)
	require.NoError(t, err)
	decodedJSON := new(ExecutionRequestsGloas)
	require.NoError(t, json.Unmarshal(encodedJSON, decodedJSON))
	require.Len(t, decodedJSON.Deposits, depositCount)

	_, err = requests.HashTreeRoot()
	require.NoError(t, err)
}

func TestExecutionRequestsGloasRetainsBoundedListLimits(t *testing.T) {
	tests := map[string]func(*ExecutionRequestsGloas){
		"withdrawals": func(requests *ExecutionRequestsGloas) {
			requests.Withdrawals = make([]*electra.WithdrawalRequest, gloasMaxWithdrawalRequests+1)
			for i := range requests.Withdrawals {
				requests.Withdrawals[i] = new(electra.WithdrawalRequest)
			}
		},
		"consolidations": func(requests *ExecutionRequestsGloas) {
			requests.Consolidations = make([]*electra.ConsolidationRequest, gloasMaxConsolidationRequests+1)
			for i := range requests.Consolidations {
				requests.Consolidations[i] = new(electra.ConsolidationRequest)
			}
		},
		"builder deposits": func(requests *ExecutionRequestsGloas) {
			requests.BuilderDeposits = make([]*BuilderDepositRequestGloas, gloasMaxBuilderDepositRequests+1)
			for i := range requests.BuilderDeposits {
				requests.BuilderDeposits[i] = new(BuilderDepositRequestGloas)
			}
		},
		"builder exits": func(requests *ExecutionRequestsGloas) {
			requests.BuilderExits = make([]*BuilderExitRequestGloas, gloasMaxBuilderExitRequests+1)
			for i := range requests.BuilderExits {
				requests.BuilderExits[i] = new(BuilderExitRequestGloas)
			}
		},
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			requests := NewExecutionRequestsGloas(nil)
			mutate(requests)
			_, err := requests.MarshalSSZ()
			require.ErrorContains(t, err, "exceeds consensus limit")
		})
	}
}

func repeatedHash(value byte) phase0.Hash32 {
	var hash phase0.Hash32
	for i := range hash {
		hash[i] = value
	}
	return hash
}

func repeatedPubkey(value byte) phase0.BLSPubKey {
	var pubkey phase0.BLSPubKey
	for i := range pubkey {
		pubkey[i] = value
	}
	return pubkey
}

func repeatedSignature(value byte) phase0.BLSSignature {
	var signature phase0.BLSSignature
	for i := range signature {
		signature[i] = value
	}
	return signature
}

func repeatedExecutionAddress(value byte) bellatrix.ExecutionAddress {
	var address bellatrix.ExecutionAddress
	for i := range address {
		address[i] = value
	}
	return address
}
