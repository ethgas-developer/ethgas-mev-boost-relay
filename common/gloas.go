package common

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"

	builderAPIElectra "github.com/attestantio/go-builder-client/api/electra"
	builderAPIFulu "github.com/attestantio/go-builder-client/api/fulu"
	builderAPIV1 "github.com/attestantio/go-builder-client/api/v1"
	"github.com/attestantio/go-eth2-client/spec/bellatrix"
	"github.com/attestantio/go-eth2-client/spec/capella"
	"github.com/attestantio/go-eth2-client/spec/deneb"
	"github.com/attestantio/go-eth2-client/spec/electra"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/holiman/uint256"
)

const weiPerGwei = uint64(1_000_000_000)

const (
	executionPayloadBidSSZFixedSize       = 224
	signedExecutionPayloadBidSSZFixedSize = 100
	denebExecutionPayloadSSZFixedSize     = 528
	gloasExecutionPayloadSSZFixedSize     = 540
	gloasSubmitBlockRequestSSZFixedSize   = 344
	bidTraceSSZSize                       = 236
)

// DomainTypeBeaconBuilder is the Gloas consensus signing domain used for
// execution payload bids and envelopes.
var DomainTypeBeaconBuilder = phase0.DomainType{0x0b, 0x00, 0x00, 0x00}

// ExecutionPayloadBid is the Gloas consensus object returned by
// getExecutionPayloadBid. It is a ProgressiveContainer in the consensus spec;
// HashTreeRoot implements EIP-7495/EIP-7916 merkleization for signing.
type ExecutionPayloadBid struct {
	ParentBlockHash       phase0.Hash32              `json:"parent_block_hash"`
	ParentBlockRoot       phase0.Root                `json:"parent_block_root"`
	BlockHash             phase0.Hash32              `json:"block_hash"`
	PrevRandao            phase0.Hash32              `json:"prev_randao"`
	FeeRecipient          bellatrix.ExecutionAddress `json:"fee_recipient"`
	GasLimit              Uint64String               `json:"gas_limit"`
	BuilderIndex          Uint64String               `json:"builder_index"`
	Slot                  phase0.Slot                `json:"slot"`
	Value                 phase0.Gwei                `json:"value"`
	ExecutionPayment      phase0.Gwei                `json:"execution_payment"`
	BlobKZGCommitments    []deneb.KZGCommitment      `json:"blob_kzg_commitments"`
	ExecutionRequestsRoot phase0.Root                `json:"execution_requests_root"`
}

// SignedExecutionPayloadBid is a relay-signed Gloas bid.
type SignedExecutionPayloadBid struct {
	Message   *ExecutionPayloadBid `json:"message"`
	Signature phase0.BLSSignature  `json:"signature"`
}

// VersionedSignedExecutionPayloadBidResponse is the JSON response envelope
// expected by Lighthouse's Gloas builder client.
type VersionedSignedExecutionPayloadBidResponse struct {
	Version string                     `json:"version"`
	Data    *SignedExecutionPayloadBid `json:"data"`
}

// MarshalSSZ serializes the Gloas ExecutionPayloadBid. ProgressiveContainer
// changes merkleization, but its wire encoding is the regular SSZ container
// encoding. BlobKZGCommitments is the only variable-size field.
func (b *ExecutionPayloadBid) MarshalSSZ() ([]byte, error) {
	if b == nil {
		return nil, fmt.Errorf("nil execution payload bid")
	}

	encoded := make([]byte, executionPayloadBidSSZFixedSize+len(b.BlobKZGCommitments)*len(deneb.KZGCommitment{}))
	copy(encoded[0:32], b.ParentBlockHash[:])
	copy(encoded[32:64], b.ParentBlockRoot[:])
	copy(encoded[64:96], b.BlockHash[:])
	copy(encoded[96:128], b.PrevRandao[:])
	copy(encoded[128:148], b.FeeRecipient[:])
	binary.LittleEndian.PutUint64(encoded[148:156], uint64(b.GasLimit))
	binary.LittleEndian.PutUint64(encoded[156:164], uint64(b.BuilderIndex))
	binary.LittleEndian.PutUint64(encoded[164:172], uint64(b.Slot))
	binary.LittleEndian.PutUint64(encoded[172:180], uint64(b.Value))
	binary.LittleEndian.PutUint64(encoded[180:188], uint64(b.ExecutionPayment))
	binary.LittleEndian.PutUint32(encoded[188:192], executionPayloadBidSSZFixedSize)
	copy(encoded[192:224], b.ExecutionRequestsRoot[:])

	offset := executionPayloadBidSSZFixedSize
	for _, commitment := range b.BlobKZGCommitments {
		copy(encoded[offset:offset+len(commitment)], commitment[:])
		offset += len(commitment)
	}
	return encoded, nil
}

// UnmarshalSSZ decodes a Gloas ExecutionPayloadBid from its canonical SSZ
// representation.
func (b *ExecutionPayloadBid) UnmarshalSSZ(input []byte) error {
	if b == nil {
		return fmt.Errorf("nil execution payload bid")
	}
	if len(input) < executionPayloadBidSSZFixedSize {
		return fmt.Errorf("execution payload bid SSZ is too short: %d", len(input))
	}
	commitmentsOffset := int(binary.LittleEndian.Uint32(input[188:192]))
	if commitmentsOffset != executionPayloadBidSSZFixedSize {
		return fmt.Errorf("invalid blob commitments offset: %d", commitmentsOffset)
	}
	commitmentSize := len(deneb.KZGCommitment{})
	commitmentsBytes := input[commitmentsOffset:]
	if len(commitmentsBytes)%commitmentSize != 0 {
		return fmt.Errorf("invalid blob commitments SSZ length: %d", len(commitmentsBytes))
	}

	copy(b.ParentBlockHash[:], input[0:32])
	copy(b.ParentBlockRoot[:], input[32:64])
	copy(b.BlockHash[:], input[64:96])
	copy(b.PrevRandao[:], input[96:128])
	copy(b.FeeRecipient[:], input[128:148])
	b.GasLimit = Uint64String(binary.LittleEndian.Uint64(input[148:156]))
	b.BuilderIndex = Uint64String(binary.LittleEndian.Uint64(input[156:164]))
	b.Slot = phase0.Slot(binary.LittleEndian.Uint64(input[164:172]))
	b.Value = phase0.Gwei(binary.LittleEndian.Uint64(input[172:180]))
	b.ExecutionPayment = phase0.Gwei(binary.LittleEndian.Uint64(input[180:188]))
	copy(b.ExecutionRequestsRoot[:], input[192:224])

	b.BlobKZGCommitments = make([]deneb.KZGCommitment, len(commitmentsBytes)/commitmentSize)
	for i := range b.BlobKZGCommitments {
		start := i * commitmentSize
		copy(b.BlobKZGCommitments[i][:], commitmentsBytes[start:start+commitmentSize])
	}
	return nil
}

// MarshalSSZ serializes the raw SignedExecutionPayloadBid used by the
// application/octet-stream response. The JSON version/data wrapper is not
// included in the SSZ response body.
func (b *SignedExecutionPayloadBid) MarshalSSZ() ([]byte, error) {
	if b == nil || b.Message == nil {
		return nil, fmt.Errorf("incomplete signed execution payload bid")
	}
	message, err := b.Message.MarshalSSZ()
	if err != nil {
		return nil, err
	}
	encoded := make([]byte, signedExecutionPayloadBidSSZFixedSize+len(message))
	binary.LittleEndian.PutUint32(encoded[0:4], signedExecutionPayloadBidSSZFixedSize)
	copy(encoded[4:100], b.Signature[:])
	copy(encoded[100:], message)
	return encoded, nil
}

// UnmarshalSSZ decodes the raw SignedExecutionPayloadBid returned by the
// application/octet-stream Builder API response.
func (b *SignedExecutionPayloadBid) UnmarshalSSZ(input []byte) error {
	if b == nil {
		return fmt.Errorf("nil signed execution payload bid")
	}
	minimumSize := signedExecutionPayloadBidSSZFixedSize + executionPayloadBidSSZFixedSize
	if len(input) < minimumSize {
		return fmt.Errorf("signed execution payload bid SSZ is too short: %d", len(input))
	}
	messageOffset := int(binary.LittleEndian.Uint32(input[0:4]))
	if messageOffset != signedExecutionPayloadBidSSZFixedSize {
		return fmt.Errorf("invalid execution payload bid offset: %d", messageOffset)
	}
	copy(b.Signature[:], input[4:100])
	b.Message = new(ExecutionPayloadBid)
	if err := b.Message.UnmarshalSSZ(input[messageOffset:]); err != nil {
		return fmt.Errorf("decode execution payload bid: %w", err)
	}
	return nil
}

// HexBytes is a variable-length byte sequence with Ethereum's 0x-prefixed JSON
// representation.  Gloas carries the EIP-7928 block access list as RLP bytes.
type HexBytes []byte

func (v HexBytes) MarshalJSON() ([]byte, error) {
	return json.Marshal("0x" + hex.EncodeToString(v))
}

func (v *HexBytes) UnmarshalJSON(input []byte) error {
	var encoded string
	if err := json.Unmarshal(input, &encoded); err != nil {
		return err
	}
	if !strings.HasPrefix(encoded, "0x") {
		return fmt.Errorf("hex bytes must have a 0x prefix")
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(encoded, "0x"))
	if err != nil {
		return fmt.Errorf("invalid hex bytes: %w", err)
	}
	*v = decoded
	return nil
}

// ExecutionPayloadGloas is the consensus-layer Gloas execution payload.  The
// first 17 fields are the Deneb/Fulu payload; Amsterdam adds the raw EIP-7928
// block access list and the consensus slot number.
type ExecutionPayloadGloas struct {
	ParentHash      phase0.Hash32              `json:"parent_hash"`
	FeeRecipient    bellatrix.ExecutionAddress `json:"fee_recipient"`
	StateRoot       phase0.Root                `json:"state_root"`
	ReceiptsRoot    phase0.Root                `json:"receipts_root"`
	LogsBloom       [256]byte                  `json:"logs_bloom"`
	PrevRandao      [32]byte                   `json:"prev_randao"`
	BlockNumber     Uint64String               `json:"block_number"`
	GasLimit        Uint64String               `json:"gas_limit"`
	GasUsed         Uint64String               `json:"gas_used"`
	Timestamp       Uint64String               `json:"timestamp"`
	ExtraData       HexBytes                   `json:"extra_data"`
	BaseFeePerGas   *uint256.Int               `json:"base_fee_per_gas"`
	BlockHash       phase0.Hash32              `json:"block_hash"`
	Transactions    []bellatrix.Transaction    `json:"transactions"`
	Withdrawals     []*capella.Withdrawal      `json:"withdrawals"`
	BlobGasUsed     Uint64String               `json:"blob_gas_used"`
	ExcessBlobGas   Uint64String               `json:"excess_blob_gas"`
	BlockAccessList HexBytes                   `json:"block_access_list"`
	SlotNumber      phase0.Slot                `json:"slot_number"`
}

// ExecutionRequestsGloas is the five-list ExecutionRequests container used by
// Gloas.  BuilderDeposits and BuilderExits are introduced by EIP-8282; unlike
// the pre-Gloas Electra representation, they are part of the payload commitment
// and must survive builder submission, simulation, caching, and publication.
type ExecutionRequestsGloas struct {
	Deposits        []*electra.DepositRequest       `json:"deposits"`
	Withdrawals     []*electra.WithdrawalRequest    `json:"withdrawals"`
	Consolidations  []*electra.ConsolidationRequest `json:"consolidations"`
	BuilderDeposits []*BuilderDepositRequestGloas   `json:"builder_deposits"`
	BuilderExits    []*BuilderExitRequestGloas      `json:"builder_exits"`
}

type ExecutionPayloadEnvelope struct {
	Payload               *ExecutionPayloadGloas  `json:"payload"`
	ExecutionRequests     *ExecutionRequestsGloas `json:"execution_requests"`
	BuilderIndex          Uint64String            `json:"builder_index"`
	BeaconBlockRoot       phase0.Root             `json:"beacon_block_root"`
	ParentBeaconBlockRoot phase0.Root             `json:"parent_beacon_block_root"`
}

type SignedExecutionPayloadEnvelope struct {
	Message   *ExecutionPayloadEnvelope `json:"message"`
	Signature phase0.BLSSignature       `json:"signature"`
}

type SignedExecutionPayloadEnvelopeContents struct {
	SignedExecutionPayloadEnvelope *SignedExecutionPayloadEnvelope `json:"signed_execution_payload_envelope"`
	KZGProofs                      []deneb.KZGProof                `json:"kzg_proofs"`
	Blobs                          []deneb.Blob                    `json:"blobs"`
}

// GloasSubmitBlockRequest is the Amsterdam V6 builder submission.  It is kept
// locally until go-builder-client exposes this fork in its versioned union.
type GloasSubmitBlockRequest struct {
	Message           *builderAPIV1.BidTrace      `json:"message"`
	ExecutionPayload  *ExecutionPayloadGloas      `json:"execution_payload"`
	BlobsBundle       *builderAPIFulu.BlobsBundle `json:"blobs_bundle"`
	ExecutionRequests *ExecutionRequestsGloas     `json:"execution_requests"`
	Signature         phase0.BLSSignature         `json:"signature"`
}

// GloasPayloadContents is the complete Amsterdam payload needed to reveal a
// selected bid.  It deliberately keeps the Amsterdam-only block access list
// and slot number instead of reducing the payload to its Fulu compatibility
// representation.
type GloasPayloadContents struct {
	ExecutionPayload  *ExecutionPayloadGloas      `json:"execution_payload"`
	BlobsBundle       *builderAPIFulu.BlobsBundle `json:"blobs_bundle"`
	ExecutionRequests *ExecutionRequestsGloas     `json:"execution_requests"`
}

// GloasPayloadCacheEntry contains the reveal data and validated bid metadata
// shared by relay API instances through Redis.
type GloasPayloadCacheEntry struct {
	Slot         uint64                     `json:"slot,string"`
	BlockHash    phase0.Hash32              `json:"block_hash"`
	FeeRecipient bellatrix.ExecutionAddress `json:"fee_recipient"`
	BidTrace     *BidTraceV2WithBlobFields  `json:"bid_trace"`
	Contents     *GloasPayloadContents      `json:"contents"`
}

// GloasSelectedPayloadCacheEntry records the exact relay bid accepted by the
// proposer and the block hash needed to resolve its full reveal payload.
type GloasSelectedPayloadCacheEntry struct {
	BlockHash phase0.Hash32              `json:"block_hash"`
	Bid       *SignedExecutionPayloadBid `json:"bid"`
}

// IsGloasSubmitBlockRequestSSZ identifies the canonical V6 SSZ shape without
// decoding the full request.  V5 and V6 have the same outer fixed section, but
// their nested execution payload fixed sections are 528 and 540 bytes,
// respectively.  This is needed for builders such as rbuilder that send SSZ
// without an Eth-Consensus-Version header.
func IsGloasSubmitBlockRequestSSZ(input []byte) bool {
	if len(input) < gloasSubmitBlockRequestSSZFixedSize+gloasExecutionPayloadSSZFixedSize {
		return false
	}
	payloadOffset := int(binary.LittleEndian.Uint32(input[236:240]))
	blobsBundleOffset := int(binary.LittleEndian.Uint32(input[240:244]))
	if payloadOffset != gloasSubmitBlockRequestSSZFixedSize ||
		blobsBundleOffset < payloadOffset+gloasExecutionPayloadSSZFixedSize ||
		blobsBundleOffset > len(input) {
		return false
	}
	extraDataOffsetPosition := payloadOffset + 436
	extraDataOffset := int(binary.LittleEndian.Uint32(input[extraDataOffsetPosition : extraDataOffsetPosition+4]))
	return extraDataOffset == gloasExecutionPayloadSSZFixedSize
}

// MarshalSSZ serializes the Amsterdam V6 execution payload emitted by
// rbuilder.  It has the Deneb payload fields followed by a variable
// block_access_list and a fixed slot_number.
func (p *ExecutionPayloadGloas) MarshalSSZ() ([]byte, error) {
	base, err := p.AsDeneb()
	if err != nil {
		return nil, err
	}
	for _, withdrawal := range base.Withdrawals {
		if withdrawal == nil {
			return nil, fmt.Errorf("nil withdrawal in Gloas execution payload")
		}
	}

	denebEncoded, err := base.MarshalSSZ()
	if err != nil {
		return nil, fmt.Errorf("encode base execution payload: %w", err)
	}
	if len(denebEncoded) < denebExecutionPayloadSSZFixedSize {
		return nil, fmt.Errorf("invalid encoded base execution payload length: %d", len(denebEncoded))
	}
	encodedLength := uint64(len(denebEncoded)) +
		(gloasExecutionPayloadSSZFixedSize - denebExecutionPayloadSSZFixedSize) +
		uint64(len(p.BlockAccessList))
	if encodedLength > uint64(^uint32(0)) {
		return nil, fmt.Errorf("Gloas execution payload exceeds SSZ offset range")
	}

	encoded := make([]byte, gloasExecutionPayloadSSZFixedSize, int(encodedLength))
	copy(encoded[:denebExecutionPayloadSSZFixedSize], denebEncoded[:denebExecutionPayloadSSZFixedSize])
	for _, offsetPosition := range [...]int{436, 504, 508} {
		offset := binary.LittleEndian.Uint32(denebEncoded[offsetPosition : offsetPosition+4])
		binary.LittleEndian.PutUint32(encoded[offsetPosition:offsetPosition+4], offset+12)
	}
	binary.LittleEndian.PutUint32(encoded[528:532], uint32(len(denebEncoded)+12))
	binary.LittleEndian.PutUint64(encoded[532:540], uint64(p.SlotNumber))
	encoded = append(encoded, denebEncoded[denebExecutionPayloadSSZFixedSize:]...)
	encoded = append(encoded, p.BlockAccessList...)
	return encoded, nil
}

// UnmarshalSSZ decodes rbuilder's Amsterdam ExecutionPayloadV4 wire shape.
// The first 17 fields are decoded through the maintained Deneb decoder after
// removing Amsterdam's 12-byte fixed section extension.
func (p *ExecutionPayloadGloas) UnmarshalSSZ(input []byte) error {
	if p == nil {
		return fmt.Errorf("nil Gloas execution payload")
	}
	if len(input) < gloasExecutionPayloadSSZFixedSize {
		return fmt.Errorf("Gloas execution payload SSZ is too short: %d", len(input))
	}

	extraDataOffset := int(binary.LittleEndian.Uint32(input[436:440]))
	transactionsOffset := int(binary.LittleEndian.Uint32(input[504:508]))
	withdrawalsOffset := int(binary.LittleEndian.Uint32(input[508:512]))
	blockAccessListOffset := int(binary.LittleEndian.Uint32(input[528:532]))
	if extraDataOffset != gloasExecutionPayloadSSZFixedSize {
		return fmt.Errorf("invalid Gloas extra_data offset: %d", extraDataOffset)
	}
	if transactionsOffset < extraDataOffset ||
		withdrawalsOffset < transactionsOffset ||
		blockAccessListOffset < withdrawalsOffset ||
		blockAccessListOffset > len(input) {
		return fmt.Errorf("invalid Gloas execution payload SSZ offsets")
	}

	// Rebuild the common Deneb/Fulu payload wire representation.  All three
	// existing variable offsets move back by the 12 bytes added in Amsterdam.
	denebEncoded := make([]byte, denebExecutionPayloadSSZFixedSize, blockAccessListOffset-12)
	copy(denebEncoded, input[:denebExecutionPayloadSSZFixedSize])
	for _, offsetPosition := range [...]int{436, 504, 508} {
		offset := binary.LittleEndian.Uint32(input[offsetPosition : offsetPosition+4])
		binary.LittleEndian.PutUint32(denebEncoded[offsetPosition:offsetPosition+4], offset-12)
	}
	denebEncoded = append(denebEncoded, input[gloasExecutionPayloadSSZFixedSize:blockAccessListOffset]...)

	base := new(deneb.ExecutionPayload)
	if err := base.UnmarshalSSZ(denebEncoded); err != nil {
		return fmt.Errorf("decode base execution payload: %w", err)
	}
	converted, err := NewExecutionPayloadGloas(
		base,
		input[blockAccessListOffset:],
		binary.LittleEndian.Uint64(input[532:540]),
	)
	if err != nil {
		return err
	}
	*p = *converted
	return nil
}

// MarshalSSZ serializes the canonical five-field SignedBidSubmissionV6 used
// by rbuilder.  Bid adjustment data is intentionally not part of this Gloas
// container.
func (s *GloasSubmitBlockRequest) MarshalSSZ() ([]byte, error) {
	if s == nil || s.Message == nil || s.ExecutionPayload == nil || s.BlobsBundle == nil || s.ExecutionRequests == nil {
		return nil, fmt.Errorf("incomplete Gloas submit-block request")
	}
	message, err := s.Message.MarshalSSZ()
	if err != nil {
		return nil, fmt.Errorf("encode bid trace: %w", err)
	}
	payload, err := s.ExecutionPayload.MarshalSSZ()
	if err != nil {
		return nil, err
	}
	blobsBundle, err := s.BlobsBundle.MarshalSSZ()
	if err != nil {
		return nil, fmt.Errorf("encode blobs bundle: %w", err)
	}
	executionRequests, err := s.ExecutionRequests.MarshalSSZ()
	if err != nil {
		return nil, fmt.Errorf("encode execution requests: %w", err)
	}
	totalLength := uint64(gloasSubmitBlockRequestSSZFixedSize) + uint64(len(payload)) + uint64(len(blobsBundle)) + uint64(len(executionRequests))
	if totalLength > uint64(^uint32(0)) {
		return nil, fmt.Errorf("Gloas submit-block request exceeds SSZ offset range")
	}

	encoded := make([]byte, gloasSubmitBlockRequestSSZFixedSize, int(totalLength))
	copy(encoded[:bidTraceSSZSize], message)
	payloadOffset := gloasSubmitBlockRequestSSZFixedSize
	blobsBundleOffset := payloadOffset + len(payload)
	executionRequestsOffset := blobsBundleOffset + len(blobsBundle)
	binary.LittleEndian.PutUint32(encoded[236:240], uint32(payloadOffset))
	binary.LittleEndian.PutUint32(encoded[240:244], uint32(blobsBundleOffset))
	binary.LittleEndian.PutUint32(encoded[244:248], uint32(executionRequestsOffset))
	copy(encoded[248:344], s.Signature[:])
	encoded = append(encoded, payload...)
	encoded = append(encoded, blobsBundle...)
	encoded = append(encoded, executionRequests...)
	return encoded, nil
}

// UnmarshalSSZ decodes the canonical five-field SignedBidSubmissionV6 emitted
// by rbuilder when use_ssz_for_submit is enabled.
func (s *GloasSubmitBlockRequest) UnmarshalSSZ(input []byte) error {
	if s == nil {
		return fmt.Errorf("nil Gloas submit-block request")
	}
	if len(input) < gloasSubmitBlockRequestSSZFixedSize {
		return fmt.Errorf("Gloas submit-block request SSZ is too short: %d", len(input))
	}
	payloadOffset := int(binary.LittleEndian.Uint32(input[236:240]))
	blobsBundleOffset := int(binary.LittleEndian.Uint32(input[240:244]))
	executionRequestsOffset := int(binary.LittleEndian.Uint32(input[244:248]))
	if payloadOffset != gloasSubmitBlockRequestSSZFixedSize {
		return fmt.Errorf("invalid Gloas execution payload offset: %d", payloadOffset)
	}
	if blobsBundleOffset < payloadOffset ||
		executionRequestsOffset < blobsBundleOffset ||
		executionRequestsOffset > len(input) {
		return fmt.Errorf("invalid Gloas submit-block request SSZ offsets")
	}

	s.Message = new(builderAPIV1.BidTrace)
	if err := s.Message.UnmarshalSSZ(input[:bidTraceSSZSize]); err != nil {
		return fmt.Errorf("decode bid trace: %w", err)
	}
	copy(s.Signature[:], input[248:344])
	s.ExecutionPayload = new(ExecutionPayloadGloas)
	if err := s.ExecutionPayload.UnmarshalSSZ(input[payloadOffset:blobsBundleOffset]); err != nil {
		return fmt.Errorf("decode Gloas execution payload: %w", err)
	}
	s.BlobsBundle = new(builderAPIFulu.BlobsBundle)
	if err := s.BlobsBundle.UnmarshalSSZ(input[blobsBundleOffset:executionRequestsOffset]); err != nil {
		return fmt.Errorf("decode blobs bundle: %w", err)
	}
	s.ExecutionRequests = new(ExecutionRequestsGloas)
	if err := s.ExecutionRequests.UnmarshalSSZ(input[executionRequestsOffset:]); err != nil {
		return fmt.Errorf("decode execution requests: %w", err)
	}
	return nil
}

func (p *ExecutionPayloadGloas) MarshalJSON() ([]byte, error) {
	base, err := p.AsDeneb()
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(base)
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		return nil, err
	}
	fields["block_access_list"], err = json.Marshal(p.BlockAccessList)
	if err != nil {
		return nil, err
	}
	fields["slot_number"], err = json.Marshal(p.SlotNumber)
	if err != nil {
		return nil, err
	}
	return json.Marshal(fields)
}

func (p *ExecutionPayloadGloas) UnmarshalJSON(input []byte) error {
	base := new(deneb.ExecutionPayload)
	if err := json.Unmarshal(input, base); err != nil {
		return err
	}
	var extra struct {
		BlockAccessList HexBytes    `json:"block_access_list"`
		SlotNumber      phase0.Slot `json:"slot_number"`
	}
	if err := json.Unmarshal(input, &extra); err != nil {
		return err
	}
	if extra.BlockAccessList == nil {
		return fmt.Errorf("block_access_list missing")
	}
	converted, err := NewExecutionPayloadGloas(base, extra.BlockAccessList, uint64(extra.SlotNumber))
	if err != nil {
		return err
	}
	*p = *converted
	return nil
}

func (p *ExecutionPayloadGloas) AsDeneb() (*deneb.ExecutionPayload, error) {
	if p == nil || p.BaseFeePerGas == nil {
		return nil, fmt.Errorf("incomplete Gloas execution payload")
	}
	return &deneb.ExecutionPayload{
		ParentHash:    p.ParentHash,
		FeeRecipient:  p.FeeRecipient,
		StateRoot:     p.StateRoot,
		ReceiptsRoot:  p.ReceiptsRoot,
		LogsBloom:     p.LogsBloom,
		PrevRandao:    p.PrevRandao,
		BlockNumber:   uint64(p.BlockNumber),
		GasLimit:      uint64(p.GasLimit),
		GasUsed:       uint64(p.GasUsed),
		Timestamp:     uint64(p.Timestamp),
		ExtraData:     append([]byte(nil), p.ExtraData...),
		BaseFeePerGas: new(uint256.Int).Set(p.BaseFeePerGas),
		BlockHash:     p.BlockHash,
		Transactions:  append([]bellatrix.Transaction(nil), p.Transactions...),
		Withdrawals:   cloneWithdrawals(p.Withdrawals),
		BlobGasUsed:   uint64(p.BlobGasUsed),
		ExcessBlobGas: uint64(p.ExcessBlobGas),
	}, nil
}

// cloneWithdrawals preserves the distinction between an empty list (`[]`) and
// a missing list (`nil`).  Post-Capella payload validation rejects a missing
// withdrawals list, while an empty list is valid and common on local devnets.
func cloneWithdrawals(withdrawals []*capella.Withdrawal) []*capella.Withdrawal {
	if withdrawals == nil {
		return nil
	}
	cloned := make([]*capella.Withdrawal, len(withdrawals))
	copy(cloned, withdrawals)
	return cloned
}

func (s *GloasSubmitBlockRequest) AsFulu() (*builderAPIFulu.SubmitBlockRequest, error) {
	if s == nil || s.Message == nil || s.ExecutionPayload == nil || s.BlobsBundle == nil || s.ExecutionRequests == nil {
		return nil, fmt.Errorf("incomplete Gloas submit-block request")
	}
	payload, err := s.ExecutionPayload.AsDeneb()
	if err != nil {
		return nil, err
	}
	executionRequests, err := s.ExecutionRequests.AsElectra()
	if err != nil {
		return nil, err
	}
	return &builderAPIFulu.SubmitBlockRequest{
		Message:           s.Message,
		ExecutionPayload:  payload,
		BlobsBundle:       s.BlobsBundle,
		ExecutionRequests: executionRequests,
		Signature:         s.Signature,
	}, nil
}

func NewExecutionPayloadGloas(payload *deneb.ExecutionPayload, blockAccessList []byte, slot uint64) (*ExecutionPayloadGloas, error) {
	if payload == nil || payload.BaseFeePerGas == nil {
		return nil, fmt.Errorf("incomplete Gloas execution payload")
	}
	return &ExecutionPayloadGloas{
		ParentHash:      payload.ParentHash,
		FeeRecipient:    payload.FeeRecipient,
		StateRoot:       payload.StateRoot,
		ReceiptsRoot:    payload.ReceiptsRoot,
		LogsBloom:       payload.LogsBloom,
		PrevRandao:      payload.PrevRandao,
		BlockNumber:     Uint64String(payload.BlockNumber),
		GasLimit:        Uint64String(payload.GasLimit),
		GasUsed:         Uint64String(payload.GasUsed),
		Timestamp:       Uint64String(payload.Timestamp),
		ExtraData:       append(HexBytes(nil), payload.ExtraData...),
		BaseFeePerGas:   new(uint256.Int).Set(payload.BaseFeePerGas),
		BlockHash:       payload.BlockHash,
		Transactions:    append([]bellatrix.Transaction(nil), payload.Transactions...),
		Withdrawals:     cloneWithdrawals(payload.Withdrawals),
		BlobGasUsed:     Uint64String(payload.BlobGasUsed),
		ExcessBlobGas:   Uint64String(payload.ExcessBlobGas),
		BlockAccessList: append(HexBytes(nil), blockAccessList...),
		SlotNumber:      phase0.Slot(slot),
	}, nil
}

func NewExecutionRequestsGloas(requests *electra.ExecutionRequests) *ExecutionRequestsGloas {
	result := &ExecutionRequestsGloas{
		Deposits:        []*electra.DepositRequest{},
		Withdrawals:     []*electra.WithdrawalRequest{},
		Consolidations:  []*electra.ConsolidationRequest{},
		BuilderDeposits: []*BuilderDepositRequestGloas{},
		BuilderExits:    []*BuilderExitRequestGloas{},
	}
	if requests != nil {
		result.Deposits = append(result.Deposits, requests.Deposits...)
		result.Withdrawals = append(result.Withdrawals, requests.Withdrawals...)
		result.Consolidations = append(result.Consolidations, requests.Consolidations...)
	}
	return result
}

// AsElectra returns the three pre-Gloas request lists for relay code which has
// not yet grown a Gloas versioned union.  The Gloas-only lists are deliberately
// not represented here; callers must retain the original ExecutionRequestsGloas
// alongside this compatibility view.
func (r *ExecutionRequestsGloas) AsElectra() (*electra.ExecutionRequests, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	deposits := make([]*electra.DepositRequest, len(r.Deposits))
	copy(deposits, r.Deposits)
	withdrawals := make([]*electra.WithdrawalRequest, len(r.Withdrawals))
	copy(withdrawals, r.Withdrawals)
	consolidations := make([]*electra.ConsolidationRequest, len(r.Consolidations))
	copy(consolidations, r.Consolidations)
	return &electra.ExecutionRequests{
		Deposits:       deposits,
		Withdrawals:    withdrawals,
		Consolidations: consolidations,
	}, nil
}

func (e *ExecutionPayloadEnvelope) HashTreeRoot() ([32]byte, error) {
	if e == nil || e.Payload == nil || e.ExecutionRequests == nil {
		return [32]byte{}, fmt.Errorf("incomplete execution payload envelope")
	}
	payloadRoot, err := e.Payload.HashTreeRoot()
	if err != nil {
		return [32]byte{}, err
	}
	requestsRoot, err := e.ExecutionRequests.HashTreeRoot()
	if err != nil {
		return [32]byte{}, err
	}
	fields := [][32]byte{
		payloadRoot,
		requestsRoot,
		uint64Root(uint64(e.BuilderIndex)),
		e.BeaconBlockRoot,
		e.ParentBeaconBlockRoot,
	}
	return mixInActiveFields(merkleizeProgressive(fields, 1), []byte{0x1f}), nil
}

func (p *ExecutionPayloadGloas) HashTreeRoot() ([32]byte, error) {
	if p == nil || p.BaseFeePerGas == nil {
		return [32]byte{}, fmt.Errorf("incomplete Gloas execution payload")
	}
	if len(p.ExtraData) > 32 {
		return [32]byte{}, fmt.Errorf("extra_data exceeds 32 bytes")
	}

	txRoots := make([][32]byte, len(p.Transactions))
	for i, tx := range p.Transactions {
		txRoots[i] = progressiveByteListRoot(tx)
	}
	withdrawalRoots := make([][32]byte, len(p.Withdrawals))
	for i, withdrawal := range p.Withdrawals {
		if withdrawal == nil {
			return [32]byte{}, fmt.Errorf("nil withdrawal")
		}
		withdrawalRoots[i] = withdrawalRoot(withdrawal)
	}

	baseFee := p.BaseFeePerGas.Bytes32()
	reverseBytes(baseFee[:])
	fields := [][32]byte{
		p.ParentHash,
		bytesRoot(p.FeeRecipient[:]),
		p.StateRoot,
		p.ReceiptsRoot,
		fixedBytesRoot(p.LogsBloom[:]),
		p.PrevRandao,
		uint64Root(uint64(p.BlockNumber)),
		uint64Root(uint64(p.GasLimit)),
		uint64Root(uint64(p.GasUsed)),
		uint64Root(uint64(p.Timestamp)),
		boundedByteListRoot(p.ExtraData, 32),
		baseFee,
		p.BlockHash,
		progressiveCompositeListRoot(txRoots),
		progressiveCompositeListRoot(withdrawalRoots),
		uint64Root(uint64(p.BlobGasUsed)),
		uint64Root(uint64(p.ExcessBlobGas)),
		progressiveByteListRoot(p.BlockAccessList),
		uint64Root(uint64(p.SlotNumber)),
	}
	return mixInActiveFields(merkleizeProgressive(fields, 1), []byte{0xff, 0xff, 0x07}), nil
}

func (r *ExecutionRequestsGloas) HashTreeRoot() ([32]byte, error) {
	if err := r.validate(); err != nil {
		return [32]byte{}, err
	}
	requestRoots := func(values any) ([][32]byte, error) {
		var roots [][32]byte
		switch requests := values.(type) {
		case []*electra.DepositRequest:
			for _, request := range requests {
				if request == nil {
					return nil, fmt.Errorf("nil deposit request")
				}
				root, err := request.HashTreeRoot()
				if err != nil {
					return nil, err
				}
				roots = append(roots, root)
			}
		case []*electra.WithdrawalRequest:
			for _, request := range requests {
				if request == nil {
					return nil, fmt.Errorf("nil withdrawal request")
				}
				root, err := request.HashTreeRoot()
				if err != nil {
					return nil, err
				}
				roots = append(roots, root)
			}
		case []*electra.ConsolidationRequest:
			for _, request := range requests {
				if request == nil {
					return nil, fmt.Errorf("nil consolidation request")
				}
				root, err := request.HashTreeRoot()
				if err != nil {
					return nil, err
				}
				roots = append(roots, root)
			}
		case []*BuilderDepositRequestGloas:
			for _, request := range requests {
				if request == nil {
					return nil, fmt.Errorf("nil builder deposit request")
				}
				root, err := request.HashTreeRoot()
				if err != nil {
					return nil, err
				}
				roots = append(roots, root)
			}
		case []*BuilderExitRequestGloas:
			for _, request := range requests {
				if request == nil {
					return nil, fmt.Errorf("nil builder exit request")
				}
				root, err := request.HashTreeRoot()
				if err != nil {
					return nil, err
				}
				roots = append(roots, root)
			}
		}
		return roots, nil
	}
	deposits, err := requestRoots(r.Deposits)
	if err != nil {
		return [32]byte{}, err
	}
	withdrawals, err := requestRoots(r.Withdrawals)
	if err != nil {
		return [32]byte{}, err
	}
	consolidations, err := requestRoots(r.Consolidations)
	if err != nil {
		return [32]byte{}, err
	}
	builderDeposits, err := requestRoots(r.BuilderDeposits)
	if err != nil {
		return [32]byte{}, err
	}
	builderExits, err := requestRoots(r.BuilderExits)
	if err != nil {
		return [32]byte{}, err
	}
	fields := [][32]byte{
		progressiveCompositeListRoot(deposits),
		progressiveCompositeListRoot(withdrawals),
		progressiveCompositeListRoot(consolidations),
		progressiveCompositeListRoot(builderDeposits),
		progressiveCompositeListRoot(builderExits),
	}
	return mixInActiveFields(merkleizeProgressive(fields, 1), []byte{0x1f}), nil
}

// HashTreeRoot returns the regular SSZ container root used for an EIP-8282
// BuilderDepositRequest.  The containing request list and ExecutionRequests
// container use the Gloas progressive Merkleization scheme.
func (r *BuilderDepositRequestGloas) HashTreeRoot() ([32]byte, error) {
	if r == nil {
		return [32]byte{}, fmt.Errorf("nil builder deposit request")
	}
	return merkleizeFixed([][32]byte{
		fixedBytesRoot(r.Pubkey[:]),
		r.WithdrawalCredentials,
		uint64Root(uint64(r.Amount)),
		fixedBytesRoot(r.Signature[:]),
	}, 4), nil
}

// HashTreeRoot returns the regular SSZ container root used for an EIP-8282
// BuilderExitRequest.
func (r *BuilderExitRequestGloas) HashTreeRoot() ([32]byte, error) {
	if r == nil {
		return [32]byte{}, fmt.Errorf("nil builder exit request")
	}
	return merkleizeFixed([][32]byte{
		bytesRoot(r.SourceAddress[:]),
		fixedBytesRoot(r.Pubkey[:]),
	}, 2), nil
}

// Uint64String is an SSZ uint64 with the canonical quoted-decimal JSON form.
type Uint64String uint64

func (v Uint64String) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%d"`, uint64(v))), nil
}

func (v *Uint64String) UnmarshalJSON(input []byte) error {
	var encoded string
	if err := json.Unmarshal(input, &encoded); err != nil {
		return fmt.Errorf("uint64 must be a quoted decimal string: %w", err)
	}
	parsed, err := strconv.ParseUint(encoded, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid uint64 string %q: %w", encoded, err)
	}
	*v = Uint64String(parsed)
	return nil
}

// NewExecutionPayloadBid maps the winning legacy Fulu/Electra BuilderBid into
// the consensus-layer Gloas bid shape. The relay is the protocol-visible
// builder and pays the proposer through the execution payload, so the legacy
// bid value is represented entirely as a trusted execution-layer payment.
// Legacy builder values are wei; Gloas value fields are Gwei, so the
// conversion deliberately rounds down.
func NewExecutionPayloadBid(
	legacy *builderAPIElectra.BuilderBid,
	executionRequests *ExecutionRequestsGloas,
	slot uint64,
	parentBlockRoot phase0.Root,
	proposerFeeRecipient bellatrix.ExecutionAddress,
	builderIndex uint64,
) (*ExecutionPayloadBid, error) {
	if legacy == nil || legacy.Header == nil || legacy.Value == nil {
		return nil, fmt.Errorf("incomplete winning builder bid")
	}

	value, err := weiToGwei(legacy.Value.ToBig())
	if err != nil {
		return nil, err
	}
	if executionRequests == nil {
		return nil, fmt.Errorf("missing Gloas execution requests")
	}
	executionRequestsRoot, err := executionRequests.HashTreeRoot()
	if err != nil {
		return nil, err
	}
	commitments := make([]deneb.KZGCommitment, len(legacy.BlobKZGCommitments))
	copy(commitments, legacy.BlobKZGCommitments)

	return &ExecutionPayloadBid{
		ParentBlockHash: legacy.Header.ParentHash,
		ParentBlockRoot: parentBlockRoot,
		BlockHash:       legacy.Header.BlockHash,
		PrevRandao:      phase0.Hash32(legacy.Header.PrevRandao),
		// Unlike the beneficiary in the legacy execution payload header, the
		// Gloas bid fee recipient is the proposer's configured fee recipient.
		FeeRecipient:          proposerFeeRecipient,
		GasLimit:              Uint64String(legacy.Header.GasLimit),
		BuilderIndex:          Uint64String(builderIndex),
		Slot:                  phase0.Slot(slot),
		Value:                 0,
		ExecutionPayment:      phase0.Gwei(value),
		BlobKZGCommitments:    commitments,
		ExecutionRequestsRoot: executionRequestsRoot,
	}, nil
}

func weiToGwei(value *big.Int) (uint64, error) {
	if value == nil || value.Sign() < 0 {
		return 0, fmt.Errorf("invalid negative or nil bid value")
	}
	gwei := new(big.Int).Div(new(big.Int).Set(value), new(big.Int).SetUint64(weiPerGwei))
	if !gwei.IsUint64() {
		return 0, fmt.Errorf("bid value exceeds uint64 Gwei")
	}
	return gwei.Uint64(), nil
}

// HashTreeRoot returns the EIP-7495 progressive-container root for all 12
// currently active ExecutionPayloadBid fields.
func (b *ExecutionPayloadBid) HashTreeRoot() ([32]byte, error) {
	if b == nil {
		return [32]byte{}, fmt.Errorf("nil execution payload bid")
	}

	commitmentsRoot := progressiveCompositeListRoot(kzgCommitmentRoots(b.BlobKZGCommitments))
	fieldRoots := [][32]byte{
		b.ParentBlockHash,
		b.ParentBlockRoot,
		b.BlockHash,
		b.PrevRandao,
		bytesRoot(b.FeeRecipient[:]),
		uint64Root(uint64(b.GasLimit)),
		uint64Root(uint64(b.BuilderIndex)),
		uint64Root(uint64(b.Slot)),
		uint64Root(uint64(b.Value)),
		uint64Root(uint64(b.ExecutionPayment)),
		commitmentsRoot,
		b.ExecutionRequestsRoot,
	}

	// ACTIVE_FIELDS = active_fields(width=12): the first 12 bits are set.
	return mixInActiveFields(merkleizeProgressive(fieldRoots, 1), []byte{0xff, 0x0f}), nil
}

// GloasExecutionRequestsRoot converts the three legacy Electra request lists
// to the five-field Gloas progressive container, with the Gloas-only lists
// empty. It remains useful for compatibility callers; native Gloas bids must
// hash the complete ExecutionRequestsGloas received from the builder.
func GloasExecutionRequestsRoot(requests *electra.ExecutionRequests) ([32]byte, error) {
	return NewExecutionRequestsGloas(requests).HashTreeRoot()
}

func kzgCommitmentRoots(commitments []deneb.KZGCommitment) [][32]byte {
	roots := make([][32]byte, 0, len(commitments))
	for _, commitment := range commitments {
		chunks := make([][32]byte, 2)
		copy(chunks[0][:], commitment[:32])
		copy(chunks[1][:], commitment[32:])
		roots = append(roots, hashPair(chunks[0], chunks[1]))
	}
	return roots
}

func progressiveCompositeListRoot(roots [][32]byte) [32]byte {
	return hashPair(merkleizeProgressive(roots, 1), uint64Root(uint64(len(roots))))
}

func merkleizeProgressive(chunks [][32]byte, subtreeLeaves uint64) [32]byte {
	if len(chunks) == 0 {
		return [32]byte{}
	}
	if subtreeLeaves == 0 || subtreeLeaves > uint64(math.MaxInt) {
		panic("invalid progressive subtree size")
	}

	take := len(chunks)
	if uint64(take) > subtreeLeaves {
		take = int(subtreeLeaves)
	}
	left := merkleizeFixed(chunks[:take], int(subtreeLeaves))
	right := merkleizeProgressive(chunks[take:], subtreeLeaves*4)
	return hashPair(left, right)
}

func merkleizeFixed(chunks [][32]byte, limit int) [32]byte {
	if limit < 1 || limit&(limit-1) != 0 || len(chunks) > limit {
		panic("invalid SSZ merkleization limit")
	}
	nodes := make([][32]byte, limit)
	copy(nodes, chunks)
	for width := limit; width > 1; width /= 2 {
		for i := 0; i < width; i += 2 {
			nodes[i/2] = hashPair(nodes[i], nodes[i+1])
		}
	}
	return nodes[0]
}

func mixInActiveFields(root [32]byte, active []byte) [32]byte {
	var packed [32]byte
	copy(packed[:], active)
	return hashPair(root, packed)
}

func bytesRoot(value []byte) [32]byte {
	var root [32]byte
	copy(root[:], value)
	return root
}

func fixedBytesRoot(value []byte) [32]byte {
	return merkleizeFixed(packBytes(value), nextPowerOfTwo(max(1, (len(value)+31)/32)))
}

func boundedByteListRoot(value []byte, limit int) [32]byte {
	chunkLimit := max(1, (limit+31)/32)
	return hashPair(merkleizeFixed(packBytes(value), nextPowerOfTwo(chunkLimit)), uint64Root(uint64(len(value))))
}

func progressiveByteListRoot(value []byte) [32]byte {
	return hashPair(merkleizeProgressive(packBytes(value), 1), uint64Root(uint64(len(value))))
}

func packBytes(value []byte) [][32]byte {
	chunks := make([][32]byte, (len(value)+31)/32)
	for i := range chunks {
		copy(chunks[i][:], value[i*32:min(len(value), (i+1)*32)])
	}
	return chunks
}

func withdrawalRoot(withdrawal *capella.Withdrawal) [32]byte {
	return merkleizeFixed([][32]byte{
		uint64Root(uint64(withdrawal.Index)),
		uint64Root(uint64(withdrawal.ValidatorIndex)),
		bytesRoot(withdrawal.Address[:]),
		uint64Root(uint64(withdrawal.Amount)),
	}, 4)
}

func reverseBytes(value []byte) {
	for left, right := 0, len(value)-1; left < right; left, right = left+1, right-1 {
		value[left], value[right] = value[right], value[left]
	}
}

func nextPowerOfTwo(value int) int {
	result := 1
	for result < value {
		result *= 2
	}
	return result
}

func uint64Root(value uint64) [32]byte {
	var root [32]byte
	binary.LittleEndian.PutUint64(root[:8], value)
	return root
}

func hashPair(left, right [32]byte) [32]byte {
	var input [64]byte
	copy(input[:32], left[:])
	copy(input[32:], right[:])
	return sha256.Sum256(input[:])
}
