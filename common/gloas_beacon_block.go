package common

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"reflect"

	"github.com/attestantio/go-eth2-client/spec/altair"
	"github.com/attestantio/go-eth2-client/spec/bellatrix"
	"github.com/attestantio/go-eth2-client/spec/capella"
	"github.com/attestantio/go-eth2-client/spec/electra"
	"github.com/attestantio/go-eth2-client/spec/phase0"
)

const (
	gloasSignedBeaconBlockFixedSize = 100
	gloasBeaconBlockFixedSize       = 84
	gloasBeaconBlockBodyFixedSize   = 396
	gloasPayloadAttestationSize     = 202
	gloasBuilderDepositRequestSize  = 184
	gloasBuilderExitRequestSize     = 68
	gloasMaxWithdrawalRequests      = 16
	gloasMaxConsolidationRequests   = 2
	gloasMaxBuilderDepositRequests  = 64
	gloasMaxBuilderExitRequests     = 16
)

// SignedBeaconBlockGloas is the complete proposer-signed beacon block sent to
// POST /eth/v1/builder/beacon_blocks.  Keep this local until go-eth2-client
// exposes Gloas consensus types.
type SignedBeaconBlockGloas struct {
	Message   *BeaconBlockGloas   `json:"message"`
	Signature phase0.BLSSignature `json:"signature"`
}

type BeaconBlockGloas struct {
	Slot          phase0.Slot           `json:"slot"`
	ProposerIndex phase0.ValidatorIndex `json:"proposer_index"`
	ParentRoot    phase0.Root           `json:"parent_root"`
	StateRoot     phase0.Root           `json:"state_root"`
	Body          *BeaconBlockBodyGloas `json:"body"`
}

type BeaconBlockBodyGloas struct {
	RANDAOReveal              phase0.BLSSignature                   `json:"randao_reveal"`
	ETH1Data                  *phase0.ETH1Data                      `json:"eth1_data"`
	Graffiti                  phase0.Root                           `json:"graffiti"`
	ProposerSlashings         []*phase0.ProposerSlashing            `json:"proposer_slashings"`
	AttesterSlashings         []*electra.AttesterSlashing           `json:"attester_slashings"`
	Attestations              []*electra.Attestation                `json:"attestations"`
	Deposits                  []*phase0.Deposit                     `json:"deposits"`
	VoluntaryExits            []*phase0.SignedVoluntaryExit         `json:"voluntary_exits"`
	SyncAggregate             *altair.SyncAggregate                 `json:"sync_aggregate"`
	BLSToExecutionChanges     []*capella.SignedBLSToExecutionChange `json:"bls_to_execution_changes"`
	SignedExecutionPayloadBid *SignedExecutionPayloadBid            `json:"signed_execution_payload_bid"`
	PayloadAttestations       []*PayloadAttestationGloas            `json:"payload_attestations"`
	ParentExecutionRequests   *ExecutionRequestsGloas               `json:"parent_execution_requests"`
}

type PayloadAttestationDataGloas struct {
	BeaconBlockRoot   phase0.Root `json:"beacon_block_root"`
	Slot              phase0.Slot `json:"slot"`
	PayloadPresent    bool        `json:"payload_present"`
	BlobDataAvailable bool        `json:"blob_data_available"`
}

type PayloadAttestationGloas struct {
	AggregationBits HexBytes                     `json:"aggregation_bits"`
	Data            *PayloadAttestationDataGloas `json:"data"`
	Signature       phase0.BLSSignature          `json:"signature"`
}

type BuilderDepositRequestGloas struct {
	Pubkey                phase0.BLSPubKey    `json:"pubkey"`
	WithdrawalCredentials phase0.Root         `json:"withdrawal_credentials"`
	Amount                phase0.Gwei         `json:"amount"`
	Signature             phase0.BLSSignature `json:"signature"`
}

type BuilderExitRequestGloas struct {
	SourceAddress bellatrix.ExecutionAddress `json:"source_address"`
	Pubkey        phase0.BLSPubKey           `json:"pubkey"`
}

func decodeStrictJSON(input []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values")
		}
		return err
	}
	return nil
}

func requireJSONFields(input []byte, names ...string) error {
	fields := make(map[string]json.RawMessage)
	if err := json.Unmarshal(input, &fields); err != nil {
		return err
	}
	for _, name := range names {
		value, exists := fields[name]
		if !exists || bytes.Equal(value, []byte("null")) {
			return fmt.Errorf("%s missing", name)
		}
	}
	return nil
}

func (s *SignedBeaconBlockGloas) UnmarshalJSON(input []byte) error {
	type signedBeaconBlockGloas SignedBeaconBlockGloas
	if err := requireJSONFields(input, "message", "signature"); err != nil {
		return err
	}
	if err := decodeStrictJSON(input, (*signedBeaconBlockGloas)(s)); err != nil {
		return fmt.Errorf("invalid signed Gloas beacon block JSON: %w", err)
	}
	return s.Validate()
}

func (b *BeaconBlockGloas) UnmarshalJSON(input []byte) error {
	type beaconBlockGloas BeaconBlockGloas
	if err := requireJSONFields(input, "slot", "proposer_index", "parent_root", "state_root", "body"); err != nil {
		return err
	}
	return decodeStrictJSON(input, (*beaconBlockGloas)(b))
}

func (b *BeaconBlockBodyGloas) UnmarshalJSON(input []byte) error {
	type beaconBlockBodyGloas BeaconBlockBodyGloas
	if err := requireJSONFields(input,
		"randao_reveal", "eth1_data", "graffiti", "proposer_slashings", "attester_slashings",
		"attestations", "deposits", "voluntary_exits", "sync_aggregate", "bls_to_execution_changes",
		"signed_execution_payload_bid", "payload_attestations", "parent_execution_requests",
	); err != nil {
		return err
	}
	return decodeStrictJSON(input, (*beaconBlockBodyGloas)(b))
}

func (p *PayloadAttestationGloas) UnmarshalJSON(input []byte) error {
	type payloadAttestationGloas PayloadAttestationGloas
	if err := requireJSONFields(input, "aggregation_bits", "data", "signature"); err != nil {
		return err
	}
	if err := decodeStrictJSON(input, (*payloadAttestationGloas)(p)); err != nil {
		return err
	}
	return p.validate()
}

func (p *PayloadAttestationDataGloas) UnmarshalJSON(input []byte) error {
	type payloadAttestationDataGloas PayloadAttestationDataGloas
	if err := requireJSONFields(input, "beacon_block_root", "slot", "payload_present", "blob_data_available"); err != nil {
		return err
	}
	return decodeStrictJSON(input, (*payloadAttestationDataGloas)(p))
}

func (s *SignedBeaconBlockGloas) Validate() error {
	if s == nil || s.Message == nil || s.Message.Body == nil {
		return fmt.Errorf("signed Gloas beacon block is incomplete")
	}
	body := s.Message.Body
	if body.ETH1Data == nil || body.SyncAggregate == nil || body.SignedExecutionPayloadBid == nil ||
		body.SignedExecutionPayloadBid.Message == nil || body.ParentExecutionRequests == nil {
		return fmt.Errorf("signed Gloas beacon block body is incomplete")
	}
	if len(body.ETH1Data.BlockHash) != 32 || len(body.SyncAggregate.SyncCommitteeBits) != 64 {
		return fmt.Errorf("signed Gloas beacon block body has an invalid fixed-length field")
	}
	if body.ProposerSlashings == nil || body.AttesterSlashings == nil || body.Attestations == nil ||
		body.Deposits == nil || body.VoluntaryExits == nil || body.BLSToExecutionChanges == nil ||
		body.PayloadAttestations == nil {
		return fmt.Errorf("signed Gloas beacon block body list is missing")
	}
	if len(body.Deposits) != 0 {
		return fmt.Errorf("Gloas beacon block deposits must be empty")
	}
	if len(body.ProposerSlashings) > 16 || len(body.AttesterSlashings) > 1 || len(body.Attestations) > 8 ||
		len(body.VoluntaryExits) > 16 || len(body.BLSToExecutionChanges) > 16 ||
		len(body.PayloadAttestations) > 4 {
		return fmt.Errorf("signed Gloas beacon block body list exceeds consensus limit")
	}
	for _, values := range []any{body.ProposerSlashings, body.AttesterSlashings, body.Attestations, body.Deposits, body.VoluntaryExits, body.BLSToExecutionChanges} {
		value := reflect.ValueOf(values)
		for i := 0; i < value.Len(); i++ {
			if value.Index(i).IsNil() {
				return fmt.Errorf("signed Gloas beacon block contains a null operation")
			}
		}
	}
	for _, attestation := range body.PayloadAttestations {
		if attestation == nil {
			return fmt.Errorf("signed Gloas beacon block contains a null payload attestation")
		}
		if err := attestation.validate(); err != nil {
			return err
		}
	}
	return body.ParentExecutionRequests.validate()
}

func (p *PayloadAttestationGloas) validate() error {
	if p == nil || p.Data == nil {
		return fmt.Errorf("payload attestation data missing")
	}
	if len(p.AggregationBits) != 64 {
		return fmt.Errorf("payload attestation aggregation_bits has length %d, want 64", len(p.AggregationBits))
	}
	return nil
}

func (s *SignedBeaconBlockGloas) Equal(other *SignedBeaconBlockGloas) bool {
	return reflect.DeepEqual(s, other)
}

func (s *SignedBeaconBlockGloas) MarshalSSZ() ([]byte, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	message, err := s.Message.MarshalSSZ()
	if err != nil {
		return nil, err
	}
	result := make([]byte, gloasSignedBeaconBlockFixedSize, gloasSignedBeaconBlockFixedSize+len(message))
	binary.LittleEndian.PutUint32(result[:4], gloasSignedBeaconBlockFixedSize)
	copy(result[4:100], s.Signature[:])
	return append(result, message...), nil
}

func (s *SignedBeaconBlockGloas) UnmarshalSSZ(input []byte) error {
	if len(input) < gloasSignedBeaconBlockFixedSize+gloasBeaconBlockFixedSize ||
		binary.LittleEndian.Uint32(input[:4]) != gloasSignedBeaconBlockFixedSize {
		return fmt.Errorf("invalid signed Gloas beacon block SSZ")
	}
	copy(s.Signature[:], input[4:100])
	s.Message = new(BeaconBlockGloas)
	if err := s.Message.UnmarshalSSZ(input[gloasSignedBeaconBlockFixedSize:]); err != nil {
		return fmt.Errorf("decode Gloas beacon block: %w", err)
	}
	return s.Validate()
}

func (b *BeaconBlockGloas) MarshalSSZ() ([]byte, error) {
	if b == nil || b.Body == nil {
		return nil, fmt.Errorf("Gloas beacon block body missing")
	}
	body, err := b.Body.MarshalSSZ()
	if err != nil {
		return nil, err
	}
	result := make([]byte, gloasBeaconBlockFixedSize, gloasBeaconBlockFixedSize+len(body))
	binary.LittleEndian.PutUint64(result[0:8], uint64(b.Slot))
	binary.LittleEndian.PutUint64(result[8:16], uint64(b.ProposerIndex))
	copy(result[16:48], b.ParentRoot[:])
	copy(result[48:80], b.StateRoot[:])
	binary.LittleEndian.PutUint32(result[80:84], gloasBeaconBlockFixedSize)
	return append(result, body...), nil
}

func (b *BeaconBlockGloas) UnmarshalSSZ(input []byte) error {
	if len(input) < gloasBeaconBlockFixedSize || binary.LittleEndian.Uint32(input[80:84]) != gloasBeaconBlockFixedSize {
		return fmt.Errorf("invalid Gloas beacon block SSZ")
	}
	b.Slot = phase0.Slot(binary.LittleEndian.Uint64(input[0:8]))
	b.ProposerIndex = phase0.ValidatorIndex(binary.LittleEndian.Uint64(input[8:16]))
	copy(b.ParentRoot[:], input[16:48])
	copy(b.StateRoot[:], input[48:80])
	b.Body = new(BeaconBlockBodyGloas)
	return b.Body.UnmarshalSSZ(input[gloasBeaconBlockFixedSize:])
}

type sszMarshaler interface {
	MarshalSSZ() ([]byte, error)
}

type sszUnmarshaler interface {
	UnmarshalSSZ([]byte) error
}

func marshalSSZItems[T sszMarshaler](items []T, variable bool) ([]byte, error) {
	encoded := make([][]byte, len(items))
	total := 0
	if variable {
		total = len(items) * 4
	}
	for i, item := range items {
		value := reflect.ValueOf(item)
		if !value.IsValid() || (value.Kind() == reflect.Ptr && value.IsNil()) {
			return nil, fmt.Errorf("nil SSZ list item")
		}
		var err error
		encoded[i], err = item.MarshalSSZ()
		if err != nil {
			return nil, err
		}
		total += len(encoded[i])
	}
	result := make([]byte, 0, total)
	if variable {
		offset := len(items) * 4
		for _, item := range encoded {
			var encodedOffset [4]byte
			binary.LittleEndian.PutUint32(encodedOffset[:], uint32(offset)) //nolint:gosec
			result = append(result, encodedOffset[:]...)
			offset += len(item)
		}
	}
	for _, item := range encoded {
		result = append(result, item...)
	}
	return result, nil
}

func splitVariableSSZList(input []byte, maxItems int) ([][]byte, error) {
	if len(input) == 0 {
		return [][]byte{}, nil
	}
	if len(input) < 4 {
		return nil, fmt.Errorf("variable SSZ list is too short")
	}
	first := int(binary.LittleEndian.Uint32(input[:4]))
	if first < 4 || first%4 != 0 || first > len(input) {
		return nil, fmt.Errorf("invalid variable SSZ list first offset: %d", first)
	}
	count := first / 4
	if count > maxItems {
		return nil, fmt.Errorf("variable SSZ list has %d items, max %d", count, maxItems)
	}
	offsets := make([]int, count+1)
	for i := 0; i < count; i++ {
		offsets[i] = int(binary.LittleEndian.Uint32(input[i*4 : i*4+4]))
		if offsets[i] < first || offsets[i] > len(input) || (i > 0 && offsets[i] < offsets[i-1]) {
			return nil, fmt.Errorf("invalid variable SSZ list offset")
		}
	}
	offsets[count] = len(input)
	items := make([][]byte, count)
	for i := range items {
		items[i] = input[offsets[i]:offsets[i+1]]
	}
	return items, nil
}

func decodeFixedSSZItems[T sszUnmarshaler](input []byte, itemSize, maxItems int, newItem func() T) ([]T, error) {
	if len(input)%itemSize != 0 {
		return nil, fmt.Errorf("invalid fixed SSZ list length %d for item size %d", len(input), itemSize)
	}
	count := len(input) / itemSize
	if maxItems > 0 && count > maxItems {
		return nil, fmt.Errorf("fixed SSZ list has %d items, max %d", count, maxItems)
	}
	result := make([]T, count)
	for i := range result {
		result[i] = newItem()
		if err := result[i].UnmarshalSSZ(input[i*itemSize : (i+1)*itemSize]); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func decodeVariableSSZItems[T sszUnmarshaler](input []byte, maxItems int, newItem func() T) ([]T, error) {
	items, err := splitVariableSSZList(input, maxItems)
	if err != nil {
		return nil, err
	}
	result := make([]T, len(items))
	for i := range result {
		result[i] = newItem()
		if err := result[i].UnmarshalSSZ(items[i]); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func appendSSZOffset(dst []byte, offset int) []byte {
	var encoded [4]byte
	binary.LittleEndian.PutUint32(encoded[:], uint32(offset)) //nolint:gosec
	return append(dst, encoded[:]...)
}

func (b *BeaconBlockBodyGloas) MarshalSSZ() ([]byte, error) {
	if err := (&SignedBeaconBlockGloas{Message: &BeaconBlockGloas{Body: b}}).Validate(); err != nil {
		return nil, err
	}
	proposerSlashings, err := marshalSSZItems(b.ProposerSlashings, false)
	if err != nil {
		return nil, err
	}
	attesterSlashings, err := marshalSSZItems(b.AttesterSlashings, true)
	if err != nil {
		return nil, err
	}
	attestations, err := marshalSSZItems(b.Attestations, true)
	if err != nil {
		return nil, err
	}
	deposits, err := marshalSSZItems(b.Deposits, false)
	if err != nil {
		return nil, err
	}
	voluntaryExits, err := marshalSSZItems(b.VoluntaryExits, false)
	if err != nil {
		return nil, err
	}
	blsChanges, err := marshalSSZItems(b.BLSToExecutionChanges, false)
	if err != nil {
		return nil, err
	}
	signedBid, err := b.SignedExecutionPayloadBid.MarshalSSZ()
	if err != nil {
		return nil, err
	}
	payloadAttestations, err := marshalSSZItems(b.PayloadAttestations, false)
	if err != nil {
		return nil, err
	}
	parentRequests, err := b.ParentExecutionRequests.MarshalSSZ()
	if err != nil {
		return nil, err
	}

	variables := [][]byte{proposerSlashings, attesterSlashings, attestations, deposits, voluntaryExits, blsChanges, signedBid, payloadAttestations, parentRequests}
	result := make([]byte, 0, gloasBeaconBlockBodyFixedSize)
	result = append(result, b.RANDAOReveal[:]...)
	eth1Data, err := b.ETH1Data.MarshalSSZ()
	if err != nil {
		return nil, err
	}
	result = append(result, eth1Data...)
	result = append(result, b.Graffiti[:]...)
	offset := gloasBeaconBlockBodyFixedSize
	for i := 0; i < 5; i++ {
		result = appendSSZOffset(result, offset)
		offset += len(variables[i])
	}
	syncAggregate, err := b.SyncAggregate.MarshalSSZ()
	if err != nil {
		return nil, err
	}
	result = append(result, syncAggregate...)
	for i := 5; i < len(variables); i++ {
		result = appendSSZOffset(result, offset)
		offset += len(variables[i])
	}
	for _, variable := range variables {
		result = append(result, variable...)
	}
	return result, nil
}

func (b *BeaconBlockBodyGloas) UnmarshalSSZ(input []byte) error {
	if len(input) < gloasBeaconBlockBodyFixedSize {
		return fmt.Errorf("Gloas beacon block body SSZ is too short: %d", len(input))
	}
	copy(b.RANDAOReveal[:], input[0:96])
	b.ETH1Data = new(phase0.ETH1Data)
	if err := b.ETH1Data.UnmarshalSSZ(input[96:168]); err != nil {
		return err
	}
	copy(b.Graffiti[:], input[168:200])
	offsetPositions := [...]int{200, 204, 208, 212, 216, 380, 384, 388, 392}
	offsets := make([]int, len(offsetPositions)+1)
	for i, position := range offsetPositions {
		offsets[i] = int(binary.LittleEndian.Uint32(input[position : position+4]))
		if offsets[i] > len(input) || (i == 0 && offsets[i] != gloasBeaconBlockBodyFixedSize) || (i > 0 && offsets[i] < offsets[i-1]) {
			return fmt.Errorf("invalid Gloas beacon block body SSZ offset")
		}
	}
	offsets[len(offsetPositions)] = len(input)
	b.SyncAggregate = new(altair.SyncAggregate)
	if err := b.SyncAggregate.UnmarshalSSZ(input[220:380]); err != nil {
		return err
	}

	var err error
	b.ProposerSlashings, err = decodeFixedSSZItems(input[offsets[0]:offsets[1]], 416, 16, func() *phase0.ProposerSlashing { return new(phase0.ProposerSlashing) })
	if err != nil {
		return fmt.Errorf("decode proposer slashings: %w", err)
	}
	b.AttesterSlashings, err = decodeVariableSSZItems(input[offsets[1]:offsets[2]], 1, func() *electra.AttesterSlashing { return new(electra.AttesterSlashing) })
	if err != nil {
		return fmt.Errorf("decode attester slashings: %w", err)
	}
	b.Attestations, err = decodeVariableSSZItems(input[offsets[2]:offsets[3]], 8, func() *electra.Attestation { return new(electra.Attestation) })
	if err != nil {
		return fmt.Errorf("decode attestations: %w", err)
	}
	b.Deposits, err = decodeFixedSSZItems(input[offsets[3]:offsets[4]], 1240, 16, func() *phase0.Deposit { return new(phase0.Deposit) })
	if err != nil {
		return fmt.Errorf("decode deposits: %w", err)
	}
	b.VoluntaryExits, err = decodeFixedSSZItems(input[offsets[4]:offsets[5]], 112, 16, func() *phase0.SignedVoluntaryExit { return new(phase0.SignedVoluntaryExit) })
	if err != nil {
		return fmt.Errorf("decode voluntary exits: %w", err)
	}
	b.BLSToExecutionChanges, err = decodeFixedSSZItems(input[offsets[5]:offsets[6]], 172, 16, func() *capella.SignedBLSToExecutionChange { return new(capella.SignedBLSToExecutionChange) })
	if err != nil {
		return fmt.Errorf("decode BLS-to-execution changes: %w", err)
	}
	b.SignedExecutionPayloadBid = new(SignedExecutionPayloadBid)
	if err := b.SignedExecutionPayloadBid.UnmarshalSSZ(input[offsets[6]:offsets[7]]); err != nil {
		return fmt.Errorf("decode signed execution payload bid: %w", err)
	}
	b.PayloadAttestations, err = decodeFixedSSZItems(input[offsets[7]:offsets[8]], gloasPayloadAttestationSize, 4, func() *PayloadAttestationGloas { return new(PayloadAttestationGloas) })
	if err != nil {
		return fmt.Errorf("decode payload attestations: %w", err)
	}
	b.ParentExecutionRequests = new(ExecutionRequestsGloas)
	if err := b.ParentExecutionRequests.UnmarshalSSZ(input[offsets[8]:offsets[9]]); err != nil {
		return fmt.Errorf("decode parent execution requests: %w", err)
	}
	return nil
}

func (p *PayloadAttestationGloas) MarshalSSZ() ([]byte, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}
	result := make([]byte, 0, gloasPayloadAttestationSize)
	result = append(result, p.AggregationBits...)
	result = append(result, p.Data.BeaconBlockRoot[:]...)
	var slot [8]byte
	binary.LittleEndian.PutUint64(slot[:], uint64(p.Data.Slot))
	result = append(result, slot[:]...)
	if p.Data.PayloadPresent {
		result = append(result, 1)
	} else {
		result = append(result, 0)
	}
	if p.Data.BlobDataAvailable {
		result = append(result, 1)
	} else {
		result = append(result, 0)
	}
	result = append(result, p.Signature[:]...)
	return result, nil
}

func (p *PayloadAttestationGloas) UnmarshalSSZ(input []byte) error {
	if len(input) != gloasPayloadAttestationSize {
		return fmt.Errorf("invalid payload attestation SSZ size: %d", len(input))
	}
	p.AggregationBits = append(HexBytes(nil), input[0:64]...)
	p.Data = new(PayloadAttestationDataGloas)
	copy(p.Data.BeaconBlockRoot[:], input[64:96])
	p.Data.Slot = phase0.Slot(binary.LittleEndian.Uint64(input[96:104]))
	if input[104] > 1 || input[105] > 1 {
		return fmt.Errorf("invalid payload attestation boolean")
	}
	p.Data.PayloadPresent = input[104] == 1
	p.Data.BlobDataAvailable = input[105] == 1
	copy(p.Signature[:], input[106:202])
	return p.validate()
}

func (r *ExecutionRequestsGloas) MarshalSSZ() ([]byte, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	deposits, err := marshalSSZItems(r.Deposits, false)
	if err != nil {
		return nil, err
	}
	withdrawals, err := marshalSSZItems(r.Withdrawals, false)
	if err != nil {
		return nil, err
	}
	consolidations, err := marshalSSZItems(r.Consolidations, false)
	if err != nil {
		return nil, err
	}
	builderDeposits, err := marshalSSZItems(r.BuilderDeposits, false)
	if err != nil {
		return nil, err
	}
	builderExits, err := marshalSSZItems(r.BuilderExits, false)
	if err != nil {
		return nil, err
	}
	variables := [][]byte{deposits, withdrawals, consolidations, builderDeposits, builderExits}
	result := make([]byte, 0, 20)
	offset := 20
	for _, variable := range variables {
		result = appendSSZOffset(result, offset)
		offset += len(variable)
	}
	for _, variable := range variables {
		result = append(result, variable...)
	}
	return result, nil
}

func (r *ExecutionRequestsGloas) UnmarshalJSON(input []byte) error {
	type executionRequestsGloas ExecutionRequestsGloas
	// Accept the three-field request emitted by pre-EIP-8282 V6 clients and
	// canonicalize its two absent Gloas-only lists to empty.  Native Gloas
	// clients send all five fields, which are decoded and preserved below.
	if err := requireJSONFields(input, "deposits", "withdrawals", "consolidations"); err != nil {
		return err
	}
	if err := decodeStrictJSON(input, (*executionRequestsGloas)(r)); err != nil {
		return fmt.Errorf("invalid Gloas execution requests JSON: %w", err)
	}
	if r.BuilderDeposits == nil {
		r.BuilderDeposits = []*BuilderDepositRequestGloas{}
	}
	if r.BuilderExits == nil {
		r.BuilderExits = []*BuilderExitRequestGloas{}
	}
	return r.validate()
}

func (r *ExecutionRequestsGloas) UnmarshalSSZ(input []byte) error {
	if len(input) < 12 {
		return fmt.Errorf("Gloas execution requests SSZ is too short: %d", len(input))
	}
	fixedSize := int(binary.LittleEndian.Uint32(input[:4]))
	// The canonical Gloas shape has five variable fields (20 fixed bytes).
	// Keep accepting the earlier three-field shape from legacy/pre-patch V6
	// clients, treating the EIP-8282 lists as empty.
	fieldCount := 0
	switch fixedSize {
	case 12:
		fieldCount = 3
	case 20:
		fieldCount = 5
	default:
		return fmt.Errorf("invalid Gloas execution requests first offset: %d", fixedSize)
	}
	if len(input) < fixedSize {
		return fmt.Errorf("Gloas execution requests SSZ is too short: %d", len(input))
	}
	offsets := make([]int, fieldCount+1)
	for i := 0; i < fieldCount; i++ {
		offsets[i] = int(binary.LittleEndian.Uint32(input[i*4 : i*4+4]))
		if offsets[i] > len(input) || (i == 0 && offsets[i] != fixedSize) || (i > 0 && offsets[i] < offsets[i-1]) {
			return fmt.Errorf("invalid Gloas execution requests SSZ offset")
		}
	}
	offsets[fieldCount] = len(input)
	var err error
	// Gloas changed deposits to ProgressiveList[DepositRequest] and removed
	// Electra/Fulu's MAX_DEPOSIT_REQUESTS_PER_PAYLOAD limit.  A zero maximum
	// tells the fixed-item decoder to accept every item present in the bounded
	// request body.
	r.Deposits, err = decodeFixedSSZItems(input[offsets[0]:offsets[1]], 192, 0, func() *electra.DepositRequest { return new(electra.DepositRequest) })
	if err != nil {
		return err
	}
	r.Withdrawals, err = decodeFixedSSZItems(input[offsets[1]:offsets[2]], 76, gloasMaxWithdrawalRequests, func() *electra.WithdrawalRequest { return new(electra.WithdrawalRequest) })
	if err != nil {
		return err
	}
	r.Consolidations, err = decodeFixedSSZItems(input[offsets[2]:offsets[3]], 116, gloasMaxConsolidationRequests, func() *electra.ConsolidationRequest { return new(electra.ConsolidationRequest) })
	if err != nil {
		return err
	}
	if fieldCount == 3 {
		r.BuilderDeposits = []*BuilderDepositRequestGloas{}
		r.BuilderExits = []*BuilderExitRequestGloas{}
		return r.validate()
	}
	r.BuilderDeposits, err = decodeFixedSSZItems(input[offsets[3]:offsets[4]], gloasBuilderDepositRequestSize, gloasMaxBuilderDepositRequests, func() *BuilderDepositRequestGloas { return new(BuilderDepositRequestGloas) })
	if err != nil {
		return err
	}
	r.BuilderExits, err = decodeFixedSSZItems(input[offsets[4]:offsets[5]], gloasBuilderExitRequestSize, gloasMaxBuilderExitRequests, func() *BuilderExitRequestGloas { return new(BuilderExitRequestGloas) })
	if err != nil {
		return err
	}
	return r.validate()
}

func (r *ExecutionRequestsGloas) validate() error {
	if r == nil || r.Deposits == nil || r.Withdrawals == nil || r.Consolidations == nil || r.BuilderDeposits == nil || r.BuilderExits == nil {
		return fmt.Errorf("Gloas execution request list is missing")
	}
	if len(r.Withdrawals) > gloasMaxWithdrawalRequests || len(r.Consolidations) > gloasMaxConsolidationRequests || len(r.BuilderDeposits) > gloasMaxBuilderDepositRequests || len(r.BuilderExits) > gloasMaxBuilderExitRequests {
		return fmt.Errorf("Gloas execution request list exceeds consensus limit")
	}
	for _, list := range []any{r.Deposits, r.Withdrawals, r.Consolidations, r.BuilderDeposits, r.BuilderExits} {
		value := reflect.ValueOf(list)
		for i := 0; i < value.Len(); i++ {
			if value.Index(i).IsNil() {
				return fmt.Errorf("Gloas execution requests contain a null item")
			}
		}
	}
	return nil
}

func (r *BuilderDepositRequestGloas) UnmarshalJSON(input []byte) error {
	type builderDepositRequestGloas BuilderDepositRequestGloas
	if err := requireJSONFields(input, "pubkey", "withdrawal_credentials", "amount", "signature"); err != nil {
		return err
	}
	return decodeStrictJSON(input, (*builderDepositRequestGloas)(r))
}

func (r *BuilderExitRequestGloas) UnmarshalJSON(input []byte) error {
	type builderExitRequestGloas BuilderExitRequestGloas
	if err := requireJSONFields(input, "source_address", "pubkey"); err != nil {
		return err
	}
	return decodeStrictJSON(input, (*builderExitRequestGloas)(r))
}

func (r *BuilderDepositRequestGloas) MarshalSSZ() ([]byte, error) {
	result := make([]byte, 0, gloasBuilderDepositRequestSize)
	result = append(result, r.Pubkey[:]...)
	result = append(result, r.WithdrawalCredentials[:]...)
	var amount [8]byte
	binary.LittleEndian.PutUint64(amount[:], uint64(r.Amount))
	result = append(result, amount[:]...)
	result = append(result, r.Signature[:]...)
	return result, nil
}

func (r *BuilderDepositRequestGloas) UnmarshalSSZ(input []byte) error {
	if len(input) != gloasBuilderDepositRequestSize {
		return fmt.Errorf("invalid builder deposit request SSZ size: %d", len(input))
	}
	copy(r.Pubkey[:], input[0:48])
	copy(r.WithdrawalCredentials[:], input[48:80])
	r.Amount = phase0.Gwei(binary.LittleEndian.Uint64(input[80:88]))
	copy(r.Signature[:], input[88:184])
	return nil
}

func (r *BuilderExitRequestGloas) MarshalSSZ() ([]byte, error) {
	result := make([]byte, 0, gloasBuilderExitRequestSize)
	result = append(result, r.SourceAddress[:]...)
	result = append(result, r.Pubkey[:]...)
	return result, nil
}

func (r *BuilderExitRequestGloas) UnmarshalSSZ(input []byte) error {
	if len(input) != gloasBuilderExitRequestSize {
		return fmt.Errorf("invalid builder exit request SSZ size: %d", len(input))
	}
	copy(r.SourceAddress[:], input[0:20])
	copy(r.Pubkey[:], input[20:68])
	return nil
}
