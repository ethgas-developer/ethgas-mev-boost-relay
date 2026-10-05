package beaconclient

import (
	"bitbucket.org/infinity-exchange/mev-boost-relay/common"
	"github.com/attestantio/go-eth2-client/spec/capella"
)

type MockMultiBeaconClient struct {
	HeaderForSlot             *GetHeaderResponse
	HeaderForSlotErr          error
	GloasBlockForSlot         *common.SignedBeaconBlockGloas
	GloasBlockForSlotErr      error
	PublishedEnvelope         any
	PublishedBlobDataIncluded bool
	PublishEnvelopeCode       int
	PublishEnvelopeErr        error
}

func NewMockMultiBeaconClient() *MockMultiBeaconClient {
	return &MockMultiBeaconClient{PublishEnvelopeCode: 200}
}

func (*MockMultiBeaconClient) BestSyncStatus() (*SyncStatusPayloadData, error) {
	return &SyncStatusPayloadData{HeadSlot: 1}, nil //nolint:exhaustruct
}

func (*MockMultiBeaconClient) SubscribeToHeadEvents(slotC chan HeadEventData) {}

func (*MockMultiBeaconClient) SubscribeToPayloadAttributesEvents(payloadAttrC chan PayloadAttributesEvent) {
}

func (*MockMultiBeaconClient) GetStateValidators(stateID string) (*GetStateValidatorsResponse, error) {
	return nil, nil
}

func (*MockMultiBeaconClient) GetProposerDuties(epoch uint64) (*ProposerDutiesResponse, error) {
	return nil, nil
}

func (*MockMultiBeaconClient) PublishBlock(block *common.VersionedSignedProposal) (code int, err error) {
	return 0, nil
}

func (c *MockMultiBeaconClient) GetHeaderForSlot(slot uint64) (*GetHeaderResponse, error) {
	return c.HeaderForSlot, c.HeaderForSlotErr
}

func (c *MockMultiBeaconClient) GetGloasSignedBeaconBlock(slot uint64, contentType string) (*common.SignedBeaconBlockGloas, error) {
	return c.GloasBlockForSlot, c.GloasBlockForSlotErr
}

func (c *MockMultiBeaconClient) PublishExecutionPayloadEnvelope(envelope any, blobDataIncluded bool) (code int, err error) {
	c.PublishedEnvelope = envelope
	c.PublishedBlobDataIncluded = blobDataIncluded
	return c.PublishEnvelopeCode, c.PublishEnvelopeErr
}

func (*MockMultiBeaconClient) GetGenesis() (*GetGenesisResponse, error) {
	resp := &GetGenesisResponse{} //nolint:exhaustruct
	resp.Data.GenesisTime = 0
	return resp, nil
}

func (*MockMultiBeaconClient) GetSpec() (spec *GetSpecResponse, err error) {
	return nil, nil
}

func (*MockMultiBeaconClient) GetForkSchedule() (spec *GetForkScheduleResponse, err error) {
	resp := &GetForkScheduleResponse{
		Data: []struct {
			PreviousVersion string `json:"previous_version"`
			CurrentVersion  string `json:"current_version"`
			Epoch           uint64 `json:"epoch,string"`
		}{
			{
				CurrentVersion: "",
				Epoch:          1,
			},
		},
	}
	return resp, nil
}

func (*MockMultiBeaconClient) GetRandao(slot uint64) (spec *GetRandaoResponse, err error) {
	return nil, nil
}

func (*MockMultiBeaconClient) GetWithdrawals(slot uint64) (spec *GetWithdrawalsResponse, err error) {
	resp := &GetWithdrawalsResponse{}                                            //nolint:exhaustruct
	resp.Data.Withdrawals = append(resp.Data.Withdrawals, &capella.Withdrawal{}) //nolint:exhaustruct
	return resp, nil
}
