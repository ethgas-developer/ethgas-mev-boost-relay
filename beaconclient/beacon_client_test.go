package beaconclient

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"bitbucket.org/infinity-exchange/mev-boost-relay/common"
	"bitbucket.org/infinity-exchange/mev-boost-relay/database"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/gorilla/mux"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testPubKey = "0x93247f2209abcacf57b75a51dafae777f9dd38bc7053d1af526f220a7489a6d3a2753e5f3e8b1cfe39b56f43611df74a"

var errTest = errors.New("test error")

type blockPublishRecord struct {
	slot               int64
	beaconIP           string
	slotStartTimestamp int64
	publishTimestamp   int64
	finishTimestamp    int64
	blockHash          string
	msIntoSlot         int64
}

type recordingBlockPublishDB struct {
	database.MockDB
	records chan blockPublishRecord
}

type recordingEnvelopeBeaconInstance struct {
	*MockBeaconInstance
	uri        string
	calls      chan<- string
	statusCode int
	err        error
}

func (instance *recordingEnvelopeBeaconInstance) GetPublishURI() string {
	return instance.uri
}

func (instance *recordingEnvelopeBeaconInstance) PublishExecutionPayloadEnvelope(envelope any, blobDataIncluded bool, broadcastMode BroadcastMode) (code int, err error) {
	instance.calls <- instance.uri
	if instance.statusCode == 0 {
		instance.statusCode = http.StatusOK
	}
	return instance.statusCode, instance.err
}

func (db *recordingBlockPublishDB) InsertBlockPublish(slot int64, beaconIP string, slotStartTimestamp, publishTimestamp, finishTimestamp int64, blockHash string, msIntoSlot int64) error {
	db.records <- blockPublishRecord{
		slot:               slot,
		beaconIP:           beaconIP,
		slotStartTimestamp: slotStartTimestamp,
		publishTimestamp:   publishTimestamp,
		finishTimestamp:    finishTimestamp,
		blockHash:          blockHash,
		msIntoSlot:         msIntoSlot,
	}
	return nil
}

func validatorResponseEntryToMap(entries []ValidatorResponseEntry) map[string]ValidatorResponseEntry {
	m := make(map[string]ValidatorResponseEntry)
	for _, entry := range entries {
		m[entry.Validator.Pubkey] = entry
	}
	return m
}

type testBackend struct {
	t               require.TestingT
	beaconInstances []*MockBeaconInstance
	beaconClient    IMultiBeaconClient
}

func newTestBackend(t require.TestingT, numBeaconNodes int) *testBackend {
	mockBeaconInstances := make([]*MockBeaconInstance, numBeaconNodes)
	beaconInstancesInterface := make([]IBeaconInstance, numBeaconNodes)
	for i := range numBeaconNodes {
		mockBeaconInstances[i] = NewMockBeaconInstance()
		beaconInstancesInterface[i] = mockBeaconInstances[i]
	}

	return &testBackend{
		t:               t,
		beaconInstances: mockBeaconInstances,
		beaconClient:    NewMultiBeaconClient(common.TestLog, beaconInstancesInterface),
	}
}

func TestBeaconInstance(t *testing.T) {
	r := mux.NewRouter()
	srv := httptest.NewServer(r)
	bc := NewProdBeaconInstance(common.TestLog, srv.URL, srv.URL)

	r.HandleFunc("/eth/v1/beacon/states/1/validators", func(w http.ResponseWriter, _ *http.Request) {
		resp := []byte(`{
  "execution_optimistic": false,
  "data": [
    {
      "index": "1",
      "balance": "1",
      "status": "active_ongoing",
      "validator": {
        "pubkey": "0x93247f2209abcacf57b75a51dafae777f9dd38bc7053d1af526f220a7489a6d3a2753e5f3e8b1cfe39b56f43611df74a",
        "withdrawal_credentials": "0xcf8e0d4e9587369b2301d0790347320302cc0943d5a1884560367e8208d920f2",
        "effective_balance": "1",
        "slashed": false,
        "activation_eligibility_epoch": "1",
        "activation_epoch": "1",
        "exit_epoch": "1",
        "withdrawable_epoch": "1"
      }
    }
  ]
}`)
		_, err := w.Write(resp)
		assert.NoError(t, err)
	})

	vals, err := bc.GetStateValidators("1")
	require.NoError(t, err)
	require.Len(t, vals.Data, 1)
	require.Contains(t, validatorResponseEntryToMap(vals.Data), "0x93247f2209abcacf57b75a51dafae777f9dd38bc7053d1af526f220a7489a6d3a2753e5f3e8b1cfe39b56f43611df74a")
}

func TestPublishExecutionPayloadEnvelope(t *testing.T) {
	r := mux.NewRouter()
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	r.HandleFunc("/eth/v1/beacon/genesis", func(w http.ResponseWriter, _ *http.Request) {
		_, err := w.Write([]byte(`{"data":{"genesis_time":"1"}}`))
		require.NoError(t, err)
	}).Methods(http.MethodGet)
	bc := NewProdBeaconInstance(common.TestLog, srv.URL, srv.URL)
	publishDB := &recordingBlockPublishDB{records: make(chan blockPublishRecord, 1)}
	bc.SetDB(publishDB)

	blockHash := phase0.Hash32{1, 2, 3}
	envelope := &common.SignedExecutionPayloadEnvelope{
		Message: &common.ExecutionPayloadEnvelope{
			Payload:      &common.ExecutionPayloadGloas{SlotNumber: 42, BlockHash: blockHash, BaseFeePerGas: uint256.NewInt(0)},
			BuilderIndex: 7,
		},
	}

	r.HandleFunc("/eth/v1/beacon/execution_payload_envelopes", func(w http.ResponseWriter, req *http.Request) {
		require.Equal(t, http.MethodPost, req.Method)
		require.Equal(t, "consensus", req.URL.Query().Get("broadcast_validation"))
		require.Equal(t, "gloas", req.Header.Get("Eth-Consensus-Version"))
		require.Equal(t, "true", req.Header.Get("Eth-Blob-Data-Included"))
		require.Equal(t, "application/json", req.Header.Get("Content-Type"))
		body, err := io.ReadAll(req.Body)
		require.NoError(t, err)
		require.Contains(t, string(body), `"builder_index":"7"`)
		require.Contains(t, string(body), `"slot_number":"42"`)
		require.Contains(t, string(body), blockHash.String())
		w.WriteHeader(http.StatusOK)
	}).Methods(http.MethodPost)

	code, err := bc.PublishExecutionPayloadEnvelope(
		envelope,
		true,
		Consensus,
	)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, code)
	select {
	case record := <-publishDB.records:
		require.Equal(t, int64(42), record.slot)
		require.Equal(t, srv.URL, record.beaconIP)
		require.Equal(t, int64(1+42*common.SecondsPerSlot), record.slotStartTimestamp)
		require.Equal(t, blockHash.String(), record.blockHash)
		require.LessOrEqual(t, record.publishTimestamp, record.finishTimestamp)
		require.Equal(t, record.publishTimestamp-record.slotStartTimestamp*1000, record.msIntoSlot)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for block_publish record")
	}
}

func TestMultiBeaconClientPublishesExecutionPayloadEnvelopeToEveryEndpoint(t *testing.T) {
	calls := make(chan string, 2)
	instances := []IBeaconInstance{
		&recordingEnvelopeBeaconInstance{MockBeaconInstance: NewMockBeaconInstance(), uri: "beacon-a", calls: calls},
		&recordingEnvelopeBeaconInstance{MockBeaconInstance: NewMockBeaconInstance(), uri: "beacon-b", calls: calls},
	}
	client := NewMultiBeaconClient(common.TestLog, instances)

	code, err := client.PublishExecutionPayloadEnvelope(struct{}{}, false)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, code)

	called := make(map[string]bool, len(instances))
	for range instances {
		select {
		case uri := <-calls:
			called[uri] = true
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for all beacon publication endpoints")
		}
	}
	require.Equal(t, map[string]bool{"beacon-a": true, "beacon-b": true}, called)
}

func TestMultiBeaconClientExecutionPayloadEnvelopePublicationResults(t *testing.T) {
	t.Run("all accepted responses are not integrated", func(t *testing.T) {
		calls := make(chan string, 2)
		client := NewMultiBeaconClient(common.TestLog, []IBeaconInstance{
			&recordingEnvelopeBeaconInstance{MockBeaconInstance: NewMockBeaconInstance(), uri: "beacon-a", calls: calls, statusCode: http.StatusAccepted},
			&recordingEnvelopeBeaconInstance{MockBeaconInstance: NewMockBeaconInstance(), uri: "beacon-b", calls: calls, statusCode: http.StatusAccepted},
		})

		code, err := client.PublishExecutionPayloadEnvelope(struct{}{}, false)
		require.Equal(t, http.StatusAccepted, code)
		require.ErrorIs(t, err, ErrExecutionPayloadEnvelope202)
	})

	t.Run("one integrated response succeeds", func(t *testing.T) {
		calls := make(chan string, 2)
		client := NewMultiBeaconClient(common.TestLog, []IBeaconInstance{
			&recordingEnvelopeBeaconInstance{MockBeaconInstance: NewMockBeaconInstance(), uri: "beacon-a", calls: calls, statusCode: http.StatusAccepted},
			&recordingEnvelopeBeaconInstance{MockBeaconInstance: NewMockBeaconInstance(), uri: "beacon-b", calls: calls, statusCode: http.StatusOK},
		})

		code, err := client.PublishExecutionPayloadEnvelope(struct{}{}, false)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, code)
	})
}

func TestGetSyncStatus(t *testing.T) {
	t.Run("returns status of highest head slot", func(t *testing.T) {
		syncStatuses := []*SyncStatusPayloadData{
			{
				HeadSlot:  3,
				IsSyncing: true,
			},
			{
				HeadSlot:  1,
				IsSyncing: false,
			},
			{
				HeadSlot:  2,
				IsSyncing: false,
			},
		}

		backend := newTestBackend(t, 3)
		for i := range backend.beaconInstances {
			backend.beaconInstances[i].MockSyncStatus = syncStatuses[i]
			backend.beaconInstances[i].ResponseDelay = 10 * time.Millisecond * time.Duration(i)
		}

		status, err := backend.beaconClient.BestSyncStatus()
		require.NoError(t, err)
		require.Equal(t, syncStatuses[1], status)
	})

	t.Run("returns status if at least one beacon node does not return error and is synced", func(t *testing.T) {
		backend := newTestBackend(t, 2)
		backend.beaconInstances[0].MockSyncStatusErr = errTest
		status, err := backend.beaconClient.BestSyncStatus()
		require.NoError(t, err)
		require.NotNil(t, status)
	})

	t.Run("returns error if all beacon nodes return error or syncing", func(t *testing.T) {
		backend := newTestBackend(t, 2)
		backend.beaconInstances[0].MockSyncStatusErr = errTest
		backend.beaconInstances[1].MockSyncStatus = &SyncStatusPayloadData{
			HeadSlot:  1,
			IsSyncing: true,
		}
		status, err := backend.beaconClient.BestSyncStatus()
		require.Equal(t, ErrBeaconNodeSyncing, err)
		require.Nil(t, status)
	})
}

func TestUpdateProposerDuties(t *testing.T) {
	t.Run("returns err if all of the beacon nodes return error", func(t *testing.T) {
		backend := newTestBackend(t, 2)
		backend.beaconInstances[0].MockProposerDutiesErr = errTest
		backend.beaconInstances[1].MockProposerDutiesErr = errTest
		status, err := backend.beaconClient.GetProposerDuties(1)
		require.Error(t, err)
		require.Nil(t, status)
	})

	t.Run("get propose duties from the first beacon node that does not error", func(t *testing.T) {
		mockResponse := &ProposerDutiesResponse{
			Data: []ProposerDutiesResponseData{
				{
					Pubkey: testPubKey,
					Slot:   2,
				},
			},
		}

		backend := newTestBackend(t, 3)
		backend.beaconInstances[0].MockProposerDutiesErr = errTest
		backend.beaconInstances[1].ResponseDelay = 10 * time.Millisecond
		backend.beaconInstances[1].MockProposerDuties = mockResponse

		duties, err := backend.beaconClient.GetProposerDuties(2)
		require.NoError(t, err)
		require.Equal(t, *mockResponse, *duties)
	})
}

func TestFetchValidators(t *testing.T) {
	t.Run("returns err if all of the beacon nodes return error", func(t *testing.T) {
		backend := newTestBackend(t, 2)
		backend.beaconInstances[0].MockFetchValidatorsErr = errTest
		backend.beaconInstances[1].MockFetchValidatorsErr = errTest
		status, err := backend.beaconClient.GetStateValidators("1")
		require.Error(t, err)
		require.Nil(t, status)
	})

	t.Run("get validator set first from beacon node that did not err", func(t *testing.T) {
		entry := ValidatorResponseEntry{
			Validator: ValidatorResponseValidatorData{
				Pubkey: testPubKey,
			},
			Index:   0,
			Balance: "0",
			Status:  "",
		}

		backend := newTestBackend(t, 3)
		backend.beaconInstances[0].MockFetchValidatorsErr = errTest
		backend.beaconInstances[1].AddValidator(entry)
		backend.beaconInstances[2].MockFetchValidatorsErr = errTest

		validators, err := backend.beaconClient.GetStateValidators("1")
		require.NoError(t, err)
		require.Len(t, validators.Data, 1)
		require.Contains(t, validatorResponseEntryToMap(validators.Data), testPubKey)

		// only beacon 2 should have a validator, and should be used by default
		backend.beaconInstances[0].MockFetchValidatorsErr = nil
		backend.beaconInstances[1].SetValidators(make(map[common.PubkeyHex]ValidatorResponseEntry))
		backend.beaconInstances[2].MockFetchValidatorsErr = nil
		backend.beaconInstances[2].AddValidator(entry)

		validators, err = backend.beaconClient.GetStateValidators("1")
		require.NoError(t, err)
		require.Len(t, validators.Data, 1)
	})
}

func TestGetForkSchedule(t *testing.T) {
	r := mux.NewRouter()
	srv := httptest.NewServer(r)
	bc := NewProdBeaconInstance(common.TestLog, srv.URL, srv.URL)

	r.HandleFunc("/eth/v1/config/fork_schedule", func(w http.ResponseWriter, _ *http.Request) {
		resp := []byte(`{
			"data": [
			  {
				"previous_version": "0x00000010",
				"current_version": "0x00000020",
				"epoch": "0"
			  },
			  {
				"previous_version": "0x00000020",
				"current_version": "0x00000030",
				"epoch": "10"
			  },
			  {
				"previous_version": "0x00000030",
				"current_version": "0x00000040",
				"epoch": "20"
			  },
			  {
				"previous_version": "0x00000040",
				"current_version": "0x00000050",
				"epoch": "30"
			  }
			]
		  }`)
		_, err := w.Write(resp)
		assert.NoError(t, err)
	})

	forkSchedule, err := bc.GetForkSchedule()
	require.NoError(t, err)
	require.Len(t, forkSchedule.Data, 4)
}
