package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"bitbucket.org/infinity-exchange/mev-boost-relay/common"
	builderAPIFulu "github.com/attestantio/go-builder-client/api/fulu"
	builderAPIV1 "github.com/attestantio/go-builder-client/api/v1"
	"github.com/attestantio/go-eth2-client/spec"
	"github.com/attestantio/go-eth2-client/spec/bellatrix"
	"github.com/attestantio/go-eth2-client/spec/capella"
	"github.com/attestantio/go-eth2-client/spec/deneb"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func TestBlockSimulationRateLimiterUsesV6ForGloas(t *testing.T) {
	type capturedSimulation struct {
		Method            string
		ExecutionRequests *common.ExecutionRequestsGloas
	}
	capturedC := make(chan capturedSimulation, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var rpcRequest struct {
			Method string `json:"method"`
			Params []struct {
				ExecutionRequests *common.ExecutionRequestsGloas `json:"execution_requests"`
			} `json:"params"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&rpcRequest))
		require.Len(t, rpcRequest.Params, 1)
		capturedC <- capturedSimulation{
			Method:            rpcRequest.Method,
			ExecutionRequests: rpcRequest.Params[0].ExecutionRequests,
		}
		w.Header().Set("Content-Type", "application/json")
		_, err := w.Write([]byte(`{"jsonrpc":"2.0","id":"1","result":{}}`))
		require.NoError(t, err)
	}))
	defer server.Close()

	basePayload := &deneb.ExecutionPayload{
		BaseFeePerGas: uint256.NewInt(1),
		Transactions:  []bellatrix.Transaction{},
		Withdrawals:   []*capella.Withdrawal{},
	}
	gloasPayload, err := common.NewExecutionPayloadGloas(basePayload, []byte{0xc0}, 42)
	require.NoError(t, err)
	executionRequests := common.NewExecutionRequestsGloas(nil)
	executionRequests.BuilderDeposits = []*common.BuilderDepositRequestGloas{{Amount: 1}}
	executionRequests.BuilderExits = []*common.BuilderExitRequestGloas{{}}
	gloas := &common.GloasSubmitBlockRequest{
		Message:           &builderAPIV1.BidTrace{},
		ExecutionPayload:  gloasPayload,
		BlobsBundle:       &builderAPIFulu.BlobsBundle{},
		ExecutionRequests: executionRequests,
	}
	fulu, err := gloas.AsFulu()
	require.NoError(t, err)
	root := phase0.Root{}
	req := &common.BuilderBlockValidationRequest{
		VersionedSubmitBlockRequest: &common.VersionedSubmitBlockRequest{},
		Gloas:                       gloas,
		RegisteredGasLimit:          30_000_000,
		ParentBeaconBlockRoot:       &root,
	}
	req.Version = spec.DataVersionFulu // Native V6 is identified by req.Gloas.
	req.Fulu = fulu

	limiter := NewBlockSimulationRateLimiter(server.URL)
	_, requestErr, validationErr := limiter.Send(t.Context(), req, false, false)
	require.NoError(t, requestErr)
	require.NoError(t, validationErr)
	captured := <-capturedC
	require.Equal(t, "flashbots_validateBuilderSubmissionV6", captured.Method)
	require.Equal(t, executionRequests, captured.ExecutionRequests)
}
