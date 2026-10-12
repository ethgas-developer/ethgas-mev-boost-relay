package api

import (
	"errors"
	"fmt"

	"github.com/attestantio/go-eth2-client/spec/bellatrix"
	"github.com/flashbots/go-boost-utils/utils"
)

// gloasSubmissionFeeRecipient applies main's exchange-buyer payout override
// to the incoming builder BidTrace. Signed proposer preferences remain the
// source of the protocol-facing Gloas bid recipient and gas-limit target.
func gloasSubmissionFeeRecipient(proposerRecipient bellatrix.ExecutionAddress, exchangeRecipient string) (bellatrix.ExecutionAddress, *bellatrix.ExecutionAddress, error) {
	if exchangeRecipient == "" {
		return proposerRecipient, nil, nil
	}
	recipient, err := utils.HexToAddress(exchangeRecipient)
	if err != nil {
		return bellatrix.ExecutionAddress{}, nil, fmt.Errorf("invalid exchange fee recipient: %w", err)
	}
	return recipient, &recipient, nil
}

// gloasPayloadPayoutRecipient does not fetch the exchange during the proposer
// deadline. Prefer the current validated per-slot response when known; when
// serving from another instance or after cache eviction, retain the payout
// policy atomically saved with the accepted payload in Redis.
func (api *RelayAPI) gloasPayloadPayoutRecipient(payload *gloasPayloadContext, proposerRecipient bellatrix.ExecutionAddress) (bellatrix.ExecutionAddress, error) {
	if disableEthgasMarketAPI {
		return proposerRecipient, nil
	}
	preconfCacheMutex.RLock()
	preconfs, known := preconfCache[payload.slot]
	preconfCacheMutex.RUnlock()
	if known {
		if preconfs.Slot == nil || *preconfs.Slot != payload.slot {
			return bellatrix.ExecutionAddress{}, errors.New("cached exchange bundles do not match Gloas slot")
		}
		recipient, _, err := gloasSubmissionFeeRecipient(proposerRecipient, preconfs.FeeRecipient)
		return recipient, err
	}
	if payload.exchangeFeeRecipient != nil {
		if *payload.exchangeFeeRecipient != payload.feeRecipient || payload.bidTrace == nil ||
			*payload.exchangeFeeRecipient != payload.bidTrace.ProposerFeeRecipient {
			return bellatrix.ExecutionAddress{}, errors.New("Gloas exchange payout cache metadata mismatch")
		}
		return *payload.exchangeFeeRecipient, nil
	}
	return proposerRecipient, nil
}
