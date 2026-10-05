package beaconclient

import (
	"encoding/json"
	"time"

	"bitbucket.org/infinity-exchange/mev-boost-relay/common"
	"github.com/r3labs/sse/v2"
)

// ProposerPreferencesEvent is the versioned event emitted by the Beacon API's
// proposer_preferences SSE topic.
type ProposerPreferencesEvent struct {
	Version string                            `json:"version"`
	Data    *common.SignedProposerPreferences `json:"data"`
}

// IProposerPreferencesSubscriber is separate from IMultiBeaconClient so older
// test and third-party implementations remain source compatible. Production
// MultiBeaconClient implements it.
type IProposerPreferencesSubscriber interface {
	SubscribeToProposerPreferencesEvents(chan ProposerPreferencesEvent)
}

func (c *ProdBeaconInstance) SubscribeToProposerPreferencesEvents(preferencesC chan ProposerPreferencesEvent) {
	eventsURL := c.beaconURI + "/eth/v1/events?topics=proposer_preferences"
	log := c.log.WithField("url", eventsURL)
	log.Info("subscribing to proposer_preferences events")

	client := sse.NewClient(eventsURL)
	for {
		err := client.SubscribeRaw(func(msg *sse.Event) {
			var event ProposerPreferencesEvent
			if err := json.Unmarshal(msg.Data, &event); err != nil {
				log.WithError(err).Error("could not unmarshal proposer_preferences event")
				return
			}
			preferencesC <- event
		})
		if err != nil {
			log.WithError(err).Error("failed to subscribe to proposer_preferences events")
			time.Sleep(time.Second)
		}
		log.Warn("beaconclient SubscribeRaw/SubscribeToProposerPreferencesEvents ended, reconnecting")
		time.Sleep(500 * time.Millisecond)
	}
}

// SubscribeToProposerPreferencesEvents listens on all configured beacon nodes.
// The relay's Redis first-write policy deduplicates repeated events and rejects
// conflicting preferences for the same branch-scoped duty.
func (c *MultiBeaconClient) SubscribeToProposerPreferencesEvents(preferencesC chan ProposerPreferencesEvent) {
	for _, instance := range c.beaconInstances {
		subscriber, ok := instance.(interface {
			SubscribeToProposerPreferencesEvents(chan ProposerPreferencesEvent)
		})
		if ok {
			go subscriber.SubscribeToProposerPreferencesEvents(preferencesC)
		}
	}
}
