// Package housekeeper contains the service doing all required regular tasks
//
// - Update known validators
// - Updating proposer duties
// - Saving metrics
// - Deleting old bids
// - ...
package housekeeper

import (
	"context"
	"errors"
	"net/http"
	_ "net/http/pprof"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"encoding/json"

	"bitbucket.org/infinity-exchange/mev-boost-relay/beaconclient"
	"bitbucket.org/infinity-exchange/mev-boost-relay/common"
	"bitbucket.org/infinity-exchange/mev-boost-relay/database"
	"bitbucket.org/infinity-exchange/mev-boost-relay/datastore"
	builderApiV1 "github.com/attestantio/go-builder-client/api/v1"
	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
	uberatomic "go.uber.org/atomic"
)

var (
	exchangeAPIURL       = GetEnvStr("EXCHANGE_API_URL", "http://localhost:3210")
	isOfacCheckingEnable = os.Getenv("OFAC_CHECKING") == "1"
)

func GetEnvStr(key, defaultValue string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return defaultValue
}

type HousekeeperOpts struct {
	Log          *logrus.Entry
	Redis        *datastore.RedisCache
	DB           database.IDatabaseService
	BeaconClient beaconclient.IMultiBeaconClient

	PprofAPI           bool
	PprofListenAddress string
}

type Housekeeper struct {
	opts *HousekeeperOpts
	log  *logrus.Entry

	redis        *datastore.RedisCache
	db           database.IDatabaseService
	beaconClient beaconclient.IMultiBeaconClient

	pprofAPI           bool
	pprofListenAddress string

	isStarted                uberatomic.Bool
	isUpdatingProposerDuties uberatomic.Bool
	proposerDutiesSlot       uint64

	headSlot uberatomic.Uint64

	proposersAlreadySaved map[uint64]string // to avoid repeating redis writes

	httpClient *http.Client
}

var ErrServerAlreadyStarted = errors.New("server was already started")

func NewHousekeeper(opts *HousekeeperOpts) *Housekeeper {
	server := &Housekeeper{
		opts:                  opts,
		log:                   opts.Log,
		redis:                 opts.Redis,
		db:                    opts.DB,
		beaconClient:          opts.BeaconClient,
		pprofAPI:              opts.PprofAPI,
		pprofListenAddress:    opts.PprofListenAddress,
		proposersAlreadySaved: make(map[uint64]string),
		httpClient:            &http.Client{Timeout: 10 * time.Second},
	}

	return server
}

// Start starts the housekeeper service, blocking
func (hk *Housekeeper) Start() (err error) {
	defer hk.isStarted.Store(false)
	if hk.isStarted.Swap(true) {
		return ErrServerAlreadyStarted
	}

	// Get best beacon-node status by head slot, process current slot and start slot updates
	bestSyncStatus, err := hk.beaconClient.BestSyncStatus()
	if err != nil {
		return err
	}

	// Start pprof API, if requested
	if hk.pprofAPI {
		go hk.startPprofAPI()
	}

	// Start initial tasks
	go hk.updateValidatorRegistrationsInRedis()

	// Process the current slot
	hk.processNewSlot(bestSyncStatus.HeadSlot)

	// Start regular slot updates
	c := make(chan beaconclient.HeadEventData)
	hk.beaconClient.SubscribeToHeadEvents(c)
	for {
		headEvent := <-c
		hk.processNewSlot(headEvent.Slot)
	}
}

func (hk *Housekeeper) startPprofAPI() {
	r := mux.NewRouter()
	hk.log.Infof("Starting pprof API at %s", hk.pprofListenAddress)
	r.PathPrefix("/debug/pprof/").Handler(http.DefaultServeMux)
	srv := http.Server{ //nolint:gosec
		Addr:    hk.pprofListenAddress,
		Handler: r,
	}
	err := srv.ListenAndServe()
	if err != nil {
		hk.log.WithError(err).Error("failed to start pprof API")
	}
}

func (hk *Housekeeper) processNewSlot(headSlot uint64) {
	prevHeadSlot := hk.headSlot.Load()
	if headSlot <= prevHeadSlot {
		return
	}
	hk.headSlot.Store(headSlot)

	log := hk.log.WithFields(logrus.Fields{
		"headSlot":     headSlot,
		"headSlotPos":  common.SlotPos(headSlot),
		"prevHeadSlot": prevHeadSlot,
	})

	// Print any missed slots
	if prevHeadSlot > 0 {
		for s := prevHeadSlot + 1; s < headSlot; s++ {
			log.WithField("missedSlot", s).Warnf("missed slot: %d", s)
		}
	}

	// Update proposer duties
	go hk.updateProposerDuties(headSlot)

	// Set headSlot in redis (for the website)
	err := hk.redis.SetStats(datastore.RedisStatsFieldLatestSlot, headSlot)
	if err != nil {
		log.WithError(err).Error("failed to set stats")
	}

	currentEpoch := headSlot / common.SlotsPerEpoch
	log.WithFields(logrus.Fields{
		"epoch":              currentEpoch,
		"slotStartNextEpoch": (currentEpoch + 1) * common.SlotsPerEpoch,
	}).Infof("updated headSlot to %d", headSlot)
}

func (hk *Housekeeper) updateProposerDuties(headSlot uint64) {
	// Should only happen once at a time
	if hk.isUpdatingProposerDuties.Swap(true) {
		return
	}
	defer hk.isUpdatingProposerDuties.Store(false)

	slotsForHalfAnEpoch := common.SlotsPerEpoch / 2
	if headSlot%slotsForHalfAnEpoch != 0 && headSlot-hk.proposerDutiesSlot < slotsForHalfAnEpoch {
		return
	}

	hk.UpdateProposerDutiesWithoutChecks(headSlot)
}

func (hk *Housekeeper) UpdateProposerDutiesWithoutChecks(headSlot uint64) {
	epoch := headSlot / common.SlotsPerEpoch

	log := hk.log.WithFields(logrus.Fields{
		"epochFrom": epoch,
		"epochTo":   epoch + 1,
	})
	log.Debug("updating proposer duties...")

	// Query current epoch
	r, err := hk.beaconClient.GetProposerDuties(epoch)
	if err != nil {
		log.WithError(err).Error("failed to get proposer duties for all beacon nodes")
		return
	}
	entries := r.Data

	// Query next epoch
	r2, err := hk.beaconClient.GetProposerDuties(epoch + 1)
	if err != nil {
		log.WithError(err).Error("failed to get proposer duties for next epoch for all beacon nodes")
	} else if r2 != nil {
		entries = append(entries, r2.Data...)
	}

	// Get registrations from database
	pubkeys := []string{}
	for _, entry := range entries {
		pubkeys = append(pubkeys, entry.Pubkey)
	}
	validatorRegistrationEntries, err := hk.db.GetValidatorRegistrationsForPubkeys(pubkeys)
	if err != nil {
		log.WithError(err).Error("failed to get validator registrations")
		return
	}

	// Only check OFAC for pubkeys that actually have a registration
	regPubkeys := make([]string, 0, len(validatorRegistrationEntries))
	for _, regEntry := range validatorRegistrationEntries {
		regPubkeys = append(regPubkeys, regEntry.Pubkey)
	}

	// get Ofac list here
	ofacList := hk.checkValidatorsIsOfac(regPubkeys)
	ofacSet := make(map[string]struct{}, len(ofacList))
	for _, pk := range ofacList {
		ofacSet[pk] = struct{}{}
	}

	// Convert db entries to signed validator registration type
	signedValidatorRegistrations := make(map[string]*builderApiV1.SignedValidatorRegistration)
	for _, regEntry := range validatorRegistrationEntries {
		signedEntry, err := regEntry.ToSignedValidatorRegistration()
		if err != nil {
			log.WithError(err).Error("failed to convert validator registration entry to signed validator registration")
			continue
		}
		signedValidatorRegistrations[regEntry.Pubkey] = signedEntry
	}

	// Prepare proposer duties
	proposerDuties := []common.BuilderGetValidatorsResponseEntry{}
	for _, duty := range entries {
		reg := signedValidatorRegistrations[duty.Pubkey]
		var entryPreferences *common.Preferences
		if _, isOfac := ofacSet[strings.ToLower(duty.Pubkey)]; isOfac {
			log.Debug("Validator is OFAC")

			filtering := "ofac"
			entryPreferences = &common.Preferences{Filtering: &filtering}
		}
		if reg != nil {
			proposerDuties = append(proposerDuties, common.BuilderGetValidatorsResponseEntry{
				Slot:           duty.Slot,
				ValidatorIndex: duty.ValidatorIndex,
				Entry:          reg,
				Preferences:    entryPreferences,
			})
		}
	}

	// Save duties to Redis
	err = hk.redis.SetProposerDuties(proposerDuties)
	if err != nil {
		log.WithError(err).Error("failed to set proposer duties")
		return
	}
	hk.proposerDutiesSlot = headSlot

	// Pretty-print
	_duties := make([]string, len(proposerDuties))
	for i, duty := range proposerDuties {
		_duties[i] = strconv.FormatUint(duty.Slot, 10)
	}
	sort.Strings(_duties)
	log.WithField("numDuties", len(_duties)).Infof("proposer duties updated: %s", strings.Join(_duties, ", "))
}

// updateValidatorRegistrationsInRedis saves all latest validator registrations from the database to Redis
func (hk *Housekeeper) updateValidatorRegistrationsInRedis() {
	regs, err := hk.db.GetLatestValidatorRegistrations(true)
	if err != nil {
		hk.log.WithError(err).Error("failed to get latest validator registrations")
		return
	}

	hk.log.Infof("updating %d validator registrations in Redis...", len(regs))
	timeStarted := time.Now()

	for _, reg := range regs {
		err = hk.redis.SetValidatorRegistrationTimestampIfNewer(common.NewPubkeyHex(reg.Pubkey), reg.Timestamp)
		if err != nil {
			hk.log.WithError(err).Error("failed to set validator registration")
			continue
		}
	}
	hk.log.Infof("updating %d validator registrations in Redis done - %f sec", len(regs), time.Since(timeStarted).Seconds())
}

func (hk *Housekeeper) checkValidatorsIsOfac(pubkeys []string) []string {
	if isOfacCheckingEnable {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Lowercase and dedup input
		seen := make(map[string]struct{}, len(pubkeys))
		lower := make([]string, 0, len(pubkeys))
		for _, pk := range pubkeys {
			pkl := strings.ToLower(pk)
			if _, ok := seen[pkl]; !ok {
				seen[pkl] = struct{}{}
				lower = append(lower, pkl)
			}
		}

		type ofacData struct {
			OfacValidators []string `json:"ofacValidator"`
		}
		type apiResponse struct {
			Success bool     `json:"success"`
			Data    ofacData `json:"data"`
		}

		const batchSize = 16
		ofacSet := make(map[string]struct{})

		for i := 0; i < len(lower); i += batchSize {
			end := i + batchSize
			if end > len(lower) {
				end = len(lower)
			}
			batch := lower[i:end]

			url := exchangeAPIURL + "/api/v1/p/validator/checkIsOfac?publicKeys=" + strings.Join(batch, ",")
			hk.log.Infof("checking %d validator registrations for OFAC...", len(batch))
			hk.log.Info(url)
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
			if err != nil {
				hk.log.WithError(err).Error("failed to create checkValidatorsIsOfac request")
				continue
			}

			resp, err := hk.httpClient.Do(req)
			if err != nil {
				hk.log.WithError(err).Warn("exchange checkValidatorsIsOfac failed")
				continue
			}
			if resp.StatusCode != http.StatusOK {
				hk.log.Warnf("checkValidatorsIsOfac returned non-200 status: %d", resp.StatusCode)
				resp.Body.Close()
				continue
			}
			var result apiResponse
			if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
				hk.log.WithError(err).Error("failed to decode checkValidatorsIsOfac response")
				resp.Body.Close()
				continue
			}
			resp.Body.Close()

			for _, pk := range result.Data.OfacValidators {
				ofacSet[strings.ToLower(pk)] = struct{}{}
			}
			hk.log.Infof("OFAC response retrieved: %+v", result.Data)
			hk.log.Infof("OFAC list retrieved: %v", result.Data.OfacValidators)
		}

		// Convert set to slice
		out := make([]string, 0, len(ofacSet))

		for pk := range ofacSet {
			hk.log.Infof("OFAC validator: %s", pk)
			out = append(out, pk)
		}
		return out
	}
	return make([]string, 0)
}
