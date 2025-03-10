// Package api contains the API webserver for the proposer and block-builder APIs
package api

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/big"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"bitbucket.org/infinity-exchange/mev-boost-relay/beaconclient"
	"bitbucket.org/infinity-exchange/mev-boost-relay/common"
	"bitbucket.org/infinity-exchange/mev-boost-relay/database"
	"bitbucket.org/infinity-exchange/mev-boost-relay/datastore"
	"bitbucket.org/infinity-exchange/mev-boost-relay/metrics"
	"github.com/NYTimes/gziphandler"
	"github.com/aohorodnyk/mimeheader"
	builderApi "github.com/attestantio/go-builder-client/api"
	builderApiCapella "github.com/attestantio/go-builder-client/api/capella"
	builderApiDeneb "github.com/attestantio/go-builder-client/api/deneb"
	builderApiV1 "github.com/attestantio/go-builder-client/api/v1"
	"github.com/attestantio/go-eth2-client/spec"
	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/buger/jsonparser"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/signer/core/apitypes"
	"github.com/flashbots/go-boost-utils/bls"
	"github.com/flashbots/go-boost-utils/ssz"
	"github.com/flashbots/go-boost-utils/utils"
	"github.com/flashbots/go-utils/cli"
	"github.com/flashbots/go-utils/httplogger"
	"github.com/gorilla/mux"
	"github.com/holiman/uint256"
	"github.com/itsahedge/go-cowswap/util/signature-scheme/eip712"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	otelapi "go.opentelemetry.io/otel/metric"
	uberatomic "go.uber.org/atomic"
)

const (
	ErrBlockAlreadyKnown  = "simulation failed: block already known"
	ErrBlockRequiresReorg = "simulation failed: block requires a reorg"
	ErrMissingTrieNode    = "missing trie node"
)

var (
	ErrMissingLogOpt              = errors.New("log parameter is nil")
	ErrMissingBeaconClientOpt     = errors.New("beacon-client is nil")
	ErrMissingDatastoreOpt        = errors.New("proposer datastore is nil")
	ErrRelayPubkeyMismatch        = errors.New("relay pubkey does not match existing one")
	ErrServerAlreadyStarted       = errors.New("server was already started")
	ErrBuilderAPIWithoutSecretKey = errors.New("cannot start builder API without secret key")
	ErrNegativeTimestamp          = errors.New("timestamp cannot be negative")
)

var (
	// Proposer API (builder-specs)
	pathStatus            = "/eth/v1/builder/status"
	pathRegisterValidator = "/eth/v1/builder/validators"
	pathGetHeader         = "/eth/v1/builder/header/{slot:[0-9]+}/{parent_hash:0x[a-fA-F0-9]+}/{pubkey:0x[a-fA-F0-9]+}"
	pathGetPayload        = "/eth/v1/builder/blinded_blocks"

	// Block builder API
	pathBuilderGetValidators = "/relay/v1/builder/validators"
	pathSubmitNewBlock       = "/relay/v1/builder/blocks"

	// Data API
	pathDataProposerPayloadDelivered = "/relay/v1/data/bidtraces/proposer_payload_delivered"
	pathDataBuilderBidsReceived      = "/relay/v1/data/bidtraces/builder_blocks_received"
	pathDataValidatorRegistration    = "/relay/v1/data/validator_registration"

	// Internal API
	pathInternalBuilderStatus     = "/internal/v1/builder/{pubkey:0x[a-fA-F0-9]+}"
	pathInternalBuilderCollateral = "/internal/v1/builder/collateral/{pubkey:0x[a-fA-F0-9]+}"

	// number of goroutines to save active validator
	numValidatorRegProcessors = cli.GetEnvInt("NUM_VALIDATOR_REG_PROCESSORS", 10)

	// API URL
	exchangeAPIURL          = GetEnvStr("EXCHANGE_API_URL", "http://localhost:3210")
	chainID                 = GetEnvStr("CHAIN_ID", "7e7e")
	exchangeLoginPrivateKey = GetEnvStr("EXCHANGE_LOGIN_PRIVATE_KEY", "5eae315483f028b5cdd5d1090ff0c7618b18737ea9bf3c35047189db22835c48")
	defaultBuilder          = GetEnvStr("DEFAULT_BUILDER_PUBKEY", "0xa1885d66bef164889a2e35845c3b626545d7b0e513efe335e97c3a45e534013fa3bc38c3b7e6143695aecc4872ac52c4")

	// various timings
	timeoutGetPayloadRetryMs     = cli.GetEnvInt("GETPAYLOAD_RETRY_TIMEOUT_MS", 100)
	getExchangeFinalizedCutoffMs = cli.GetEnvInt("GETHEADER_EXCHANGE_FINALIZED_CUTOFF_MS", -2000)
	getTargetedBuilderCutoffMs   = cli.GetEnvInt("GETHEADER_TARGETED_BUILDER_REQUEST_CUTOFF_MS", 0)
	getHeaderRequestCutoffMs     = cli.GetEnvInt("GETHEADER_REQUEST_CUTOFF_MS", 3000)
	getPayloadRequestCutoffMs    = cli.GetEnvInt("GETPAYLOAD_REQUEST_CUTOFF_MS", 4000)
	getPayloadResponseDelayMs    = cli.GetEnvInt("GETPAYLOAD_RESPONSE_DELAY_MS", 1000)

	// api settings
	apiReadTimeoutMs       = cli.GetEnvInt("API_TIMEOUT_READ_MS", 1500)
	apiReadHeaderTimeoutMs = cli.GetEnvInt("API_TIMEOUT_READHEADER_MS", 600)
	apiIdleTimeoutMs       = cli.GetEnvInt("API_TIMEOUT_IDLE_MS", 3_000)
	apiWriteTimeoutMs      = cli.GetEnvInt("API_TIMEOUT_WRITE_MS", 10_000)
	apiMaxHeaderBytes      = cli.GetEnvInt("API_MAX_HEADER_BYTES", 60_000)
	apiMaxPayloadBytes     = cli.GetEnvInt("API_MAX_PAYLOAD_BYTES", 15*1024*1024) // 15 MiB

	// api shutdown: wait time (to allow removal from load balancer before stopping http server)
	apiShutdownWaitDuration = common.GetEnvDurationSec("API_SHUTDOWN_WAIT_SEC", 30)

	// api shutdown: whether to stop sending bids during shutdown phase (only useful if running a single-instance testnet setup)
	apiShutdownStopSendingBids = os.Getenv("API_SHUTDOWN_STOP_SENDING_BIDS") == "1"

	// maximum payload bytes for a block submission to be fast-tracked (large payloads slow down other fast-tracked requests!)
	fastTrackPayloadSizeLimit = cli.GetEnvInt("FAST_TRACK_PAYLOAD_SIZE_LIMIT", 230_000)

	// user-agents which shouldn't receive bids
	apiNoHeaderUserAgents = common.GetEnvStrSlice("NO_HEADER_USERAGENTS", []string{
		"mev-boost/v1.5.0 Go-http-client/1.1", // Prysm v4.0.1 (Shapella signing issue)
	})
)

// Declare a global variable for ApiClient
var client = &ApiClient{
	APIURL: exchangeAPIURL,
	// APIURL:  "http://localhost:3210",
	ChainID: chainID,
	Client: &http.Client{
		Timeout: 10 * time.Second,
	},
}

func GetEnvStr(key, defaultValue string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return defaultValue
}

// ApiClient represents the client for interacting with the API
type ApiClient struct {
	APIURL       string
	ChainID      string
	Client       *http.Client
	AccessToken  string
	RefreshToken string // Keeping this as a field for storing the refresh token
}

// LoginResponse represents the login response structure
type LoginResponse struct {
	Status        string `json:"status"`
	EIP712Message string `json:"eip712Message"`
	NonceHash     string `json:"nonceHash"`
}

// VerifyResponse represents the verification response structure
type VerifyResponse struct {
	User        User        `json:"user"`
	AccessToken AccessToken `json:"accessToken"`
}

// User represents the user details returned in the verification response
type User struct {
	UserID    uint64  `json:"userId"`
	Address   string  `json:"address"`
	UserType  uint32  `json:"userType"`
	UserClass *uint32 `json:"userClass,omitempty"`
}

// AccessToken represents the access token structure
type AccessToken struct {
	Token string `json:"token"`
}

// ApiResponse represents the standard API response structure
type ApiResponse struct {
	Success bool            `json:"success"`
	Data    json.RawMessage `json:"data"`
}

// Define the slotBundle type at the top of the file
// type SlotBundleResponse struct {
// 	SlotBundle PreconfBundles `json:"slotBundle"`
// }

type PreconfBundles struct {
	Bundles      []PreconfBundle `json:"bundles"`
	EmptySpace   string          `json:"emptySpace,omitempty"`
	feeRecipient string          `json:"feeRecipient,omitempty"`
}

// PreconfBundle represents a single preconfigured bundle.
type PreconfBundle struct {
	Txs             []PreconfTx `json:"txs"`
	UUID            string      `json:"replacementUuid"`
	AverageBidPrice float64     `json:"averageBidPrice"`
	BundleType      int         `json:"bundleType,omitempty"`
	Ordering        int         `json:"ordering,omitempty"`
}
type PreconfTx struct {
	Tx        string `json:"tx"`        // Transaction string
	TxHash    string `json:"txHash"`    // Transaction hash
	CanRevert bool   `json:"canRevert"` // Indicates if the transaction can be reverted
	// CreateDate uint64 `json:"createDate"` // Uncomment and use if needed
}

// // match this response
// // https://bitbucket.org/infinity-exchange/infinity-core/src/eafb819faac26b8fe23e27f44c0be30a2ef5058b/scripts/preconfMultiTxAsyncServer.ts?at=enhancement%2F8_jan
//
//	type PreconfResponse struct {
//		PreconfTxs        [][]byte `json:"preconf_txs"`
//		PreconfConditions struct {
//			OrderingMetaData struct {
//				Index int `json:"index"`
//			} `json:"ordering_meta_data"`
//		} `json:"preconf_conditions"`
//		SignedTxs   []string `json:"signedTxs"`
//		AvgBidPrice uint64   `json:"avg_bid_price"`
//	}
type DataResponse struct {
	Builder BuilderResponse `json:"builder"`
}

type BuilderResponse struct {
	Builder         string `json:"builder"`
	FallbackBuilder string `json:"fallbackBuilder"`
}

// RelayAPIOpts contains the options for a relay
type RelayAPIOpts struct {
	Log *logrus.Entry

	ListenAddr  string
	BlockSimURL string

	BeaconClient beaconclient.IMultiBeaconClient
	Datastore    *datastore.Datastore
	Redis        *datastore.RedisCache
	Memcached    *datastore.Memcached
	DB           database.IDatabaseService

	SecretKey *bls.SecretKey // used to sign bids (getHeader responses)

	// Network specific variables
	EthNetDetails common.EthNetworkDetails

	// APIs to enable
	ProposerAPI     bool
	BlockBuilderAPI bool
	DataAPI         bool
	PprofAPI        bool
	InternalAPI     bool
}

type payloadAttributesHelper struct {
	slot              uint64
	parentHash        string
	withdrawalsRoot   phase0.Root
	parentBeaconRoot  *phase0.Root
	payloadAttributes beaconclient.PayloadAttributes
}

// Data needed to issue a block validation request.
type blockSimOptions struct {
	isHighPrio bool
	fastTrack  bool
	log        *logrus.Entry
	builder    *blockBuilderCacheEntry
	req        *common.BuilderBlockValidationRequest
}

type blockBuilderCacheEntry struct {
	status     common.BuilderStatus
	collateral *big.Int
}

type blockSimResult struct {
	wasSimulated         bool
	blockValue           *uint256.Int
	optimisticSubmission bool
	requestErr           error
	validationErr        error
}

// RelayAPI represents a single Relay instance
type RelayAPI struct {
	opts RelayAPIOpts
	log  *logrus.Entry

	blsSk     *bls.SecretKey
	publicKey *phase0.BLSPubKey

	srv         *http.Server
	srvStarted  uberatomic.Bool
	srvShutdown uberatomic.Bool

	beaconClient beaconclient.IMultiBeaconClient
	datastore    *datastore.Datastore
	redis        *datastore.RedisCache
	memcached    *datastore.Memcached
	db           database.IDatabaseService

	headSlot     uberatomic.Uint64
	genesisInfo  *beaconclient.GetGenesisResponse
	capellaEpoch int64
	denebEpoch   int64
	electraEpoch int64

	proposerDutiesLock       sync.RWMutex
	proposerDutiesResponse   *[]byte // raw http response
	proposerDutiesMap        map[uint64]*common.BuilderGetValidatorsResponseEntry
	proposerDutiesSlot       uint64
	isUpdatingProposerDuties uberatomic.Bool

	blockSimRateLimiter IBlockSimRateLimiter

	validatorRegC chan builderApiV1.SignedValidatorRegistration

	// used to notify when a new validator has been registered
	validatorUpdateCh chan struct{}

	// used to wait on any active getPayload calls on shutdown
	getPayloadCallsInFlight sync.WaitGroup

	// Feature flags
	ffForceGetHeader204          bool
	ffDisableLowPrioBuilders     bool
	ffDisablePayloadDBStorage    bool // disable storing the execution payloads in the database
	ffLogInvalidSignaturePayload bool // log payload if getPayload signature validation fails
	ffEnableCancellations        bool // whether to enable block builder cancellations
	ffRegValContinueOnInvalidSig bool // whether to continue processing further validators if one fails
	ffIgnorableValidationErrors  bool // whether to enable ignorable validation errors

	payloadAttributes     map[string]payloadAttributesHelper // key:parentBlockHash
	payloadAttributesLock sync.RWMutex

	// The slot we are currently optimistically simulating.
	optimisticSlot uberatomic.Uint64
	// The number of optimistic blocks being processed (only used for logging).
	optimisticBlocksInFlight uberatomic.Uint64
	// Wait group used to monitor status of per-slot optimistic processing.
	optimisticBlocksWG sync.WaitGroup
	// Cache for builder statuses and collaterals.
	blockBuildersCache map[string]*blockBuilderCacheEntry
}

// Add a cache map and a mutex for thread safety
var (
	preconfCache      = make(map[uint64]PreconfBundles)
	preconfCacheMutex sync.RWMutex
	currentSlot       uint64
)

// NewRelayAPI creates a new service. if builders is nil, allow any builder
func NewRelayAPI(opts RelayAPIOpts) (api *RelayAPI, err error) {
	if err := metrics.Setup(context.Background()); err != nil {
		return nil, err
	}

	if opts.Log == nil {
		return nil, ErrMissingLogOpt
	}

	if opts.BeaconClient == nil {
		return nil, ErrMissingBeaconClientOpt
	}

	if opts.Datastore == nil {
		return nil, ErrMissingDatastoreOpt
	}

	// If block-builder API is enabled, then ensure secret key is all set
	var publicKey phase0.BLSPubKey
	if opts.BlockBuilderAPI {
		if opts.SecretKey == nil {
			return nil, ErrBuilderAPIWithoutSecretKey
		}

		// If using a secret key, ensure it's the correct one
		blsPubkey, err := bls.PublicKeyFromSecretKey(opts.SecretKey)
		if err != nil {
			return nil, err
		}
		publicKey, err = utils.BlsPublicKeyToPublicKey(blsPubkey)
		if err != nil {
			return nil, err
		}
		opts.Log.Infof("Using BLS key: %s", publicKey.String())

		// ensure pubkey is same across all relay instances
		_pubkey, err := opts.Redis.GetRelayConfig(datastore.RedisConfigFieldPubkey)
		if err != nil {
			return nil, err
		} else if _pubkey == "" {
			err := opts.Redis.SetRelayConfig(datastore.RedisConfigFieldPubkey, publicKey.String())
			if err != nil {
				return nil, err
			}
		} else if _pubkey != publicKey.String() {
			return nil, fmt.Errorf("%w: new=%s old=%s", ErrRelayPubkeyMismatch, publicKey.String(), _pubkey)
		}
	}

	api = &RelayAPI{
		opts:         opts,
		log:          opts.Log,
		blsSk:        opts.SecretKey,
		publicKey:    &publicKey,
		datastore:    opts.Datastore,
		beaconClient: opts.BeaconClient,
		redis:        opts.Redis,
		memcached:    opts.Memcached,
		db:           opts.DB,

		payloadAttributes: make(map[string]payloadAttributesHelper),

		proposerDutiesResponse: &[]byte{},
		blockSimRateLimiter:    NewBlockSimulationRateLimiter(opts.BlockSimURL),

		validatorRegC:     make(chan builderApiV1.SignedValidatorRegistration, 450_000),
		validatorUpdateCh: make(chan struct{}),
	}

	if os.Getenv("FORCE_GET_HEADER_204") == "1" {
		api.log.Warn("env: FORCE_GET_HEADER_204 - forcing getHeader to always return 204")
		api.ffForceGetHeader204 = true
	}

	if os.Getenv("DISABLE_LOWPRIO_BUILDERS") == "1" {
		api.log.Warn("env: DISABLE_LOWPRIO_BUILDERS - allowing only high-level builders")
		api.ffDisableLowPrioBuilders = true
	}

	if os.Getenv("DISABLE_PAYLOAD_DATABASE_STORAGE") == "1" {
		api.log.Warn("env: DISABLE_PAYLOAD_DATABASE_STORAGE - disabling storing payloads in the database")
		api.ffDisablePayloadDBStorage = true
	}

	if os.Getenv("LOG_INVALID_GETPAYLOAD_SIGNATURE") == "1" {
		api.log.Warn("env: LOG_INVALID_GETPAYLOAD_SIGNATURE - getPayload payloads with invalid proposer signature will be logged")
		api.ffLogInvalidSignaturePayload = true
	}

	if os.Getenv("ENABLE_BUILDER_CANCELLATIONS") == "1" {
		api.log.Warn("env: ENABLE_BUILDER_CANCELLATIONS - builders are allowed to cancel submissions when using ?cancellation=1")
		api.ffEnableCancellations = true
	}

	if os.Getenv("REGISTER_VALIDATOR_CONTINUE_ON_INVALID_SIG") == "1" {
		api.log.Warn("env: REGISTER_VALIDATOR_CONTINUE_ON_INVALID_SIG - validator registration will continue processing even if one validator has an invalid signature")
		api.ffRegValContinueOnInvalidSig = true
	}

	if os.Getenv("ENABLE_IGNORABLE_VALIDATION_ERRORS") == "1" {
		api.log.Warn("env: ENABLE_IGNORABLE_VALIDATION_ERRORS - some validation errors will be ignored")
		api.ffIgnorableValidationErrors = true
	}
	go InitLoginAndStartTokenRefresh()
	return api, nil
}

func (api *RelayAPI) getRouter() http.Handler {
	r := mux.NewRouter()

	r.HandleFunc("/", api.handleRoot).Methods(http.MethodGet)
	r.HandleFunc("/livez", api.handleLivez).Methods(http.MethodGet)
	r.HandleFunc("/readyz", api.handleReadyz).Methods(http.MethodGet)
	r.Handle("/metrics", promhttp.Handler()).Methods(http.MethodGet)

	// Proposer API
	if api.opts.ProposerAPI {
		api.log.Info("proposer API enabled")
		r.HandleFunc(pathStatus, api.handleStatus).Methods(http.MethodGet)
		r.HandleFunc(pathRegisterValidator, api.handleRegisterValidator).Methods(http.MethodPost)
		r.HandleFunc(pathGetHeader, api.handleGetHeader).Methods(http.MethodGet)
		r.HandleFunc(pathGetPayload, api.handleGetPayload).Methods(http.MethodPost)
	}

	// Builder API
	if api.opts.BlockBuilderAPI {
		api.log.Info("block builder API enabled")
		r.HandleFunc(pathBuilderGetValidators, api.handleBuilderGetValidators).Methods(http.MethodGet)
		r.HandleFunc(pathSubmitNewBlock, api.handleSubmitNewBlock).Methods(http.MethodPost)
	}

	// Data API
	if api.opts.DataAPI {
		api.log.Info("data API enabled")
		r.HandleFunc(pathDataProposerPayloadDelivered, api.handleDataProposerPayloadDelivered).Methods(http.MethodGet)
		r.HandleFunc(pathDataBuilderBidsReceived, api.handleDataBuilderBidsReceived).Methods(http.MethodGet)
		r.HandleFunc(pathDataValidatorRegistration, api.handleDataValidatorRegistration).Methods(http.MethodGet)
	}

	// Pprof
	if api.opts.PprofAPI {
		api.log.Info("pprof API enabled")
		r.PathPrefix("/debug/pprof/").Handler(http.DefaultServeMux)
	}

	// /internal/...
	if api.opts.InternalAPI {
		api.log.Info("internal API enabled")
		r.HandleFunc(pathInternalBuilderStatus, api.handleInternalBuilderStatus).Methods(http.MethodGet, http.MethodPost, http.MethodPut)
		r.HandleFunc(pathInternalBuilderCollateral, api.handleInternalBuilderCollateral).Methods(http.MethodPost, http.MethodPut)
	}

	mresp := common.MustB64Gunzip("H4sICAtOkWQAA2EudHh0AKWVPW+DMBCGd36Fe9fIi5Mt8uqqs4dIlZiCEqosKKhVO2Txj699GBtDcEl4JwTnh/t4dS7YWom2FcVaiETSDEmIC+pWLGRVgKrD3UY0iwnSj6THofQJDomiR13BnPgjvJDqNWX+OtzH7inWEGvr76GOCGtg3Kp7Ak+lus3zxLNtmXaMUncjcj1cwbOH3xBZtJCYG6/w+hdpB6ErpnqzFPZxO4FdXB3SAEgpscoDqWeULKmJA4qyfYFg0QV+p7hD8GGDd6C8+mElGDKab1CWeUQMVVvVDTJVj6nngHmNOmSoe6yH1BM3KZIKpuRaHKrOFd/3ksQwzdK+ejdM4VTzSDfjJsY1STeVTWb0T9JWZbJs8DvsNvwaddKdUy4gzVIzWWaWk3IF8D35kyUDf3FfKipwk/DYUee2nYyWQD0xEKDHeprzeXYwVmZD/lXt1OOg8EYhFfitsmQVcwmbUutpdt3PoqWdMyd2DYHKbgcmPlEYMxPjR6HhxOfuNG52xZr7TtzpygJJKNtWS14Uf0T6XSmzBwAA")
	r.HandleFunc("/miladyz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK); w.Write(mresp) }).Methods(http.MethodGet) //nolint:errcheck

	// r.Use(mux.CORSMethodMiddleware(r))
	loggedRouter := httplogger.LoggingMiddlewareLogrus(api.log, r)
	withGz := gziphandler.GzipHandler(loggedRouter)
	return withGz
}

// StartServer starts up this API instance and HTTP server
// - First it initializes the cache and updates local information
// - Once that is done, the HTTP server is started
func (api *RelayAPI) StartServer() (err error) {
	if api.srvStarted.Swap(true) {
		return ErrServerAlreadyStarted
	}

	log := api.log.WithField("method", "StartServer")

	// Get best beacon-node status by head slot, process current slot and start slot updates
	syncStatus, err := api.beaconClient.BestSyncStatus()
	if err != nil {
		return err
	}
	currentSlot := syncStatus.HeadSlot

	// Initialize block builder cache.
	api.blockBuildersCache = make(map[string]*blockBuilderCacheEntry)

	// Get genesis info
	api.genesisInfo, err = api.beaconClient.GetGenesis()
	if err != nil {
		return err
	}
	log.Infof("genesis info: %d", api.genesisInfo.Data.GenesisTime)

	// Get and prepare fork schedule
	forkSchedule, err := api.beaconClient.GetForkSchedule()
	if err != nil {
		return err
	}

	api.capellaEpoch = -1
	api.denebEpoch = -1
	api.electraEpoch = -1
	for _, fork := range forkSchedule.Data {
		log.Infof("forkSchedule: version=%s / epoch=%d", fork.CurrentVersion, fork.Epoch)
		switch fork.CurrentVersion {
		case api.opts.EthNetDetails.CapellaForkVersionHex:
			api.capellaEpoch = int64(fork.Epoch) //nolint:gosec
		case api.opts.EthNetDetails.DenebForkVersionHex:
			api.denebEpoch = int64(fork.Epoch) //nolint:gosec
		case api.opts.EthNetDetails.ElectraForkVersionHex:
			api.electraEpoch = int64(fork.Epoch) //nolint:gosec
		}
	}

	if api.denebEpoch == -1 {
		// log warning that deneb epoch was not found in CL fork schedule, suggest CL upgrade
		log.Info("Deneb epoch not found in fork schedule")
	}
	if api.electraEpoch == -1 {
		// log warning that electra epoch was not found in CL fork schedule, suggest CL upgrade
		log.Info("Electra epoch not found in fork schedule")
	}

	// Print fork version information
	if hasReachedFork(currentSlot, api.electraEpoch) {
		log.Infof("electra fork detected (currentEpoch: %d / electraEpoch: %d)", common.SlotToEpoch(currentSlot), api.electraEpoch)
	} else if hasReachedFork(currentSlot, api.denebEpoch) {
		log.Infof("deneb fork detected (currentEpoch: %d / denebEpoch: %d)", common.SlotToEpoch(currentSlot), api.denebEpoch)
	} else if hasReachedFork(currentSlot, api.capellaEpoch) {
		log.Infof("capella fork detected (currentEpoch: %d / capellaEpoch: %d)", common.SlotToEpoch(currentSlot), api.capellaEpoch)
	}

	// start proposer API specific things
	if api.opts.ProposerAPI {
		// Update known validators (which can take 10-30 sec). This is a requirement for service readiness, because without them,
		// getPayload() doesn't have the information it needs (known validators), which could lead to missed slots.
		go api.datastore.RefreshKnownValidators(api.log, api.beaconClient, currentSlot)

		// Start the validator registration db-save processor
		api.log.Infof("starting %d validator registration processors", numValidatorRegProcessors)
		for range numValidatorRegProcessors {
			go api.startValidatorRegistrationDBProcessor()
		}
	}

	// start block-builder API specific things
	if api.opts.BlockBuilderAPI {
		// Initialize metrics
		metrics.BuilderDemotionCount.Add(context.Background(), 0)

		// Get current proposer duties blocking before starting, to have them ready
		api.updateProposerDuties(syncStatus.HeadSlot)

		// Subscribe to payload attributes events (only for builder-api)
		go func() {
			c := make(chan beaconclient.PayloadAttributesEvent)
			api.beaconClient.SubscribeToPayloadAttributesEvents(c)
			for {
				payloadAttributes := <-c
				api.processPayloadAttributes(payloadAttributes)
			}
		}()
	}

	// Process current slot
	api.processNewSlot(currentSlot)

	// Start regular slot updates
	go func() {
		c := make(chan beaconclient.HeadEventData)
		api.beaconClient.SubscribeToHeadEvents(c)
		for {
			headEvent := <-c
			api.processNewSlot(headEvent.Slot)
		}
	}()

	// create and start HTTP server
	api.srv = &http.Server{
		Addr:    api.opts.ListenAddr,
		Handler: api.getRouter(),

		ReadTimeout:       time.Duration(apiReadTimeoutMs) * time.Millisecond,
		ReadHeaderTimeout: time.Duration(apiReadHeaderTimeoutMs) * time.Millisecond,
		WriteTimeout:      time.Duration(apiWriteTimeoutMs) * time.Millisecond,
		IdleTimeout:       time.Duration(apiIdleTimeoutMs) * time.Millisecond,
		MaxHeaderBytes:    apiMaxHeaderBytes,
	}
	err = api.srv.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (api *RelayAPI) IsReady() bool {
	// If server is shutting down, return false
	if api.srvShutdown.Load() {
		return false
	}

	// Proposer API readiness checks
	if api.opts.ProposerAPI {
		knownValidatorsUpdated := api.datastore.KnownValidatorsWasUpdated.Load()
		return knownValidatorsUpdated
	}

	// Block-builder API readiness checks
	return true
}

// StopServer gracefully shuts down the HTTP server:
// - Stop returning bids
// - Set ready /readyz to negative status
// - Wait a bit to allow removal of service from load balancer and draining of requests
func (api *RelayAPI) StopServer() (err error) {
	// avoid running this twice. setting srvShutdown to true makes /readyz switch to negative status
	if wasStopping := api.srvShutdown.Swap(true); wasStopping {
		return nil
	}

	// start server shutdown
	api.log.Info("Stopping server...")

	// stop returning bids on getHeader calls (should only be used when running a single instance)
	if api.opts.ProposerAPI && apiShutdownStopSendingBids {
		api.ffForceGetHeader204 = true
		api.log.Info("Disabled returning bids on getHeader")
	}

	// wait some time to get service removed from load balancer
	api.log.Infof("Waiting %.2f seconds before shutdown...", apiShutdownWaitDuration.Seconds())
	time.Sleep(apiShutdownWaitDuration)

	// wait for any active getPayload call to finish
	api.getPayloadCallsInFlight.Wait()

	// shutdown
	return api.srv.Shutdown(context.Background())
}

func (api *RelayAPI) ValidatorUpdateCh() chan struct{} {
	return api.validatorUpdateCh
}

func (api *RelayAPI) isCapella(slot uint64) bool {
	return hasReachedFork(slot, api.capellaEpoch) && !hasReachedFork(slot, api.denebEpoch)
}

func (api *RelayAPI) isDeneb(slot uint64) bool {
	return hasReachedFork(slot, api.denebEpoch) && !hasReachedFork(slot, api.electraEpoch)
}

func (api *RelayAPI) isElectra(slot uint64) bool {
	return hasReachedFork(slot, api.electraEpoch)
}

func (api *RelayAPI) startValidatorRegistrationDBProcessor() {
	for valReg := range api.validatorRegC {
		err := api.datastore.SaveValidatorRegistration(valReg)
		if err != nil {
			api.log.WithError(err).WithFields(logrus.Fields{
				"reg_pubkey":       valReg.Message.Pubkey,
				"reg_feeRecipient": valReg.Message.FeeRecipient,
				"reg_gasLimit":     valReg.Message.GasLimit,
				"reg_timestamp":    valReg.Message.Timestamp,
			}).Error("error saving validator registration")
		}
	}
}

// simulateBlock sends a request for a block simulation to blockSimRateLimiter.
func (api *RelayAPI) simulateBlock(ctx context.Context, opts blockSimOptions) (blockValue *uint256.Int, requestErr, validationErr error) {
	t := time.Now()
	response, requestErr, validationErr := api.blockSimRateLimiter.Send(ctx, opts.req, opts.isHighPrio, opts.fastTrack)
	log := opts.log.WithFields(logrus.Fields{
		"durationMs": time.Since(t).Milliseconds(),
		"numWaiting": api.blockSimRateLimiter.CurrentCounter(),
	})
	if validationErr != nil {
		if api.ffIgnorableValidationErrors {
			// Operators chooses to ignore certain validation errors
			ignoreError := validationErr.Error() == ErrBlockAlreadyKnown || validationErr.Error() == ErrBlockRequiresReorg || strings.Contains(validationErr.Error(), ErrMissingTrieNode)
			if ignoreError {
				log.WithError(validationErr).Warn("block validation failed with ignorable error")
				return nil, nil, nil
			}
		}
		log.WithError(validationErr).Warn("block validation failed")
		return nil, nil, validationErr
	}
	if requestErr != nil {
		log.WithError(requestErr).Warn("block validation failed: request error")
		return nil, requestErr, nil
	}

	log.Info("block validation successful")
	if response == nil {
		log.Warn("block validation response is nil")
		return nil, nil, nil
	}
	return response.BlockValue, nil, nil
}

func (api *RelayAPI) demoteBuilder(pubkey string, req *common.VersionedSubmitBlockRequest, simError error) {
	metrics.BuilderDemotionCount.Add(
		context.Background(),
		1,
	)

	builderEntry, ok := api.blockBuildersCache[pubkey]
	if !ok {
		api.log.Warnf("builder %v not in the builder cache", pubkey)
		builderEntry = &blockBuilderCacheEntry{} //nolint:exhaustruct
	}
	newStatus := common.BuilderStatus{
		IsHighPrio:    builderEntry.status.IsHighPrio,
		IsBlacklisted: builderEntry.status.IsBlacklisted,
		IsOptimistic:  false,
	}
	api.log.Infof("demoted builder, new status: %v", newStatus)
	if err := api.db.SetBlockBuilderIDStatusIsOptimistic(pubkey, false); err != nil {
		api.log.Error(fmt.Errorf("error setting builder: %v status: %w", pubkey, err))
	}
	// Write to demotions table.
	api.log.WithFields(logrus.Fields{
		"builderPubkey": pubkey,
		"slot":          req.Slot,
		"blockHash":     req.BlockHash,
		"demotionErr":   simError.Error(),
	}).Info("demoting builder")
	bidTrace, err := req.BidTrace()
	if err != nil {
		api.log.WithError(err).Warn("failed to get bid trace from submit block request")
	}
	if err := api.db.InsertBuilderDemotion(req, simError); err != nil {
		api.log.WithError(err).WithFields(logrus.Fields{
			"errorWritingDemotionToDB": true,
			"bidTrace":                 bidTrace,
			"simError":                 simError,
		}).Error("failed to save demotion to database")
	}
}

// processOptimisticBlock is called on a new goroutine when a optimistic block
// needs to be simulated.
func (api *RelayAPI) processOptimisticBlock(opts blockSimOptions, simResultC chan *blockSimResult) {
	api.optimisticBlocksInFlight.Add(1)
	defer func() { api.optimisticBlocksInFlight.Sub(1) }()
	api.optimisticBlocksWG.Add(1)
	defer api.optimisticBlocksWG.Done()

	ctx := context.Background()
	submission, err := common.GetBlockSubmissionInfo(opts.req.VersionedSubmitBlockRequest)
	if err != nil {
		opts.log.WithError(err).Error("error getting block submission info")
		return
	}
	builderPubkey := submission.BidTrace.BuilderPubkey.String()
	opts.log.WithFields(logrus.Fields{
		"builderPubkey": builderPubkey,
		// NOTE: this value is just an estimate because many goroutines could be
		// updating api.optimisticBlocksInFlight concurrently. Since we just use
		// it for logging, it is not atomic to avoid the performance impact.
		"optBlocksInFlight": api.optimisticBlocksInFlight,
	}).Infof("simulating optimistic block with hash: %v", submission.BidTrace.BlockHash.String())
	blockValue, reqErr, simErr := api.simulateBlock(ctx, opts)
	simResultC <- &blockSimResult{reqErr == nil, blockValue, true, reqErr, simErr}
	if reqErr != nil || simErr != nil {
		// Mark builder as non-optimistic.
		opts.builder.status.IsOptimistic = false
		api.log.WithError(simErr).Warn("block simulation failed in processOptimisticBlock, demoting builder")

		var demotionErr error
		if reqErr != nil {
			demotionErr = reqErr
		} else {
			demotionErr = simErr
		}

		// Demote the builder.
		api.demoteBuilder(builderPubkey, opts.req.VersionedSubmitBlockRequest, demotionErr)
	}
}

func (api *RelayAPI) processPayloadAttributes(payloadAttributes beaconclient.PayloadAttributesEvent) {
	apiHeadSlot := api.headSlot.Load()
	payloadAttrSlot := payloadAttributes.Data.ProposalSlot

	// require proposal slot in the future
	if payloadAttrSlot <= apiHeadSlot {
		return
	}
	log := api.log.WithFields(logrus.Fields{
		"headSlot":          apiHeadSlot,
		"payloadAttrSlot":   payloadAttrSlot,
		"payloadAttrParent": payloadAttributes.Data.ParentBlockHash,
	})

	// discard payload attributes if already known
	api.payloadAttributesLock.RLock()
	_, ok := api.payloadAttributes[getPayloadAttributesKey(payloadAttributes.Data.ParentBlockHash, payloadAttrSlot)]
	api.payloadAttributesLock.RUnlock()

	if ok {
		return
	}

	var withdrawalsRoot phase0.Root
	var err error
	if hasReachedFork(payloadAttrSlot, api.capellaEpoch) {
		withdrawalsRoot, err = ComputeWithdrawalsRoot(payloadAttributes.Data.PayloadAttributes.Withdrawals)
		log = log.WithField("withdrawalsRoot", withdrawalsRoot.String())
		if err != nil {
			log.WithError(err).Error("error computing withdrawals root")
			return
		}
	}

	var parentBeaconRoot *phase0.Root
	if hasReachedFork(payloadAttrSlot, api.denebEpoch) {
		if payloadAttributes.Data.PayloadAttributes.ParentBeaconBlockRoot == "" {
			log.Error("parent beacon block root in payload attributes is empty")
			return
		}
		// TODO: (deneb) HexToRoot util function
		hash, err := utils.HexToHash(payloadAttributes.Data.PayloadAttributes.ParentBeaconBlockRoot)
		if err != nil {
			log.WithError(err).Error("error parsing parent beacon block root from payload attributes")
			return
		}
		root := phase0.Root(hash)
		parentBeaconRoot = &root
	}

	api.payloadAttributesLock.Lock()
	defer api.payloadAttributesLock.Unlock()

	// Step 1: clean up old ones
	for parentBlockHash, attr := range api.payloadAttributes {
		if attr.slot < apiHeadSlot {
			delete(api.payloadAttributes, getPayloadAttributesKey(parentBlockHash, attr.slot))
		}
	}

	// Step 2: save new one
	api.payloadAttributes[getPayloadAttributesKey(payloadAttributes.Data.ParentBlockHash, payloadAttrSlot)] = payloadAttributesHelper{
		slot:              payloadAttrSlot,
		parentHash:        payloadAttributes.Data.ParentBlockHash,
		withdrawalsRoot:   withdrawalsRoot,
		parentBeaconRoot:  parentBeaconRoot,
		payloadAttributes: payloadAttributes.Data.PayloadAttributes,
	}

	log.WithFields(logrus.Fields{
		"randao":    payloadAttributes.Data.PayloadAttributes.PrevRandao,
		"timestamp": payloadAttributes.Data.PayloadAttributes.Timestamp,
	}).Info("updated payload attributes")
}

func (api *RelayAPI) processNewSlot(headSlot uint64) {
	prevHeadSlot := api.headSlot.Load()
	if headSlot <= prevHeadSlot {
		return
	}

	// If there's gaps between previous and new headslot, print the missed slots
	if prevHeadSlot > 0 {
		for s := prevHeadSlot + 1; s < headSlot; s++ {
			api.log.WithField("missedSlot", s).Warnf("missed slot: %d", s)
		}
	}

	// store the head slot
	api.headSlot.Store(headSlot)

	// only for builder-api
	if api.opts.BlockBuilderAPI || api.opts.ProposerAPI {
		// update proposer duties in the background
		go api.updateProposerDuties(headSlot)

		// update the optimistic slot
		go api.prepareBuildersForSlot(headSlot)
	}

	if api.opts.ProposerAPI {
		go api.datastore.RefreshKnownValidators(api.log, api.beaconClient, headSlot)
	}

	// log
	epoch := headSlot / common.SlotsPerEpoch
	api.log.WithFields(logrus.Fields{
		"epoch":              epoch,
		"slotHead":           headSlot,
		"slotStartNextEpoch": (epoch + 1) * common.SlotsPerEpoch,
	}).Infof("updated headSlot to %d", headSlot)
}

func (api *RelayAPI) updateProposerDuties(headSlot uint64) {
	// Ensure only one updating is running at a time
	if api.isUpdatingProposerDuties.Swap(true) {
		return
	}
	defer api.isUpdatingProposerDuties.Store(false)

	// Update once every 8 slots (or more, if a slot was missed)
	if headSlot%8 != 0 && headSlot-api.proposerDutiesSlot < 8 {
		return
	}

	api.UpdateProposerDutiesWithoutChecks(headSlot)
}

func (api *RelayAPI) UpdateProposerDutiesWithoutChecks(headSlot uint64) {
	// Load upcoming proposer duties from Redis
	duties, err := api.redis.GetProposerDuties()
	if err != nil {
		api.log.WithError(err).Error("failed getting proposer duties from redis")
		return
	}

	// Prepare raw bytes for HTTP response
	respBytes, err := json.Marshal(duties)
	if err != nil {
		api.log.WithError(err).Error("error marshalling duties")
	}

	// Prepare the map for lookup by slot
	dutiesMap := make(map[uint64]*common.BuilderGetValidatorsResponseEntry)
	for index, duty := range duties {
		dutiesMap[duty.Slot] = &duties[index]
	}

	// Update
	api.proposerDutiesLock.Lock()
	if len(respBytes) > 0 {
		api.proposerDutiesResponse = &respBytes
	}
	api.proposerDutiesMap = dutiesMap
	api.proposerDutiesSlot = headSlot
	api.proposerDutiesLock.Unlock()

	// pretty-print
	_duties := make([]string, len(duties))
	for i, duty := range duties {
		_duties[i] = strconv.FormatUint(duty.Slot, 10)
	}
	sort.Strings(_duties)
	api.log.Infof("proposer duties updated: %s", strings.Join(_duties, ", "))
}

func (api *RelayAPI) prepareBuildersForSlot(headSlot uint64) {
	// Wait until there are no optimistic blocks being processed. Then we can
	// safely update the slot.
	api.optimisticBlocksWG.Wait()
	api.optimisticSlot.Store(headSlot + 1)

	builders, err := api.db.GetBlockBuilders()
	if err != nil {
		api.log.WithError(err).Error("unable to read block builders from db, not updating builder cache")
		return
	}
	api.log.Debugf("Updating builder cache with %d builders from database", len(builders))

	newCache := make(map[string]*blockBuilderCacheEntry)
	for _, v := range builders {
		entry := &blockBuilderCacheEntry{ //nolint:exhaustruct
			status: common.BuilderStatus{
				IsHighPrio:    v.IsHighPrio,
				IsBlacklisted: v.IsBlacklisted,
				IsOptimistic:  v.IsOptimistic,
			},
		}
		// Try to parse builder collateral string to big int.
		builderCollateral, ok := big.NewInt(0).SetString(v.Collateral, 10)
		if !ok {
			api.log.WithError(err).Errorf("could not parse builder collateral string %s", v.Collateral)
			entry.collateral = big.NewInt(0)
		} else {
			entry.collateral = builderCollateral
		}
		newCache[v.BuilderPubkey] = entry
	}
	api.blockBuildersCache = newCache
}

func (api *RelayAPI) RespondError(w http.ResponseWriter, code int, message string) {
	api.Respond(w, code, HTTPErrorResp{code, message})
}

func (api *RelayAPI) RespondOK(w http.ResponseWriter, response any) {
	api.Respond(w, http.StatusOK, response)
}

func (api *RelayAPI) RespondMsg(w http.ResponseWriter, code int, msg string) {
	api.Respond(w, code, HTTPMessageResp{msg})
}

func (api *RelayAPI) Respond(w http.ResponseWriter, code int, response any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if response == nil {
		return
	}

	// write the json response
	if err := json.NewEncoder(w).Encode(response); err != nil {
		api.log.WithField("response", response).WithError(err).Error("Couldn't write response")
		http.Error(w, "", http.StatusInternalServerError)
	}
}

func (api *RelayAPI) handleStatus(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(http.StatusOK)
}

const (
	ApplicationJSON        = "application/json"
	ApplicationOctetStream = "application/octet-stream"
)

// RequestAcceptsJSON returns true if the Accept header is empty (defaults to JSON)
// or application/json can be negotiated.
func RequestAcceptsJSON(req *http.Request) bool {
	ah := req.Header.Get("Accept")
	if ah == "" {
		return true
	}
	mh := mimeheader.ParseAcceptHeader(ah)
	_, _, matched := mh.Negotiate(
		[]string{ApplicationJSON},
		ApplicationJSON,
	)
	return matched
}

// NegotiateRequestResponseType returns whether the request accepts
// JSON (application/json) or SSZ (application/octet-stream) responses.
// If accepted is false, no mime type could be negotiated and the server
// should respond with http.StatusNotAcceptable.
func NegotiateRequestResponseType(req *http.Request) (mimeType string, err error) {
	ah := req.Header.Get("Accept")
	if ah == "" {
		return ApplicationJSON, nil
	}
	mh := mimeheader.ParseAcceptHeader(ah)
	_, mimeType, matched := mh.Negotiate(
		[]string{ApplicationJSON, ApplicationOctetStream},
		ApplicationJSON,
	)
	if !matched {
		return "", ErrNotAcceptable
	}
	return mimeType, nil
}

// ---------------
//  PROPOSER APIS
// ---------------

func (api *RelayAPI) handleRoot(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "MEV-Boost Relay API")
}

func (api *RelayAPI) handleRegisterValidator(w http.ResponseWriter, req *http.Request) {
	ua := req.UserAgent()
	log := api.log.WithFields(logrus.Fields{
		"method":        "registerValidator",
		"ua":            ua,
		"mevBoostV":     common.GetMevBoostVersionFromUserAgent(ua),
		"headSlot":      api.headSlot.Load(),
		"contentLength": req.ContentLength,
	})

	// If the Content-Type header is included, for now only allow JSON.
	// TODO: support Content-Type: application/octet-stream and allow SSZ
	// request bodies.
	if ct := req.Header.Get("Content-Type"); ct != "" {
		switch ct {
		case ApplicationJSON:
			break
		default:
			api.RespondError(w, http.StatusUnsupportedMediaType, "only Content-Type: application/json is currently supported")
			return
		}
	}

	start := time.Now().UTC()
	registrationTimestampUpperBound := start.Unix() + 10 // 10 seconds from now

	numRegTotal := 0
	numRegProcessed := 0
	numRegActive := 0
	numRegNew := 0
	processingStoppedByError := false

	// Setup error handling
	handleError := func(_log *logrus.Entry, code int, msg string) {
		processingStoppedByError = true
		_log.Warnf("error: %s", msg)
		api.RespondError(w, code, msg)
	}

	// Start processing
	if req.ContentLength == 0 {
		log.Info("empty request")
		api.RespondError(w, http.StatusBadRequest, "empty request")
		return
	}

	limitReader := io.LimitReader(req.Body, int64(apiMaxPayloadBytes))
	body, err := io.ReadAll(limitReader)
	if err != nil {
		log.WithError(err).Warn("failed to read request body")
		api.RespondError(w, http.StatusBadRequest, "failed to read request body")
		return
	}
	req.Body.Close()

	parseRegistration := func(value []byte) (reg *builderApiV1.SignedValidatorRegistration, err error) {
		// Pubkey
		_pubkey, err := jsonparser.GetUnsafeString(value, "message", "pubkey")
		if err != nil {
			return nil, fmt.Errorf("registration message error (pubkey): %w", err)
		}

		pubkey, err := utils.HexToPubkey(_pubkey)
		if err != nil {
			return nil, fmt.Errorf("registration message error (pubkey): %w", err)
		}

		// Timestamp
		_timestamp, err := jsonparser.GetUnsafeString(value, "message", "timestamp")
		if err != nil {
			return nil, fmt.Errorf("registration message error (timestamp): %w", err)
		}

		timestamp, err := strconv.ParseInt(_timestamp, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid timestamp: %w", err)
		}
		if timestamp < 0 {
			return nil, ErrNegativeTimestamp
		}

		// GasLimit
		_gasLimit, err := jsonparser.GetUnsafeString(value, "message", "gas_limit")
		if err != nil {
			return nil, fmt.Errorf("registration message error (gasLimit): %w", err)
		}

		gasLimit, err := strconv.ParseUint(_gasLimit, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid gasLimit: %w", err)
		}

		// FeeRecipient
		_feeRecipient, err := jsonparser.GetUnsafeString(value, "message", "fee_recipient")
		if err != nil {
			return nil, fmt.Errorf("registration message error (fee_recipient): %w", err)
		}

		feeRecipient, err := utils.HexToAddress(_feeRecipient)
		if err != nil {
			return nil, fmt.Errorf("registration message error (fee_recipient): %w", err)
		}

		// Signature
		_signature, err := jsonparser.GetUnsafeString(value, "signature")
		if err != nil {
			return nil, fmt.Errorf("registration message error (signature): %w", err)
		}

		signature, err := utils.HexToSignature(_signature)
		if err != nil {
			return nil, fmt.Errorf("registration message error (signature): %w", err)
		}

		// Construct and return full registration object
		reg = &builderApiV1.SignedValidatorRegistration{
			Message: &builderApiV1.ValidatorRegistration{
				FeeRecipient: feeRecipient,
				GasLimit:     gasLimit,
				Timestamp:    time.Unix(timestamp, 0),
				Pubkey:       pubkey,
			},
			Signature: signature,
		}

		return reg, nil
	}

	// Iterate over the registrations
	_, err = jsonparser.ArrayEach(body, func(value []byte, dataType jsonparser.ValueType, offset int, _err error) {
		numRegTotal += 1
		if processingStoppedByError {
			return
		}
		numRegProcessed += 1
		regLog := log.WithFields(logrus.Fields{
			"numRegistrationsSoFar":     numRegTotal,
			"numRegistrationsProcessed": numRegProcessed,
		})

		// Extract immediately necessary registration fields
		signedValidatorRegistration, err := parseRegistration(value)
		if err != nil {
			handleError(regLog, http.StatusBadRequest, err.Error())
			return
		}

		// Add validator pubkey to logs
		pkHex := common.PubkeyHex(signedValidatorRegistration.Message.Pubkey.String())
		regLog = regLog.WithFields(logrus.Fields{
			"pubkey":       pkHex,
			"signature":    signedValidatorRegistration.Signature.String(),
			"feeRecipient": signedValidatorRegistration.Message.FeeRecipient.String(),
			"gasLimit":     signedValidatorRegistration.Message.GasLimit,
			"timestamp":    signedValidatorRegistration.Message.Timestamp,
		})

		// Ensure a valid timestamp (not too early, and not too far in the future)
		registrationTimestamp := signedValidatorRegistration.Message.Timestamp.Unix()
		if registrationTimestamp < int64(api.genesisInfo.Data.GenesisTime) { //nolint:gosec
			handleError(regLog, http.StatusBadRequest, "timestamp too early")
			return
		} else if registrationTimestamp > registrationTimestampUpperBound {
			handleError(regLog, http.StatusBadRequest, "timestamp too far in the future")
			return
		}

		// Check if a real validator
		isKnownValidator := api.datastore.IsKnownValidator(pkHex)
		if !isKnownValidator {
			handleError(regLog, http.StatusBadRequest, fmt.Sprintf("not a known validator: %s", pkHex))
			return
		}

		// Check for a previous registration timestamp
		prevTimestamp, err := api.redis.GetValidatorRegistrationTimestamp(pkHex)
		if err != nil {
			regLog.WithError(err).Error("error getting last registration timestamp")
		} else if prevTimestamp >= uint64(signedValidatorRegistration.Message.Timestamp.Unix()) { //nolint:gosec
			// abort if the current registration timestamp is older or equal to the last known one
			return
		}

		// Verify the signature
		ok, err := ssz.VerifySignature(signedValidatorRegistration.Message, api.opts.EthNetDetails.DomainBuilder, signedValidatorRegistration.Message.Pubkey[:], signedValidatorRegistration.Signature[:])
		if err != nil {
			regLog.WithError(err).Error("error verifying registerValidator signature")
			return
		} else if !ok {
			regLog.Info("invalid validator signature")
			if api.ffRegValContinueOnInvalidSig {
				return
			} else {
				handleError(regLog, http.StatusBadRequest, "failed to verify validator signature for "+signedValidatorRegistration.Message.Pubkey.String())
				return
			}
		}

		// Now we have a new registration to process
		numRegNew += 1

		// Save to database
		select {
		case api.validatorRegC <- *signedValidatorRegistration:
		default:
			regLog.Error("validator registration channel full")
		}
	})

	log = log.WithFields(logrus.Fields{
		"timeNeededSec":             time.Since(start).Seconds(),
		"timeNeededMs":              time.Since(start).Milliseconds(),
		"numRegistrations":          numRegTotal,
		"numRegistrationsActive":    numRegActive,
		"numRegistrationsProcessed": numRegProcessed,
		"numRegistrationsNew":       numRegNew,
		"processingStoppedByError":  processingStoppedByError,
	})

	if err != nil {
		handleError(log, http.StatusBadRequest, "error in traversing json")
		return
	}

	// notify that new registrations are available
	select {
	case api.validatorUpdateCh <- struct{}{}:
	default:
	}

	log.Info("validator registrations call processed")
	w.WriteHeader(http.StatusOK)
}

func (api *RelayAPI) handleGetHeader(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	slotStr := vars["slot"]
	parentHashHex := vars["parent_hash"]
	proposerPubkeyHex := vars["pubkey"]
	ua := req.UserAgent()
	headSlot := api.headSlot.Load()

	slot, err := strconv.ParseUint(slotStr, 10, 64)
	if err != nil {
		api.RespondError(w, http.StatusBadRequest, common.ErrInvalidSlot.Error())
		return
	}

	requestTime := time.Now().UTC()
	slotStartTimestamp := api.genesisInfo.Data.GenesisTime + (slot * common.SecondsPerSlot)
	msIntoSlot := requestTime.UnixMilli() - int64(slotStartTimestamp*1000) //nolint:gosec

	log := api.log.WithFields(logrus.Fields{
		"method":           "getHeader",
		"headSlot":         headSlot,
		"slot":             slotStr,
		"parentHash":       parentHashHex,
		"pubkey":           proposerPubkeyHex,
		"ua":               ua,
		"mevBoostV":        common.GetMevBoostVersionFromUserAgent(ua),
		"requestTimestamp": requestTime.Unix(),
		"slotStartSec":     slotStartTimestamp,
		"msIntoSlot":       msIntoSlot,
	})

	if len(proposerPubkeyHex) != 98 {
		api.RespondError(w, http.StatusBadRequest, common.ErrInvalidPubkey.Error())
		return
	}

	if len(parentHashHex) != 66 {
		api.RespondError(w, http.StatusBadRequest, common.ErrInvalidHash.Error())
		return
	}

	if slot < headSlot {
		api.RespondError(w, http.StatusBadRequest, "slot is too old")
		return
	}

	// TODO: Use NegotiateRequestResponseType, for now we only accept JSON
	if !RequestAcceptsJSON(req) {
		api.RespondError(w, http.StatusNotAcceptable, "only Accept: application/json is currently supported")
		return
	}

	log.Debug("getHeader request received")
	defer func() {
		metrics.GetHeaderLatencyHistogram.Record(
			req.Context(),
			float64(time.Since(requestTime).Milliseconds()),
		)
	}()

	if slices.Contains(apiNoHeaderUserAgents, ua) {
		log.Info("rejecting getHeader by user agent")
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if api.ffForceGetHeader204 {
		log.Info("forced getHeader 204 response")
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Only allow requests for the current slot until a certain cutoff time
	if getHeaderRequestCutoffMs > 0 && msIntoSlot > 0 && msIntoSlot > int64(getHeaderRequestCutoffMs) {
		log.Info("getHeader sent too late")
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Only allow requests for the current slot after exchange finished trading
	if msIntoSlot < int64(getExchangeFinalizedCutoffMs) {
		log.Info("getHeader sent too early, wait for exchange finalized")
		w.WriteHeader(http.StatusNoContent)
		return
	}

	builderResp, err := FetchBuilderPubKey(exchangeAPIURL, slot)
	if err != nil {
		log.WithError(err).Error("failed to get builder id from API")
		builderResp = &BuilderResponse{
			Builder:         defaultBuilder,
			FallbackBuilder: defaultBuilder,
		}
	}
	log.Info("builder id", builderResp.Builder)
	// bid, err := api.redis.GetBestBid(slot, parentHashHex, proposerPubkeyHex)
	bid, err := api.redis.GetBuilderLatestBid(slot, parentHashHex, proposerPubkeyHex, builderResp.Builder)

	if err != nil {
		log.WithError(err).Error("could not get bid")
		api.RespondError(w, http.StatusBadRequest, err.Error())
		return
	}

	isBuilderValidPreconf, err := api.redis.GetIsValidPreconf(slot, parentHashHex, proposerPubkeyHex, builderResp.Builder)
	log.Println("get header | is builder valid preconf: ", isBuilderValidPreconf)
	if err != nil {
		log.WithError(err).Error("[builder] could not GetIsValidPreconf")
		api.RespondError(w, http.StatusBadRequest, err.Error())
		return
	}
	// 1. use builder bid when isBuilderValidPreconf is true
	if !isBuilderValidPreconf {
		isFallbackBuilderValidPreconf, err := api.redis.GetIsValidPreconf(slot, parentHashHex, proposerPubkeyHex, builderResp.FallbackBuilder)
		log.Println("get header | is fallback builder valid preconf: ", isFallbackBuilderValidPreconf)
		if err != nil {
			log.WithError(err).Error("[fallback builder] could not GetIsValidPreconf")
			api.RespondError(w, http.StatusBadRequest, err.Error())
			return
		}

		fallbackBid, err := api.redis.GetBuilderLatestBid(slot, parentHashHex, proposerPubkeyHex, builderResp.FallbackBuilder)
		if err != nil {
			log.WithError(err).Error("could not get fallback bid")
			api.RespondError(w, http.StatusBadRequest, err.Error())
			return
		}
		if fallbackBid == nil || fallbackBid.IsEmpty() {
			log.Info("no fallback bid")
			w.WriteHeader(http.StatusNoContent)
			return
		}

		if isFallbackBuilderValidPreconf {
			//2. use fallback bid with valid = true
			bid = fallbackBid
		} else {
			//both fallback bid and builder bid valid = false
			//3. use bid if builder bid is not empty // no change
			//4. use fallbackBid if bid is empty
			if bid == nil || bid.IsEmpty() {
				bid = fallbackBid
			}
		}
	}

	if bid == nil || bid.IsEmpty() {
		//5. if still empty then use best bid (which not builder nor fallback builder)
		bid, err = api.redis.GetBestBid(slot, parentHashHex, proposerPubkeyHex)
		if err != nil {
			log.WithError(err).Error("could not get best bid")
			api.RespondError(w, http.StatusBadRequest, err.Error())
			return
		}
		//6. if there have no any bid then return
		if bid == nil || bid.IsEmpty() {
			log.Info("no targeted bid, remain time: ", msIntoSlot)
			w.WriteHeader(http.StatusNoContent)
			return
		}
	}

	// HARDCODE to modify the bid value to force validator select our block
	if bid.Capella != nil {
		// log.Info("set fake bid.Capella.Message.Value")
		// log.Info("old bid.Capella.Message.Value: ", bid.Capella.Message.Value)
		bid.Capella.Message.Value = uint256.MustFromDecimal("11000000000000000000000") // Set to desired value (11000 ETH)
		// Serialize the bid data
		// log.Info("new bid.Capella.Message.Value: ", bid.Capella.Message.Value)
		// log.Info("old bid.Capella.Message.Pubkey: ", bid.Capella.Message.Pubkey)

		bid.Capella.Message.Pubkey = *api.publicKey
		// log.Info("new bid.Capella.Message.Pubkey: ", bid.Capella.Message.Pubkey)

		builderBid := builderApiCapella.BuilderBid{
			Value:  bid.Capella.Message.Value,
			Header: bid.Capella.Message.Header,
			Pubkey: *api.publicKey,
		}

		// print("RESIGN")
		// log.Info(&builderBid)
		// log.Info(&api.opts.EthNetDetails.DomainBuilder)
		// log.Info(api.blsSk)
		signature, err := ssz.SignMessage(&builderBid, api.opts.EthNetDetails.DomainBuilder, api.blsSk)

		if err != nil {
			log.WithError(err).Error("failed to signature bid")
			api.RespondError(w, http.StatusInternalServerError, "failed to signature bid")
			return
		}
		// Re-sign the payload with the new bid value
		signatureBytes := signature[:]

		// Ensure the signature is the correct size
		if len(signatureBytes) != 96 {
			log.Error("signature size is incorrect")
			api.RespondError(w, http.StatusInternalServerError, "signature size is incorrect")
			return
		}
		// log.Info("old bid.Capella.Signature: ", bid.Capella.Signature)

		// Assign the signature
		copy(bid.Capella.Signature[:], signatureBytes)
		// log.Info("new bid.Capella.Signature: ", bid.Capella.Signature)

	} else if bid.Deneb != nil {
		// log.Info("set fake bid.Deneb.Message.Value")
		bid.Deneb.Message.Value = uint256.NewInt(11000000000000000000) // Set to desired value (100 ETH)
		bid.Deneb.Message.Pubkey = *api.publicKey

		// Serialize the bid data

		builderBid := builderApiDeneb.BuilderBid{
			Value:  bid.Deneb.Message.Value,
			Header: bid.Deneb.Message.Header,
			Pubkey: *api.publicKey,
		}

		signature, err := ssz.SignMessage(&builderBid, api.opts.EthNetDetails.DomainBuilder, api.blsSk)
		if err != nil {
			log.WithError(err).Error("failed to signature bid")
			api.RespondError(w, http.StatusInternalServerError, "failed to signature bid")
			return
		}
		signatureBytes := signature[:]

		// Ensure the signature is the correct size
		if len(signatureBytes) != 96 {
			log.Error("signature size is incorrect")
			api.RespondError(w, http.StatusInternalServerError, "signature size is incorrect")
			return
		}

		// Assign the signature
		copy(bid.Deneb.Signature[:], signatureBytes)
	}

	value, err := bid.Value()
	if err != nil {
		log.WithError(err).Info("could not get bid value")
		api.RespondError(w, http.StatusBadRequest, err.Error())
	}
	blockHash, err := bid.BlockHash()
	if err != nil {
		log.WithError(err).Info("could not get bid block hash")
		api.RespondError(w, http.StatusBadRequest, err.Error())
	}

	// preconf txs should have 0 bid value if there have no public txs
	// // Error on bid without value
	// if value.Cmp(uint256.NewInt(0)) == 0 {
	// 	w.WriteHeader(http.StatusNoContent)
	// 	return
	// }
	decodeTime := time.Now().UTC()

	go func() {
		err := api.db.InsertGetPayload(uint64(slot), proposerPubkeyHex, blockHash.String(), slotStartTimestamp, uint64(requestTime.UnixMilli()), uint64(decodeTime.UnixMilli()), uint64(msIntoSlot))
		if err != nil {
			log.WithError(err).Error("failed to insert get header into db")
		}
	}()

	log.WithFields(logrus.Fields{
		"value":     value.String(),
		"blockHash": blockHash.String(),
	}).Info("bid delivered")

	api.RespondOK(w, bid)
}

func (api *RelayAPI) checkProposerSignature(block *common.VersionedSignedBlindedBeaconBlock, pubKey []byte) (bool, error) {
	switch block.Version { //nolint:exhaustive
	case spec.DataVersionCapella:
		return verifyBlockSignature(block, api.opts.EthNetDetails.DomainBeaconProposerCapella, pubKey)
	case spec.DataVersionDeneb:
		return verifyBlockSignature(block, api.opts.EthNetDetails.DomainBeaconProposerDeneb, pubKey)
	case spec.DataVersionElectra:
		return verifyBlockSignature(block, api.opts.EthNetDetails.DomainBeaconProposerElectra, pubKey)
	default:
		return false, errors.New("unsupported consensus data version")
	}
}

func (api *RelayAPI) handleGetPayload(w http.ResponseWriter, req *http.Request) {
	api.getPayloadCallsInFlight.Add(1)
	defer api.getPayloadCallsInFlight.Done()

	ua := req.UserAgent()
	headSlot := api.headSlot.Load()
	receivedAt := time.Now().UTC()
	log := api.log.WithFields(logrus.Fields{
		"method":                "getPayload",
		"ua":                    ua,
		"mevBoostV":             common.GetMevBoostVersionFromUserAgent(ua),
		"contentLength":         req.ContentLength,
		"headSlot":              headSlot,
		"headSlotEpochPos":      (headSlot % common.SlotsPerEpoch) + 1,
		"idArg":                 req.URL.Query().Get("id"),
		"timestampRequestStart": receivedAt.UnixMilli(),
	})

	// Log at start and end of request
	log.Info("handleGetPayload request initiated")
	defer func() {
		log.WithFields(logrus.Fields{
			"timestampRequestFin": time.Now().UTC().UnixMilli(),
			"requestDurationMs":   time.Since(receivedAt).Milliseconds(),
		}).Info("request finished")

		metrics.GetPayloadLatencyHistogram.Record(
			req.Context(),
			float64(time.Since(receivedAt).Milliseconds()),
		)
	}()

	// Read the body first, so we can decode it later
	limitReader := io.LimitReader(req.Body, int64(apiMaxPayloadBytes))
	body, err := io.ReadAll(limitReader)
	if err != nil {
		if strings.Contains(err.Error(), "i/o timeout") {
			log.WithError(err).Error("getPayload request failed to decode (i/o timeout)")
			api.RespondError(w, http.StatusInternalServerError, err.Error())
			return
		}

		log.WithError(err).Error("could not read body of request from the beacon node")
		api.RespondError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Decode payload
	payload := new(common.VersionedSignedBlindedBeaconBlock)
	if err := json.NewDecoder(bytes.NewReader(body)).Decode(payload); err != nil {
		log.WithError(err).Warn("failed to decode getPayload request")
		api.RespondError(w, http.StatusBadRequest, "failed to decode payload")
		return
	}

	// Take time after the decoding, and add to logging
	decodeTime := time.Now().UTC()
	slot, err := payload.Slot()
	if err != nil {
		log.WithError(err).Warn("failed to get payload slot")
		api.RespondError(w, http.StatusBadRequest, "failed to get payload slot")
		return
	}
	blockHash, err := payload.ExecutionBlockHash()
	if err != nil {
		log.WithError(err).Warn("failed to get payload block hash")
		api.RespondError(w, http.StatusBadRequest, "failed to get payload block hash")
		return
	}
	proposerIndex, err := payload.ProposerIndex()
	if err != nil {
		log.WithError(err).Warn("failed to get payload proposer index")
		api.RespondError(w, http.StatusBadRequest, "failed to get payload proposer index")
		return
	}
	slotStartTimestamp := api.genesisInfo.Data.GenesisTime + (uint64(slot) * common.SecondsPerSlot)
	msIntoSlot := decodeTime.UnixMilli() - int64(slotStartTimestamp*1000) //nolint:gosec
	log = log.WithFields(logrus.Fields{
		"slot":                 slot,
		"slotEpochPos":         (uint64(slot) % common.SlotsPerEpoch) + 1,
		"blockHash":            blockHash.String(),
		"slotStartSec":         slotStartTimestamp,
		"msIntoSlot":           msIntoSlot,
		"timestampAfterDecode": decodeTime.UnixMilli(),
		"proposerIndex":        proposerIndex,
	})

	// Ensure the proposer index is expected
	api.proposerDutiesLock.RLock()
	slotDuty := api.proposerDutiesMap[uint64(slot)]
	api.proposerDutiesLock.RUnlock()
	if slotDuty == nil {
		log.Warn("could not find slot duty")
	} else {
		log = log.WithField("feeRecipient", slotDuty.Entry.Message.FeeRecipient.String())
		if slotDuty.ValidatorIndex != uint64(proposerIndex) {
			log.WithField("expectedProposerIndex", slotDuty.ValidatorIndex).Warn("not the expected proposer index")
			api.RespondError(w, http.StatusBadRequest, "not the expected proposer index")
			return
		}
	}

	// Get the proposer pubkey based on the validator index from the payload
	proposerPubkey, found := api.datastore.GetKnownValidatorPubkeyByIndex(uint64(proposerIndex))
	if !found {
		log.Errorf("could not find proposer pubkey for index %d", proposerIndex)
		api.RespondError(w, http.StatusBadRequest, "could not match proposer index to pubkey")
		return
	}

	// Add proposer pubkey to logs
	log = log.WithField("proposerPubkey", proposerPubkey.String())

	// Create a BLS pubkey from the hex pubkey
	pk, err := utils.HexToPubkey(proposerPubkey.String())
	if err != nil {
		log.WithError(err).Warn("could not convert pubkey to phase0.BLSPubKey")
		api.RespondError(w, http.StatusBadRequest, "could not convert pubkey to phase0.BLSPubKey")
		return
	}

	// Validate proposer signature
	ok, err := api.checkProposerSignature(payload, pk[:])
	if !ok || err != nil {
		if api.ffLogInvalidSignaturePayload {
			txt, _ := json.Marshal(payload) //nolint:errchkjson
			log.Info("payload_invalid_sig: ", string(txt), "pubkey:", proposerPubkey.String())
		}
		log.WithError(err).Warn("could not verify payload signature")
		api.RespondError(w, http.StatusBadRequest, "could not verify payload signature")
		return
	}

	// Log about received payload (with a valid proposer signature)
	log = log.WithField("timestampAfterSignatureVerify", time.Now().UTC().UnixMilli())
	log.Info("getPayload request received")

	var getPayloadResp *builderApi.VersionedSubmitBlindedBlockResponse
	var msNeededForPublishing uint64

	// Save information about delivered payload
	defer func() {
		bidTrace, err := api.redis.GetBidTrace(uint64(slot), proposerPubkey.String(), blockHash.String())
		if err != nil {
			log.WithError(err).Info("failed to get bidTrace for delivered payload from redis")
			return
		}

		err = api.db.SaveDeliveredPayload(bidTrace, payload, decodeTime, msNeededForPublishing)
		if err != nil {
			log.WithError(err).WithFields(logrus.Fields{
				"bidTrace": bidTrace,
				"payload":  payload,
			}).Error("failed to save delivered payload")
		}

		// Increment builder stats
		err = api.db.IncBlockBuilderStatsAfterGetPayload(bidTrace.BuilderPubkey.String())
		if err != nil {
			log.WithError(err).Error("failed to increment builder-stats after getPayload")
		}

		// Wait until optimistic blocks are complete.
		api.optimisticBlocksWG.Wait()

		// Check if there is a demotion for the winning block.
		_, err = api.db.GetBuilderDemotion(bidTrace)
		// If demotion not found, we are done!
		if errors.Is(err, sql.ErrNoRows) {
			log.Info("no demotion in getPayload, successful block proposal")
			return
		}
		if err != nil {
			log.WithError(err).Error("failed to read demotion table in getPayload")
			return
		}
		// Demotion found, update the demotion table with refund data.
		builderPubkey := bidTrace.BuilderPubkey.String()
		log = log.WithFields(logrus.Fields{
			"builderPubkey": builderPubkey,
			"slot":          bidTrace.Slot,
			"blockHash":     bidTrace.BlockHash,
		})
		log.Warn("demotion found in getPayload, inserting refund justification")

		// Prepare refund data.
		signedBeaconBlock, err := common.SignedBlindedBeaconBlockToBeaconBlock(payload, getPayloadResp)
		if err != nil {
			log.WithError(err).Error("failed to convert signed blinded beacon block to beacon block")
			api.RespondError(w, http.StatusInternalServerError, "failed to convert signed blinded beacon block to beacon block")
			return
		}

		// Get registration entry from the DB.
		registrationEntry, err := api.db.GetValidatorRegistration(proposerPubkey.String())
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				log.WithError(err).Error("no registration found for validator " + proposerPubkey.String())
			} else {
				log.WithError(err).Error("error reading validator registration")
			}
		}
		var signedRegistration *builderApiV1.SignedValidatorRegistration
		if registrationEntry != nil {
			signedRegistration, err = registrationEntry.ToSignedValidatorRegistration()
			if err != nil {
				log.WithError(err).Error("error converting registration to signed registration")
			}
		}

		err = api.db.UpdateBuilderDemotion(bidTrace, signedBeaconBlock, signedRegistration)
		if err != nil {
			log.WithFields(logrus.Fields{
				"errorWritingRefundToDB": true,
				"bidTrace":               bidTrace,
				"signedBeaconBlock":      signedBeaconBlock,
				"signedRegistration":     signedRegistration,
			}).WithError(err).Error("unable to update builder demotion with refund justification")
		}
	}()

	// Get the response - from Redis, Memcache or DB
	// note that recent mev-boost versions only send getPayload to relays that provided the bid
	getPayloadResp, err = api.datastore.GetGetPayloadResponse(log, uint64(slot), proposerPubkey.String(), blockHash.String())
	if err != nil || getPayloadResp == nil {
		log.WithError(err).Warn("failed getting execution payload (1/2)")
		time.Sleep(time.Duration(timeoutGetPayloadRetryMs) * time.Millisecond)

		// Try again
		getPayloadResp, err = api.datastore.GetGetPayloadResponse(log, uint64(slot), proposerPubkey.String(), blockHash.String())
		if err != nil || getPayloadResp == nil {
			// Still not found! Error out now.
			if errors.Is(err, datastore.ErrExecutionPayloadNotFound) {
				// Couldn't find the execution payload, maybe it never was submitted to our relay! Check that now
				bid, err := api.db.GetBlockSubmissionEntry(uint64(slot), proposerPubkey.String(), blockHash.String())
				if errors.Is(err, sql.ErrNoRows) {
					log.Warn("failed getting execution payload (2/2) - payload not found, block was never submitted to this relay")
					api.RespondError(w, http.StatusBadRequest, "no execution payload for this request - block was never seen by this relay")
				} else if err != nil {
					log.WithError(err).Error("failed getting execution payload (2/2) - payload not found, and error on checking bids")
				} else if bid.EligibleAt.Valid {
					log.Error("failed getting execution payload (2/2) - payload not found, but found bid in database")
				} else {
					log.Info("found bid but payload was never saved as bid was ineligible being below floor value")
				}
			} else { // some other error
				log.WithError(err).Error("failed getting execution payload (2/2) - error")
			}
			api.RespondError(w, http.StatusBadRequest, "no execution payload for this request")
			return
		}
	}

	// Now we know this relay also has the payload
	log = log.WithField("timestampAfterLoadResponse", time.Now().UTC().UnixMilli())

	// Check whether getPayload has already been called -- TODO: do we need to allow multiple submissions of one blinded block?
	err = api.redis.CheckAndSetLastSlotAndHashDelivered(uint64(slot), blockHash.String())
	log = log.WithField("timestampAfterAlreadyDeliveredCheck", time.Now().UTC().UnixMilli())
	if err != nil {
		if errors.Is(err, datastore.ErrAnotherPayloadAlreadyDeliveredForSlot) {
			// BAD VALIDATOR, 2x GETPAYLOAD FOR DIFFERENT PAYLOADS
			log.Warn("validator called getPayload twice for different payload hashes")
			api.RespondError(w, http.StatusBadRequest, "another payload for this slot was already delivered")
			return
		} else if errors.Is(err, datastore.ErrPastSlotAlreadyDelivered) {
			// BAD VALIDATOR, 2x GETPAYLOAD FOR PAST SLOT
			log.Warn("validator called getPayload for past slot")
			api.RespondError(w, http.StatusBadRequest, "payload for this slot was already delivered")
			return
		} else if errors.Is(err, redis.TxFailedErr) {
			// BAD VALIDATOR, 2x GETPAYLOAD + RACE
			log.Warn("validator called getPayload twice (race)")
			api.RespondError(w, http.StatusBadRequest, "payload for this slot was already delivered (race)")
			return
		}
		log.WithError(err).Error("redis.CheckAndSetLastSlotAndHashDelivered failed")
	}

	go func() {
		err := api.db.InsertGetPayload(uint64(slot), proposerPubkey.String(), blockHash.String(), slotStartTimestamp, uint64(receivedAt.UnixMilli()), uint64(decodeTime.UnixMilli()), uint64(msIntoSlot))
		if err != nil {
			log.WithError(err).Error("failed to insert payload too late into db")
		}
	}()

	// Handle early/late requests
	if msIntoSlot < 0 {
		// Wait until slot start (t=0) if still in the future
		_msSinceSlotStart := time.Now().UTC().UnixMilli() - int64(slotStartTimestamp*1000) //nolint:gosec
		if _msSinceSlotStart < 0 {
			delayMillis := _msSinceSlotStart * -1
			log = log.WithField("delayMillis", delayMillis)
			log.Info("waiting until slot start t=0")
			time.Sleep(time.Duration(delayMillis) * time.Millisecond)
		}
	} else if getPayloadRequestCutoffMs > 0 && msIntoSlot > int64(getPayloadRequestCutoffMs) {
		// Reject requests after cutoff time
		log.Warn("getPayload sent too late")
		api.RespondError(w, http.StatusBadRequest, fmt.Sprintf("sent too late - %d ms into slot", msIntoSlot))

		go func() {
			err := api.db.InsertTooLateGetPayload(uint64(slot), proposerPubkey.String(), blockHash.String(), slotStartTimestamp, uint64(receivedAt.UnixMilli()), uint64(decodeTime.UnixMilli()), uint64(msIntoSlot)) //nolint:gosec
			if err != nil {
				log.WithError(err).Error("failed to insert payload too late into db")
			}
		}()
		return
	}

	// Check that BlindedBlockContent fields (sent by the proposer) match our known BlockContents
	err = EqBlindedBlockContentsToBlockContents(payload, getPayloadResp)
	if err != nil {
		log.WithError(err).Warn("ExecutionPayloadHeader not matching known ExecutionPayload")
		api.RespondError(w, http.StatusBadRequest, "invalid execution payload header")
		return
	}

	// Publish the signed beacon block via beacon-node
	timeBeforePublish := time.Now().UTC().UnixMilli()
	log = log.WithField("timestampBeforePublishing", timeBeforePublish)
	signedBeaconBlock, err := common.SignedBlindedBeaconBlockToBeaconBlock(payload, getPayloadResp)
	if err != nil {
		log.WithError(err).Error("failed to convert signed blinded beacon block to beacon block")
		api.RespondError(w, http.StatusInternalServerError, "failed to convert signed blinded beacon block to beacon block")
		return
	}
	code, err := api.beaconClient.PublishBlock(signedBeaconBlock) // errors are logged inside
	if err != nil || (code != http.StatusOK && code != http.StatusAccepted) {
		log.WithError(err).WithField("code", code).Error("failed to publish block")
		api.RespondError(w, http.StatusBadRequest, "failed to publish block")
		return
	}

	timeAfterPublish := time.Now().UTC().UnixMilli()
	msNeededForPublishing = uint64(timeAfterPublish - timeBeforePublish) //nolint:gosec
	log = log.WithField("timestampAfterPublishing", timeAfterPublish)
	log.WithField("msNeededForPublishing", msNeededForPublishing).Info("block published through beacon node")
	metrics.PublishBlockLatencyHistogram.Record(req.Context(), float64(msNeededForPublishing))

	// give the beacon network some time to propagate the block
	time.Sleep(time.Duration(getPayloadResponseDelayMs) * time.Millisecond)

	// respond to the HTTP request
	api.RespondOK(w, getPayloadResp)
	blockNumber, err := payload.ExecutionBlockNumber()
	if err != nil {
		log.WithError(err).Info("failed to get block number")
	}
	txs, err := getPayloadResp.Transactions()
	if err != nil {
		log.WithError(err).Info("failed to get transactions")
	}
	log = log.WithFields(logrus.Fields{
		"numTx":       len(txs),
		"blockNumber": blockNumber,
	})
	if getPayloadResp.Version >= spec.DataVersionDeneb {
		blobs, err := getPayloadResp.Blobs()
		if err != nil {
			log.WithError(err).Info("failed to get blobs")
		}
		blobGasUsed, err := getPayloadResp.BlobGasUsed()
		if err != nil {
			log.WithError(err).Info("failed to get blobGasUsed")
		}
		excessBlobGas, err := getPayloadResp.ExcessBlobGas()
		if err != nil {
			log.WithError(err).Info("failed to get excessBlobGas")
		}
		log = log.WithFields(logrus.Fields{
			"numBlobs":      len(blobs),
			"blobGasUsed":   blobGasUsed,
			"excessBlobGas": excessBlobGas,
		})
	}
	log.Info("execution payload delivered")
}

// --------------------
//
//	BLOCK BUILDER APIS
//
// --------------------
func (api *RelayAPI) handleBuilderGetValidators(w http.ResponseWriter, req *http.Request) {
	api.proposerDutiesLock.RLock()
	resp := api.proposerDutiesResponse
	api.proposerDutiesLock.RUnlock()
	_, err := w.Write(*resp)
	if err != nil {
		api.log.WithError(err).Warn("failed to write response for builderGetValidators")
	}
}

func (api *RelayAPI) checkSubmissionFeeRecipient(w http.ResponseWriter, log *logrus.Entry, bidTrace *builderApiV1.BidTrace, feeRecipient string, builderPubkey string) (uint64, bool) {
	api.proposerDutiesLock.RLock()
	slotDuty := api.proposerDutiesMap[bidTrace.Slot]
	api.proposerDutiesLock.RUnlock()
	expectedFeeRecipient := ""
	if slotDuty != nil {
		expectedFeeRecipient = slotDuty.Entry.Message.FeeRecipient.String()
	}
	if feeRecipient != "" {
		expectedFeeRecipient = feeRecipient
	}
	if slotDuty == nil {
		log.Warn("could not find slot duty")
		api.RespondError(w, http.StatusBadRequest, "could not find slot duty")
		return 0, false
	} else if strings.EqualFold(builderPubkey, defaultBuilder) && !strings.EqualFold(expectedFeeRecipient, bidTrace.ProposerFeeRecipient.String()) {
		//only fallback builder need to check fee recipient
		log.WithFields(logrus.Fields{
			"expectedFeeRecipient": expectedFeeRecipient,
			"actualFeeRecipient":   bidTrace.ProposerFeeRecipient.String(),
		}).Info("fee recipient does not match")
		api.RespondError(w, http.StatusBadRequest, "fee recipient does not match")
		return 0, false
	}
	return slotDuty.Entry.Message.GasLimit, true
}

func (api *RelayAPI) checkSubmissionPayloadAttrs(w http.ResponseWriter, log *logrus.Entry, submission *common.BlockSubmissionInfo) (payloadAttributesHelper, bool) {
	api.payloadAttributesLock.RLock()
	attrs, ok := api.payloadAttributes[getPayloadAttributesKey(submission.BidTrace.ParentHash.String(), submission.BidTrace.Slot)]
	api.payloadAttributesLock.RUnlock()
	if !ok || submission.BidTrace.Slot != attrs.slot {
		log.WithFields(logrus.Fields{
			"attributesFound": ok,
			"payloadSlot":     submission.BidTrace.Slot,
			"attrsSlot":       attrs.slot,
		}).Warn("payload attributes not (yet) known")
		api.RespondError(w, http.StatusBadRequest, "payload attributes not (yet) known")
		return attrs, false
	}

	if submission.PrevRandao.String() != attrs.payloadAttributes.PrevRandao {
		msg := fmt.Sprintf("incorrect prev_randao - got: %s, expected: %s", submission.PrevRandao.String(), attrs.payloadAttributes.PrevRandao)
		log.Info(msg)
		api.RespondError(w, http.StatusBadRequest, msg)
		return attrs, false
	}

	if hasReachedFork(submission.BidTrace.Slot, api.capellaEpoch) {
		withdrawalsRoot, err := ComputeWithdrawalsRoot(submission.Withdrawals)
		if err != nil {
			log.WithError(err).Warn("could not compute withdrawals root from payload")
			api.RespondError(w, http.StatusBadRequest, "could not compute withdrawals root")
			return attrs, false
		}
		if withdrawalsRoot != attrs.withdrawalsRoot {
			msg := fmt.Sprintf("incorrect withdrawals root - got: %s, expected: %s", withdrawalsRoot.String(), attrs.withdrawalsRoot.String())
			log.Info(msg)
			api.RespondError(w, http.StatusBadRequest, msg)
			return attrs, false
		}
	}

	return attrs, true
}

func (api *RelayAPI) checkSubmissionSlotDetails(w http.ResponseWriter, log *logrus.Entry, headSlot uint64, payload *common.VersionedSubmitBlockRequest, submission *common.BlockSubmissionInfo) bool {
	if api.isElectra(submission.BidTrace.Slot) && payload.Electra == nil {
		log.Info("rejecting submission - non electra payload for electra fork")
		api.RespondError(w, http.StatusBadRequest, "not electra payload")
		return false
	}
	if api.isDeneb(submission.BidTrace.Slot) && payload.Deneb == nil {
		log.Info("rejecting submission - non deneb payload for deneb fork")
		api.RespondError(w, http.StatusBadRequest, "not deneb payload")
		return false
	}
	if api.isCapella(submission.BidTrace.Slot) && payload.Capella == nil {
		log.Info("rejecting submission - non capella payload for capella fork")
		api.RespondError(w, http.StatusBadRequest, "not capella payload")
		return false
	}

	if submission.BidTrace.Slot <= headSlot {
		log.Info("submitNewBlock failed: submission for past slot")
		api.RespondError(w, http.StatusBadRequest, "submission for past slot")
		return false
	}

	// Timestamp check
	expectedTimestamp := api.genesisInfo.Data.GenesisTime + (submission.BidTrace.Slot * common.SecondsPerSlot)
	if submission.Timestamp != expectedTimestamp {
		log.Warnf("incorrect timestamp. got %d, expected %d", submission.Timestamp, expectedTimestamp)
		api.RespondError(w, http.StatusBadRequest, fmt.Sprintf("incorrect timestamp. got %d, expected %d", submission.Timestamp, expectedTimestamp))
		return false
	}

	return true
}

func (api *RelayAPI) checkBuilderEntry(w http.ResponseWriter, log *logrus.Entry, builderPubkey phase0.BLSPubKey) (*blockBuilderCacheEntry, bool) {
	builderEntry, ok := api.blockBuildersCache[builderPubkey.String()]
	if !ok {
		log.Infof("unable to read builder: %s from the builder cache, using low-prio and no collateral", builderPubkey.String())
		builderEntry = &blockBuilderCacheEntry{
			status: common.BuilderStatus{
				IsHighPrio:    false,
				IsOptimistic:  false,
				IsBlacklisted: false,
			},
			collateral: big.NewInt(0),
		}
	}

	if builderEntry.status.IsBlacklisted {
		log.Info("builder is blacklisted")
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		return builderEntry, false
	}

	// In case only high-prio requests are accepted, fail others
	if api.ffDisableLowPrioBuilders && !builderEntry.status.IsHighPrio {
		log.Info("rejecting low-prio builder (ff-disable-low-prio-builders)")
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		return builderEntry, false
	}

	return builderEntry, true
}

type bidFloorOpts struct {
	w                    http.ResponseWriter
	tx                   redis.Pipeliner
	log                  *logrus.Entry
	cancellationsEnabled bool
	simResultC           chan *blockSimResult
	submission           *common.BlockSubmissionInfo
}

func (api *RelayAPI) checkFloorBidValue(opts bidFloorOpts) (*big.Int, bool) {
	// Reject new submissions once the payload for this slot was delivered - TODO: store in memory as well
	slotLastPayloadDelivered, err := api.redis.GetLastSlotDelivered(context.Background(), opts.tx)
	if err != nil && !errors.Is(err, redis.Nil) {
		opts.log.WithError(err).Error("failed to get delivered payload slot from redis")
	} else if opts.submission.BidTrace.Slot <= slotLastPayloadDelivered {
		opts.log.Info("rejecting submission because payload for this slot was already delivered")
		api.RespondError(opts.w, http.StatusBadRequest, "payload for this slot was already delivered")
		return nil, false
	}

	// Grab floor bid value
	floorBidValue, err := api.redis.GetFloorBidValue(context.Background(), opts.tx, opts.submission.BidTrace.Slot, opts.submission.BidTrace.ParentHash.String(), opts.submission.BidTrace.ProposerPubkey.String())
	if err != nil {
		opts.log.WithError(err).Error("failed to get floor bid value from redis")
	} else {
		opts.log = opts.log.WithField("floorBidValue", floorBidValue.String())
	}

	// --------------------------------------------
	// Skip submission if below the floor bid value
	// --------------------------------------------
	isBidBelowFloor := floorBidValue != nil && opts.submission.BidTrace.Value.ToBig().Cmp(floorBidValue) == -1
	isBidAtOrBelowFloor := floorBidValue != nil && opts.submission.BidTrace.Value.ToBig().Cmp(floorBidValue) < 1
	if opts.cancellationsEnabled && isBidBelowFloor { // with cancellations: if below floor -> delete previous bid
		opts.simResultC <- &blockSimResult{false, nil, false, nil, nil}
		opts.log.Info("submission below floor bid value, with cancellation")
		err := api.redis.DelBuilderBid(context.Background(), opts.tx, opts.submission.BidTrace.Slot, opts.submission.BidTrace.ParentHash.String(), opts.submission.BidTrace.ProposerPubkey.String(), opts.submission.BidTrace.BuilderPubkey.String())
		if err != nil {
			opts.log.WithError(err).Error("failed processing cancellable bid below floor")
			api.RespondError(opts.w, http.StatusInternalServerError, "failed processing cancellable bid below floor")
			return nil, false
		}
		api.Respond(opts.w, http.StatusAccepted, "accepted bid below floor, skipped validation")
		return nil, false
	} else if !opts.cancellationsEnabled && isBidAtOrBelowFloor { // without cancellations: if at or below floor -> ignore
		opts.simResultC <- &blockSimResult{false, nil, false, nil, nil}
		opts.log.Info("submission at or below floor bid value, without cancellation")
		api.RespondMsg(opts.w, http.StatusAccepted, "accepted bid below floor, skipped validation")
		return nil, false
	}
	return floorBidValue, true
}

type redisUpdateBidOpts struct {
	w                    http.ResponseWriter
	tx                   redis.Pipeliner
	log                  *logrus.Entry
	cancellationsEnabled bool
	receivedAt           time.Time
	floorBidValue        *big.Int
	payload              *common.VersionedSubmitBlockRequest
	isValidPreconf       bool
}

func (api *RelayAPI) updateRedisBid(opts redisUpdateBidOpts) (*datastore.SaveBidAndUpdateTopBidResponse, *builderApi.VersionedSubmitBlindedBlockResponse, bool) {
	// Prepare the response data
	getHeaderResponse, err := common.BuildGetHeaderResponse(opts.payload, api.blsSk, api.publicKey, api.opts.EthNetDetails.DomainBuilder)
	if err != nil {
		opts.log.WithError(err).Error("could not sign builder bid")
		api.RespondError(opts.w, http.StatusBadRequest, err.Error())
		return nil, nil, false
	}

	getPayloadResponse, err := common.BuildGetPayloadResponse(opts.payload)
	if err != nil {
		opts.log.WithError(err).Error("could not build getPayload response")
		api.RespondError(opts.w, http.StatusBadRequest, err.Error())
		return nil, nil, false
	}

	submission, err := common.GetBlockSubmissionInfo(opts.payload)
	if err != nil {
		opts.log.WithError(err).Error("could not get block submission info")
		api.RespondError(opts.w, http.StatusBadRequest, err.Error())
		return nil, nil, false
	}

	bidTrace := common.BidTraceV2WithBlobFields{
		BidTrace:      *submission.BidTrace,
		BlockNumber:   submission.BlockNumber,
		NumTx:         uint64(len(submission.Transactions)),
		NumBlobs:      uint64(len(submission.Blobs)),
		BlobGasUsed:   submission.BlobGasUsed,
		ExcessBlobGas: submission.ExcessBlobGas,
	}

	//
	// Save to Redis
	//
	updateBidResult, err := api.redis.SaveBidAndUpdateTopBid(context.Background(), opts.tx, &bidTrace, opts.payload, getPayloadResponse, getHeaderResponse, opts.receivedAt, opts.cancellationsEnabled, opts.floorBidValue, opts.isValidPreconf)
	if err != nil {
		opts.log.WithError(err).Error("could not save bid and update top bids")
		api.RespondError(opts.w, http.StatusInternalServerError, "failed saving and updating bid")
		return nil, nil, false
	}
	return &updateBidResult, getPayloadResponse, true
}

func (api *RelayAPI) handleSubmitNewBlock(w http.ResponseWriter, req *http.Request) {
	var pf common.Profile
	var prevTime, nextTime time.Time

	headSlot := api.headSlot.Load()
	receivedAt := time.Now().UTC()
	prevTime = receivedAt

	//always true to allow replace old preconf order
	// args := req.URL.Query()
	// isCancellationEnabled := args.Get("cancellations") == "1"
	isCancellationEnabled := true
	log := api.log.WithFields(logrus.Fields{
		"method":                "submitNewBlock",
		"contentLength":         req.ContentLength,
		"headSlot":              headSlot,
		"cancellationEnabled":   isCancellationEnabled,
		"timestampRequestStart": receivedAt.UnixMilli(),
	})

	// Log at start and end of request
	log.Info("request initiated")
	defer func() {
		log.WithFields(logrus.Fields{
			"timestampRequestFin": time.Now().UTC().UnixMilli(),
			"requestDurationMs":   time.Since(receivedAt).Milliseconds(),
		}).Info("request finished")

		// metrics
		api.saveBlockSubmissionMetrics(pf, receivedAt)
	}()

	// If cancellations are disabled but builder requested it, return error
	if isCancellationEnabled && !api.ffEnableCancellations {
		log.Info("builder submitted with cancellations enabled, but feature flag is disabled")
		api.RespondError(w, http.StatusBadRequest, "cancellations are disabled")
		return
	}

	var err error
	var r io.Reader = req.Body
	isGzip := req.Header.Get("Content-Encoding") == "gzip"
	pf.IsGzip = isGzip
	log = log.WithField("reqIsGzip", isGzip)
	if isGzip {
		r, err = gzip.NewReader(req.Body)
		if err != nil {
			log.WithError(err).Warn("could not create gzip reader")
			api.RespondError(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	limitReader := io.LimitReader(r, int64(apiMaxPayloadBytes))
	requestPayloadBytes, err := io.ReadAll(limitReader)
	if err != nil {
		log.WithError(err).Warn("could not read payload")
		api.RespondError(w, http.StatusBadRequest, err.Error())
		return
	}

	nextTime = time.Now().UTC()
	pf.PayloadLoad = uint64(nextTime.Sub(prevTime).Microseconds()) //nolint:gosec
	prevTime = nextTime

	payload := new(common.VersionedSubmitBlockRequest)

	// Check for SSZ encoding
	contentType := req.Header.Get("Content-Type")
	if contentType == "application/octet-stream" {
		log = log.WithField("reqContentType", "ssz")
		pf.ContentType = "ssz"
		if err = payload.UnmarshalSSZ(requestPayloadBytes); err != nil {
			log.WithError(err).Warn("could not decode payload - SSZ")

			// SSZ decoding failed. try JSON as fallback (some builders used octet-stream for json before)
			if err2 := json.Unmarshal(requestPayloadBytes, payload); err2 != nil {
				log.WithError(fmt.Errorf("%w / %w", err, err2)).Warn("could not decode payload - SSZ or JSON")
				api.RespondError(w, http.StatusBadRequest, err.Error())
				return
			}
			log = log.WithField("reqContentType", "json")
			pf.ContentType = "json"
		} else {
			log.Debug("received ssz-encoded payload")
		}
	} else {
		log = log.WithField("reqContentType", "json")
		pf.ContentType = "json"
		if err := json.Unmarshal(requestPayloadBytes, payload); err != nil {
			log.WithError(err).Warn("could not decode payload - JSON")
			api.RespondError(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	nextTime = time.Now().UTC()
	pf.Decode = uint64(nextTime.Sub(prevTime).Microseconds()) //nolint:gosec
	prevTime = nextTime

	isLargeRequest := len(requestPayloadBytes) > fastTrackPayloadSizeLimit
	// getting block submission info also validates bid trace and execution submission are not empty
	submission, err := common.GetBlockSubmissionInfo(payload)
	if err != nil {
		log.WithError(err).Warn("missing fields in submit block request")
		api.RespondError(w, http.StatusBadRequest, err.Error())
		return
	}
	slotStartTimestamp := api.genesisInfo.Data.GenesisTime + ((submission.BidTrace.Slot) * common.SecondsPerSlot)
	msIntoSlot := receivedAt.UnixMilli() - int64((slotStartTimestamp * 1000))
	api.log.Info("=====msIntoSlo")

	api.log.Info(msIntoSlot)
	//before deadline: no check, is valid = false
	//after deadline: check builder id, is fullfilled preconf, is valid = false/true
	//get header: 1. builder + true, 2. fallback builder + true, 3. builder + false 4. fallback builder + false 5. other builder
	isValidPreconf := "" // empty string means valid
	feeRecipient := ""
	if msIntoSlot < int64(getExchangeFinalizedCutoffMs) {
		api.log.Info("handleSubmitNewBlock sent too early, wait for exchange finalized")
		isValidPreconf = "submission too early, before exchange finalization cutoff"
	} else if msIntoSlot >= int64(getExchangeFinalizedCutoffMs) {
		// Cache management code
		preconfCacheMutex.Lock()
		if submission.BidTrace.Slot != currentSlot {
			preconfCache = make(map[uint64]PreconfBundles)
			currentSlot = submission.BidTrace.Slot
		}
		preconfCacheMutex.Unlock()

		// Check cache first
		preconfCacheMutex.RLock()
		cachedPreconfs, exists := preconfCache[submission.BidTrace.Slot]
		preconfCacheMutex.RUnlock()

		if !exists {
			// url := fmt.Sprintf("%s/api/slot/bundles?slot=%d", client.APIURL, submission.BidTrace.Slot)
			url := fmt.Sprintf("%s/api/v1/slot/bundles?slot=%d", client.APIURL, submission.BidTrace.Slot)
			log.Printf(url)
			req, err := http.NewRequest("GET", url, nil)
			if err != nil {
				log.Printf("cannot fetch preconf requests from preconf server, %v", err)
				return
			}
			header := fmt.Sprintf("Bearer %s", client.AccessToken)
			req.Header.Set("AUTHORIZATION", header)

			resp, err := client.Client.Do(req)
			if err != nil {
				log.Printf("cannot fetch preconf requests from preconf server, %v", err)
				return
			}
			defer resp.Body.Close()

			var apiResponse ApiResponse
			err = json.NewDecoder(resp.Body).Decode(&apiResponse)
			if err != nil {
				log.Printf("Failed to fetch preconf request: %v", err)
				return
			}

			if !apiResponse.Success {
				log.Printf("Failed to fetch inclusion preconf from server: %v", apiResponse)
				return
			}

			var preconfBundles PreconfBundles
			err = json.Unmarshal(apiResponse.Data, &preconfBundles)
			if err != nil {
				log.Printf("Failed to unmarshal preconf bundles: %v", err)
				return
			}

			// Store in cache
			preconfCacheMutex.Lock()
			preconfCache[submission.BidTrace.Slot] = preconfBundles
			preconfCacheMutex.Unlock()

			cachedPreconfs = preconfBundles
		}

		// Transaction checking logic
		// Convert all block transactions to lowercase for case-insensitive comparison
		blockTxMap := make(map[string]struct{})
		for _, tx := range submission.Transactions {
			txLower := "0x" + strings.ToLower(hex.EncodeToString(tx))
			blockTxMap[txLower] = struct{}{}
		}

		missingTxs := []string{}
		missingOrderBundle := []string{}
		count := 0
		feeRecipient = cachedPreconfs.feeRecipient

		for _, preconf := range cachedPreconfs.Bundles {
			// Skip transaction checking for MEV-type bundles
			// const bundleType = {
			// 	STANDARD: 1,
			// 	MEV: 2,
			// 	BLOBS: 3, //TODO
			// }
			if preconf.BundleType == 2 { //mev type can skip
				continue
			}

			// Check ordering
			numOfTxsInBundle := len(preconf.Txs)

			if preconf.Ordering == 1 {
				// "1" means the top
				// Check if the first `numOfTxsInBundle` transactions in the block match the preconf transactions
				for i, preconfTx := range preconf.Txs {
					if i >= len(submission.Transactions) || strings.ToLower(preconfTx.Tx) != "0x"+strings.ToLower(hex.EncodeToString(submission.Transactions[i])) {
						missingOrderBundle = append(missingOrderBundle, preconf.UUID)
						continue
					}
				}
			} else if preconf.Ordering == -1 {
				// "-1" means the bottom
				// Check if the last `numOfTxsInBundle` transactions in the block match the preconf transactions
				for i, preconfTx := range preconf.Txs {
					blockTxIndex := len(submission.Transactions) - numOfTxsInBundle + i
					if blockTxIndex < 0 || strings.ToLower(preconfTx.Tx) != "0x"+strings.ToLower(hex.EncodeToString(submission.Transactions[blockTxIndex])) {
						missingOrderBundle = append(missingOrderBundle, preconf.UUID)
						continue
					}
				}
			} else {
				// Existing transaction existence check
				for _, preconfTx := range preconf.Txs {
					txLower := strings.ToLower(preconfTx.Tx)
					if _, exists := blockTxMap[txLower]; !exists {
						missingTxs = append(missingTxs, preconfTx.Tx)
					}
				}
			}
		}
		if len(missingOrderBundle) > 0 {
			log.Printf("Total missing ordering bundle: %d",
				len(missingOrderBundle))
			log.Println("Missing preconf transaction hexes:", missingOrderBundle)
			isValidPreconf = fmt.Sprintf("missing ordering bundle: %s", missingOrderBundle)
		}

		if len(missingTxs) > 0 {
			log.Printf("Total missing transactions: %d, included preconf transaction: %d",
				len(missingTxs), count-len(missingTxs))
			log.Printf("Number of transactions: %d", len(submission.Transactions))
			log.Println("Missing preconf transaction hexes:", missingTxs)
			log.Println("transaction in this block:", blockTxMap)
			reason := fmt.Sprintf("missing %d required preconf transactions", len(missingTxs))
			if isValidPreconf != "" {
				isValidPreconf += "; " + reason
			} else {
				isValidPreconf = reason
			}

		} else {
			log.Printf("All preconf transactions are included in the block!")
			// Remains empty string for valid case
		}

		//check remain empty space
		//hardcode test:
		// cachedPreconfs.EmptySpace = "30000000"
		if cachedPreconfs.EmptySpace != "" {
			requiredSpace, err := strconv.ParseUint(cachedPreconfs.EmptySpace, 10, 64)
			if err == nil {
				remainingGas := submission.BidTrace.GasLimit - submission.BidTrace.GasUsed
				if remainingGas < requiredSpace {
					reason := fmt.Sprintf("block doesn't have enough empty space (remaining gas %d < required %d)",
						remainingGas, requiredSpace)
					if isValidPreconf != "" {
						isValidPreconf += "; " + reason
					} else {
						isValidPreconf = reason
					}
				}
			}
		}
	}

	log = log.WithFields(logrus.Fields{
		"timestampAfterDecoding": time.Now().UTC().UnixMilli(),
		"slot":                   submission.BidTrace.Slot,
		"builderPubkey":          submission.BidTrace.BuilderPubkey.String(),
		"blockHash":              submission.BidTrace.BlockHash.String(),
		"proposerPubkey":         submission.BidTrace.ProposerPubkey.String(),
		"parentHash":             submission.BidTrace.ParentHash.String(),
		"value":                  submission.BidTrace.Value.Dec(),
		"numTx":                  len(submission.Transactions),
		"payloadBytes":           len(requestPayloadBytes),
		"isLargeRequest":         isLargeRequest,
	})
	if payload.Version >= spec.DataVersionDeneb {
		blobs, err := payload.Blobs()
		if err != nil {
			api.RespondError(w, http.StatusBadRequest, err.Error())
			return
		}
		blobGasUsed, err := payload.BlobGasUsed()
		if err != nil {
			api.RespondError(w, http.StatusBadRequest, err.Error())
			return
		}
		excessBlobGas, err := payload.ExcessBlobGas()
		if err != nil {
			api.RespondError(w, http.StatusBadRequest, err.Error())
			return
		}
		log = log.WithFields(logrus.Fields{
			"numBlobs":      len(blobs),
			"blobGasUsed":   blobGasUsed,
			"excessBlobGas": excessBlobGas,
		})
	}

	ok := api.checkSubmissionSlotDetails(w, log, headSlot, payload, submission)
	if !ok {
		return
	}

	builderPubkey := submission.BidTrace.BuilderPubkey
	builderEntry, ok := api.checkBuilderEntry(w, log, builderPubkey)
	if !ok {
		return
	}

	log = log.WithField("builderIsHighPrio", builderEntry.status.IsHighPrio)

	gasLimit, ok := api.checkSubmissionFeeRecipient(w, log, submission.BidTrace, feeRecipient, builderPubkey.String())
	if !ok {
		return
	}

	// preconf txs should have 0 bid value if there have no public txs
	// // Don't accept blocks with 0 value
	// if submission.BidTrace.Value.ToBig().Cmp(ZeroU256.BigInt()) == 0 || len(submission.Transactions) == 0 {
	// 	log.Info("submitNewBlock failed: block with 0 value or no txs")
	// 	w.WriteHeader(http.StatusOK)
	// 	return
	// }

	// Sanity check the submission
	err = SanityCheckBuilderBlockSubmission(payload)
	if err != nil {
		log.WithError(err).Info("block submission sanity checks failed")
		api.RespondError(w, http.StatusBadRequest, err.Error())
		return
	}

	//temp: disable payload checking for devnet
	// attrs, ok := api.checkSubmissionPayloadAttrs(w, log, submission)

	// if !ok {
	// 	return
	// }
	attrs := api.payloadAttributes[getPayloadAttributesKey(submission.BidTrace.ParentHash.String(), submission.BidTrace.Slot)]

	// Verify the signature
	log = log.WithField("timestampBeforeSignatureCheck", time.Now().UTC().UnixMilli())
	signature := submission.Signature
	ok, err = ssz.VerifySignature(submission.BidTrace, api.opts.EthNetDetails.DomainBuilder, builderPubkey[:], signature[:])
	log = log.WithField("timestampAfterSignatureCheck", time.Now().UTC().UnixMilli())
	if err != nil {
		log.WithError(err).Warn("failed verifying builder signature")
		api.RespondError(w, http.StatusBadRequest, "failed verifying builder signature")
		return
	} else if !ok {
		log.Warn("invalid builder signature")
		api.RespondError(w, http.StatusBadRequest, "invalid signature")
		return
	}

	log = log.WithField("timestampBeforeCheckingFloorBid", time.Now().UTC().UnixMilli())

	// Create the redis pipeline tx
	tx := api.redis.NewTxPipeline()

	// channel to send simulation result to the deferred function
	simResultC := make(chan *blockSimResult, 1)
	var eligibleAt time.Time // will be set once the bid is ready

	bfOpts := bidFloorOpts{
		w:                    w,
		tx:                   tx,
		log:                  log,
		cancellationsEnabled: isCancellationEnabled,
		simResultC:           simResultC,
		submission:           submission,
	}
	floorBidValue, ok := api.checkFloorBidValue(bfOpts)
	if !ok {
		return
	}

	pf.AboveFloorBid = true
	log = log.WithField("timestampAfterCheckingFloorBid", time.Now().UTC().UnixMilli())

	// Deferred saving of the builder submission to database (whenever this function ends)
	defer func() {
		savePayloadToDatabase := !api.ffDisablePayloadDBStorage
		var simResult *blockSimResult
		select {
		case simResult = <-simResultC:
		case <-time.After(10 * time.Second):
			log.Warn("timed out waiting for simulation result")
			simResult = &blockSimResult{false, nil, false, nil, nil}
		}

		if isValidPreconf != "" {
			simResult.requestErr = fmt.Errorf("invalid preconf: %s", isValidPreconf)
			log.Warn("Invalid preconf detected: " + isValidPreconf)
		}

		submissionEntry, err := api.db.SaveBuilderBlockSubmission(payload, simResult.requestErr, simResult.validationErr, receivedAt, eligibleAt, simResult.wasSimulated, savePayloadToDatabase, pf, simResult.optimisticSubmission, simResult.blockValue)
		if err != nil {
			log.WithError(err).WithFields(logrus.Fields{
				"payload":   payload,
				"simResult": simResult,
			}).Error("saving builder block submission to database failed")
			return
		}

		err = api.db.UpsertBlockBuilderEntryAfterSubmission(submissionEntry, simResult.validationErr != nil)
		if err != nil {
			log.WithError(err).Error("failed to upsert block-builder-entry")
		}
	}()

	// ---------------------------------
	// THE BID WILL BE SIMULATED SHORTLY
	// ---------------------------------

	log = log.WithField("timestampBeforeCheckingTopBid", time.Now().UTC().UnixMilli())

	// Get the latest top bid value from Redis
	bidIsTopBid := false
	topBidValue, err := api.redis.GetTopBidValue(context.Background(), tx, submission.BidTrace.Slot, submission.BidTrace.ParentHash.String(), submission.BidTrace.ProposerPubkey.String())
	if err != nil {
		log.WithError(err).Error("failed to get top bid value from redis")
	} else {
		bidIsTopBid = submission.BidTrace.Value.ToBig().Cmp(topBidValue) == 1
		log = log.WithFields(logrus.Fields{
			"topBidValue":    topBidValue.String(),
			"newBidIsTopBid": bidIsTopBid,
		})
	}

	log = log.WithField("timestampAfterCheckingTopBid", time.Now().UTC().UnixMilli())

	nextTime = time.Now().UTC()
	pf.Prechecks = uint64(nextTime.Sub(prevTime).Microseconds()) //nolint:gosec
	prevTime = nextTime

	// Simulate the block submission and save to db
	fastTrackValidation := builderEntry.status.IsHighPrio && bidIsTopBid && !isLargeRequest
	timeBeforeValidation := time.Now().UTC()

	log = log.WithFields(logrus.Fields{
		"timestampBeforeValidation": timeBeforeValidation.UTC().UnixMilli(),
		"fastTrackValidation":       fastTrackValidation,
	})

	// Construct simulation request
	opts := blockSimOptions{
		isHighPrio: builderEntry.status.IsHighPrio,
		fastTrack:  fastTrackValidation,
		log:        log,
		builder:    builderEntry,
		req: &common.BuilderBlockValidationRequest{
			VersionedSubmitBlockRequest: payload,
			RegisteredGasLimit:          gasLimit,
			ParentBeaconBlockRoot:       attrs.parentBeaconRoot,
		},
	}
	// With sufficient collateral, process the block optimistically.
	optimistic := builderEntry.status.IsOptimistic &&
		builderEntry.collateral.Cmp(submission.BidTrace.Value.ToBig()) >= 0 &&
		submission.BidTrace.Slot == api.optimisticSlot.Load()
	pf.Optimistic = optimistic
	if optimistic {
		go api.processOptimisticBlock(opts, simResultC)
	} else {
		// Simulate block (synchronously).
		blockValue, requestErr, validationErr := api.simulateBlock(context.Background(), opts) // success/error logging happens inside
		simResultC <- &blockSimResult{requestErr == nil, blockValue, false, requestErr, validationErr}
		validationDurationMs := time.Since(timeBeforeValidation).Milliseconds()
		log = log.WithFields(logrus.Fields{
			"timestampAfterValidation": time.Now().UTC().UnixMilli(),
			"validationDurationMs":     validationDurationMs,
		})
		if requestErr != nil { // Request error
			if os.IsTimeout(requestErr) {
				api.RespondError(w, http.StatusGatewayTimeout, "validation request timeout")
			} else {
				api.RespondError(w, http.StatusBadRequest, requestErr.Error())
			}
			return
		} else {
			if validationErr != nil {
				api.RespondError(w, http.StatusBadRequest, validationErr.Error())
				return
			}
		}
	}

	nextTime = time.Now().UTC()
	pf.Simulation = uint64(nextTime.Sub(prevTime).Microseconds()) //nolint:gosec
	pf.SimulationSuccess = true
	prevTime = nextTime

	// If cancellations are enabled, then abort now if this submission is not the latest one
	if isCancellationEnabled {
		// Ensure this request is still the latest one. This logic intentionally ignores the value of the bids and makes the current active bid the one
		// that arrived at the relay last. This allows for builders to reduce the value of their bid (effectively cancel a high bid) by ensuring a lower
		// bid arrives later. Even if the higher bid takes longer to simulate, by checking the receivedAt timestamp, this logic ensures that the low bid
		// is not overwritten by the high bid.
		//
		// NOTE: this can lead to a rather tricky race condition. If a builder submits two blocks to the relay concurrently, then the randomness of network
		// latency will make it impossible to predict which arrives first. Thus a high bid could unintentionally be overwritten by a low bid that happened
		// to arrive a few microseconds later. If builders are submitting blocks at a frequency where they cannot reliably predict which bid will arrive at
		// the relay first, they should instead use multiple pubkeys to avoid uninitentionally overwriting their own bids.
		latestPayloadReceivedAt, err := api.redis.GetBuilderLatestPayloadReceivedAt(context.Background(), tx, submission.BidTrace.Slot, submission.BidTrace.BuilderPubkey.String(), submission.BidTrace.ParentHash.String(), submission.BidTrace.ProposerPubkey.String())
		if err != nil {
			log.WithError(err).Error("failed getting latest payload receivedAt from redis")
		} else if receivedAt.UnixMilli() < latestPayloadReceivedAt {
			log.Infof("already have a newer payload: now=%d / prev=%d", receivedAt.UnixMilli(), latestPayloadReceivedAt)
			api.RespondError(w, http.StatusBadRequest, "already using a newer payload")
			return
		}
	}

	redisOpts := redisUpdateBidOpts{
		w:                    w,
		tx:                   tx,
		log:                  log,
		cancellationsEnabled: isCancellationEnabled,
		receivedAt:           receivedAt,
		floorBidValue:        floorBidValue,
		payload:              payload,
		isValidPreconf:       isValidPreconf == "",
	}
	updateBidResult, getPayloadResponse, ok := api.updateRedisBid(redisOpts)
	if !ok {
		return
	}

	// Add fields to logs
	log = log.WithFields(logrus.Fields{
		"timestampAfterBidUpdate":    time.Now().UTC().UnixMilli(),
		"wasBidSavedInRedis":         updateBidResult.WasBidSaved,
		"wasTopBidUpdated":           updateBidResult.WasTopBidUpdated,
		"topBidValue":                updateBidResult.TopBidValue,
		"prevTopBidValue":            updateBidResult.PrevTopBidValue,
		"profileRedisSavePayloadUs":  updateBidResult.TimeSavePayload.Microseconds(),
		"profileRedisUpdateTopBidUs": updateBidResult.TimeUpdateTopBid.Microseconds(),
		"profileRedisUpdateFloorUs":  updateBidResult.TimeUpdateFloor.Microseconds(),
	})

	if updateBidResult.WasBidSaved {
		// Bid is eligible to win the auction
		eligibleAt = time.Now().UTC()
		log = log.WithField("timestampEligibleAt", eligibleAt.UnixMilli())

		// Save to memcache in the background
		if api.memcached != nil {
			go func() {
				err = api.memcached.SaveExecutionPayload(submission.BidTrace.Slot, submission.BidTrace.ProposerPubkey.String(), submission.BidTrace.BlockHash.String(), getPayloadResponse)
				if err != nil {
					log.WithError(err).Error("failed saving execution payload in memcached")
				}
			}()
		}
	}

	nextTime = time.Now().UTC()
	pf.WasBidSaved = updateBidResult.WasBidSaved
	pf.RedisUpdate = uint64(nextTime.Sub(prevTime).Microseconds())                 //nolint:gosec
	pf.RedisSavePayload = uint64(updateBidResult.TimeSavePayload.Microseconds())   //nolint:gosec
	pf.RedisUpdateTopBid = uint64(updateBidResult.TimeUpdateTopBid.Microseconds()) //nolint:gosec
	pf.RedisUpdateFloor = uint64(updateBidResult.TimeUpdateFloor.Microseconds())   //nolint:gosec
	pf.Total = uint64(nextTime.Sub(receivedAt).Microseconds())                     //nolint:gosec

	// All done, log with profiling information
	log.WithFields(logrus.Fields{
		"profileDecodeUs":    pf.Decode,
		"profilePrechecksUs": pf.Prechecks,
		"profileSimUs":       pf.Simulation,
		"profileRedisUs":     pf.RedisUpdate,
		"profileTotalUs":     pf.Total,
	}).Info("received block from builder")
	w.WriteHeader(http.StatusOK)
}

func (api *RelayAPI) saveBlockSubmissionMetrics(pf common.Profile, receivedTime time.Time) {
	if pf.PayloadLoad > 0 {
		metrics.SubmitNewBlockReadLatencyHistogram.Record(
			context.Background(),
			float64(pf.PayloadLoad)/1000,
			otelapi.WithAttributes(attribute.Bool("isGzip", pf.IsGzip)),
		)
	}
	if pf.Decode > 0 {
		metrics.SubmitNewBlockDecodeLatencyHistogram.Record(
			context.Background(),
			float64(pf.Decode)/1000,
			otelapi.WithAttributes(attribute.String("contentType", pf.ContentType)),
		)
	}

	if pf.Prechecks > 0 {
		metrics.SubmitNewBlockPrechecksLatencyHistogram.Record(
			context.Background(),
			float64(pf.Prechecks)/1000,
		)
	}

	if pf.Simulation > 0 {
		metrics.SubmitNewBlockSimulationLatencyHistogram.Record(
			context.Background(),
			float64(pf.Simulation)/1000,
			otelapi.WithAttributes(attribute.Bool("simulationSuccess", pf.SimulationSuccess)),
		)
	}

	if pf.RedisUpdate > 0 {
		metrics.SubmitNewBlockRedisLatencyHistogram.Record(
			context.Background(),
			float64(pf.RedisUpdate)/1000,
			otelapi.WithAttributes(attribute.Bool("wasBidSaved", pf.WasBidSaved)),
		)
	}

	if pf.RedisSavePayload > 0 {
		metrics.SubmitNewBlockRedisPayloadLatencyHistogram.Record(
			context.Background(),
			float64(pf.RedisSavePayload)/1000,
		)
	}

	if pf.RedisUpdateTopBid > 0 {
		metrics.SubmitNewBlockRedisTopBidLatencyHistogram.Record(
			context.Background(),
			float64(pf.RedisUpdateTopBid)/1000,
		)
	}

	if pf.RedisUpdateFloor > 0 {
		metrics.SubmitNewBlockRedisFloorLatencyHistogram.Record(
			context.Background(),
			float64(pf.RedisUpdateFloor)/1000,
		)
	}

	metrics.SubmitNewBlockLatencyHistogram.Record(
		context.Background(),
		float64(time.Since(receivedTime).Milliseconds()),
		otelapi.WithAttributes(
			attribute.String("contentType", pf.ContentType),
			attribute.Bool("isGzip", pf.IsGzip),
			attribute.Bool("aboveFloorBid", pf.AboveFloorBid),
			attribute.Bool("simulationSuccess", pf.SimulationSuccess),
			attribute.Bool("wasBidSaved", pf.WasBidSaved),
			attribute.Bool("optimistic", pf.Optimistic),
		),
	)
}

// ---------------
//
//	INTERNAL APIS
//
// ---------------
func (api *RelayAPI) handleInternalBuilderStatus(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	builderPubkey := vars["pubkey"]
	builderEntry, err := api.db.GetBlockBuilderByPubkey(builderPubkey)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			api.RespondError(w, http.StatusBadRequest, "builder not found")
			return
		}

		api.log.WithError(err).Error("could not get block builder")
		api.RespondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if req.Method == http.MethodGet {
		api.RespondOK(w, builderEntry)
		return
	} else if req.Method == http.MethodPost || req.Method == http.MethodPut || req.Method == http.MethodPatch {
		st := common.BuilderStatus{
			IsHighPrio:    builderEntry.IsHighPrio,
			IsBlacklisted: builderEntry.IsBlacklisted,
			IsOptimistic:  builderEntry.IsOptimistic,
		}
		trueStr := "true"
		args := req.URL.Query()
		if args.Get("high_prio") != "" {
			st.IsHighPrio = args.Get("high_prio") == trueStr
		}
		if args.Get("blacklisted") != "" {
			st.IsBlacklisted = args.Get("blacklisted") == trueStr
		}
		if args.Get("optimistic") != "" {
			st.IsOptimistic = args.Get("optimistic") == trueStr
		}
		api.log.WithFields(logrus.Fields{
			"builderPubkey": builderPubkey,
			"isHighPrio":    st.IsHighPrio,
			"isBlacklisted": st.IsBlacklisted,
			"isOptimistic":  st.IsOptimistic,
		}).Info("updating builder status")
		err := api.db.SetBlockBuilderStatus(builderPubkey, st)
		if err != nil {
			err := fmt.Errorf("error setting builder: %v status: %w", builderPubkey, err)
			api.log.Error(err)
			api.RespondError(w, http.StatusInternalServerError, err.Error())
			return
		}
		api.RespondOK(w, st)
	}
}

func (api *RelayAPI) handleInternalBuilderCollateral(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	builderPubkey := vars["pubkey"]
	if req.Method == http.MethodPost || req.Method == http.MethodPut {
		args := req.URL.Query()
		collateral := args.Get("collateral")
		value := args.Get("value")
		log := api.log.WithFields(logrus.Fields{
			"pubkey":     builderPubkey,
			"collateral": collateral,
			"value":      value,
		})
		log.Infof("updating builder collateral")
		if err := api.db.SetBlockBuilderCollateral(builderPubkey, collateral, value); err != nil {
			fullErr := fmt.Errorf("unable to set collateral in db for pubkey: %v: %w", builderPubkey, err)
			log.Error(fullErr.Error())
			api.RespondError(w, http.StatusInternalServerError, fullErr.Error())
			return
		}
		api.RespondOK(w, NilResponse)
	}
}

// -----------
//  DATA APIS
// -----------

func (api *RelayAPI) handleDataProposerPayloadDelivered(w http.ResponseWriter, req *http.Request) {
	var err error
	args := req.URL.Query()

	filters := database.GetPayloadsFilters{
		Limit: 200,
	}

	if args.Get("slot") != "" && args.Get("cursor") != "" {
		api.RespondError(w, http.StatusBadRequest, "cannot specify both slot and cursor")
		return
	} else if args.Get("slot") != "" {
		filters.Slot, err = strconv.ParseInt(args.Get("slot"), 10, 64)
		if err != nil {
			api.RespondError(w, http.StatusBadRequest, "invalid slot argument")
			return
		}
	} else if args.Get("cursor") != "" {
		filters.Cursor, err = strconv.ParseInt(args.Get("cursor"), 10, 64)
		if err != nil {
			api.RespondError(w, http.StatusBadRequest, "invalid cursor argument")
			return
		}
	}

	if args.Get("block_hash") != "" {
		_, err := utils.HexToHash(args.Get("block_hash"))
		if err != nil {
			api.RespondError(w, http.StatusBadRequest, "invalid block_hash argument")
			return
		}
		filters.BlockHash = args.Get("block_hash")
	}

	if args.Get("block_number") != "" {
		filters.BlockNumber, err = strconv.ParseInt(args.Get("block_number"), 10, 64)
		if err != nil {
			api.RespondError(w, http.StatusBadRequest, "invalid block_number argument")
			return
		}
	}

	if args.Get("proposer_pubkey") != "" {
		if err = checkBLSPublicKeyHex(args.Get("proposer_pubkey")); err != nil {
			api.RespondError(w, http.StatusBadRequest, "invalid proposer_pubkey argument")
			return
		}
		filters.ProposerPubkey = args.Get("proposer_pubkey")
	}

	if args.Get("builder_pubkey") != "" {
		if err = checkBLSPublicKeyHex(args.Get("builder_pubkey")); err != nil {
			api.RespondError(w, http.StatusBadRequest, "invalid builder_pubkey argument")
			return
		}
		filters.BuilderPubkey = args.Get("builder_pubkey")
	}

	if args.Get("limit") != "" {
		_limit, err := strconv.ParseUint(args.Get("limit"), 10, 64)
		if err != nil {
			api.RespondError(w, http.StatusBadRequest, "invalid limit argument")
			return
		}
		if _limit > filters.Limit {
			api.RespondError(w, http.StatusBadRequest, fmt.Sprintf("maximum limit is %d", filters.Limit))
			return
		}
		filters.Limit = _limit
	}

	if args.Get("order_by") == "value" {
		filters.OrderByValue = 1
	} else if args.Get("order_by") == "-value" {
		filters.OrderByValue = -1
	}

	deliveredPayloads, err := api.db.GetRecentDeliveredPayloads(filters)
	if err != nil {
		api.log.WithError(err).Error("error getting recently delivered payloads")
		api.RespondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	response := make([]common.BidTraceV2JSON, len(deliveredPayloads))
	for i, payload := range deliveredPayloads {
		response[i] = database.DeliveredPayloadEntryToBidTraceV2JSON(payload)
	}

	api.RespondOK(w, response)
}

func (api *RelayAPI) handleDataBuilderBidsReceived(w http.ResponseWriter, req *http.Request) {
	var err error
	args := req.URL.Query()

	filters := database.GetBuilderSubmissionsFilters{
		Limit:         500,
		Slot:          0,
		BlockHash:     "",
		BlockNumber:   0,
		BuilderPubkey: "",
	}

	if args.Get("cursor") != "" {
		api.RespondError(w, http.StatusBadRequest, "cursor argument not supported")
		return
	}

	if args.Get("slot") != "" {
		filters.Slot, err = strconv.ParseInt(args.Get("slot"), 10, 64)
		if err != nil {
			api.RespondError(w, http.StatusBadRequest, "invalid slot argument")
			return
		}
	}

	if args.Get("block_hash") != "" {
		_, err := utils.HexToHash(args.Get("block_hash"))
		if err != nil {
			api.RespondError(w, http.StatusBadRequest, "invalid block_hash argument")
			return
		}
		filters.BlockHash = args.Get("block_hash")
	}

	if args.Get("block_number") != "" {
		filters.BlockNumber, err = strconv.ParseInt(args.Get("block_number"), 10, 64)
		if err != nil {
			api.RespondError(w, http.StatusBadRequest, "invalid block_number argument")
			return
		}
	}

	if args.Get("builder_pubkey") != "" {
		if err = checkBLSPublicKeyHex(args.Get("builder_pubkey")); err != nil {
			api.RespondError(w, http.StatusBadRequest, "invalid builder_pubkey argument")
			return
		}
		filters.BuilderPubkey = args.Get("builder_pubkey")
	}

	// at least one query arguments is required
	if filters.Slot == 0 && filters.BlockHash == "" && filters.BlockNumber == 0 && filters.BuilderPubkey == "" {
		api.RespondError(w, http.StatusBadRequest, "need to query for specific slot or block_hash or block_number or builder_pubkey")
		return
	}

	if args.Get("limit") != "" {
		_limit, err := strconv.ParseInt(args.Get("limit"), 10, 64)
		if err != nil {
			api.RespondError(w, http.StatusBadRequest, "invalid limit argument")
			return
		}
		if _limit > filters.Limit {
			api.RespondError(w, http.StatusBadRequest, fmt.Sprintf("maximum limit is %d", filters.Limit))
			return
		}
		filters.Limit = _limit
	}

	blockSubmissions, err := api.db.GetBuilderSubmissions(filters)
	if err != nil {
		api.log.WithError(err).Error("error getting recent builder submissions")
		api.RespondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	response := make([]common.BidTraceV2WithTimestampJSON, len(blockSubmissions))
	for i, payload := range blockSubmissions {
		response[i] = database.BuilderSubmissionEntryToBidTraceV2WithTimestampJSON(payload)
	}

	api.RespondOK(w, response)
}

func (api *RelayAPI) handleDataValidatorRegistration(w http.ResponseWriter, req *http.Request) {
	pkStr := req.URL.Query().Get("pubkey")
	if pkStr == "" {
		api.RespondError(w, http.StatusBadRequest, "missing pubkey argument")
		return
	}

	_, err := utils.HexToPubkey(pkStr)
	if err != nil {
		api.RespondError(w, http.StatusBadRequest, "invalid pubkey")
		return
	}

	registrationEntry, err := api.db.GetValidatorRegistration(pkStr)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			api.RespondError(w, http.StatusBadRequest, "no registration found for validator "+pkStr)
			return
		}
		api.log.WithError(err).Error("error getting validator registration")
		api.RespondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	signedRegistration, err := registrationEntry.ToSignedValidatorRegistration()
	if err != nil {
		api.log.WithError(err).Error("error converting registration entry to signed validator registration")
		api.RespondError(w, http.StatusInternalServerError, err.Error())
		return
	}

	api.RespondOK(w, signedRegistration)
}

func (api *RelayAPI) handleLivez(w http.ResponseWriter, req *http.Request) {
	api.RespondMsg(w, http.StatusOK, "live")
}

func (api *RelayAPI) handleReadyz(w http.ResponseWriter, req *http.Request) {
	if api.IsReady() {
		api.RespondMsg(w, http.StatusOK, "ready")
	} else {
		api.RespondMsg(w, http.StatusServiceUnavailable, "not ready")
	}
}

// FetchBuilderPubKey fetches the builder and fallbackBuilder from the /builder/pubkey/:slot endpoint
func FetchBuilderPubKey(apiURL string, slot uint64) (*BuilderResponse, error) {
	// Construct the URL for the API request
	// url := fmt.Sprintf("%s/api/p/builder/pubkey/%d", apiURL, slot)
	url := fmt.Sprintf("%s/api/v1/p/builder/%d", apiURL, slot)

	// Send HTTP GET request
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch builder pubkey: %w", err)
	}
	defer resp.Body.Close()

	// Check if the HTTP response status code is OK (200)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var apiResponse ApiResponse
	err = json.NewDecoder(resp.Body).Decode(&apiResponse)
	if err != nil {
		log.Printf("Failed to decode API response: %v", err)
		return nil, fmt.Errorf("failed to decode API response: %v", err)
	}

	if !apiResponse.Success {
		log.Printf("API response indicates failure: %v", apiResponse)
		return nil, fmt.Errorf("API response indicates failure: %v", apiResponse)
	}

	// Handle empty data case explicitly
	if len(apiResponse.Data) == 0 {
		return nil, fmt.Errorf("empty data in API response")
	}

	var builderResp DataResponse
	err = json.Unmarshal(apiResponse.Data, &builderResp)
	if err != nil {
		log.Printf("Failed to unmarshal builder response: %v", err)
		return nil, err
	}

	// Validate required fields
	if builderResp.Builder.Builder == "" || builderResp.Builder.FallbackBuilder == "" {
		return nil, fmt.Errorf("invalid builder response: missing required fields")
	}

	return &builderResp.Builder, nil
}

// Login sends a login request and completes the EIP712 signature process
func (c *ApiClient) Login(privateKey string) (string, string, error) {
	privateKeyBytes, err := hex.DecodeString(privateKey)
	if err != nil {
		return "", "", fmt.Errorf("failed to decode private key: %w", err)
	}
	privateKeyECDSA, err := crypto.ToECDSA(privateKeyBytes)
	if err != nil {
		return "", "", fmt.Errorf("failed to create ECDSA private key: %w", err)
	}
	address := crypto.PubkeyToAddress(privateKeyECDSA.PublicKey).Hex()

	// loginURL := fmt.Sprintf("%s/api/user/login", c.APIURL)
	loginURL := fmt.Sprintf("%s/api/v1/user/login", c.APIURL)

	formData := url.Values{}
	formData.Set("addr", address)
	formData.Set("chainId", c.ChainID)

	resp, err := c.Client.PostForm(loginURL, formData)
	if err != nil {
		return "", "", fmt.Errorf("failed to send login request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return "", "", fmt.Errorf("login failed with status %d: %s", resp.StatusCode, string(body))
	}

	var apiResponse ApiResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResponse); err != nil {
		return "", "", fmt.Errorf("failed to parse login response: %w", err)
	}
	if !apiResponse.Success {
		fmt.Println("Failed response:", apiResponse)
		return "", "", fmt.Errorf("login failed: response indicates failure")
	}

	var loginData LoginResponse
	if err := json.Unmarshal(apiResponse.Data, &loginData); err != nil {
		return "", "", fmt.Errorf("failed to parse login data: %w", err)
	}

	fmt.Println("EIP712Message:", loginData.EIP712Message)
	fmt.Println("NonceHash:", loginData.NonceHash)
	// Parse the EIP712Message JSON string into apitypes.TypedData
	var typedData apitypes.TypedData
	if err := json.Unmarshal([]byte(loginData.EIP712Message), &typedData); err != nil {
		return "", "", fmt.Errorf("failed to unmarshal EIP712Message: %w", err)
	}
	// signature, err := SignEIP712Message(privateKeyECDSA, loginData.EIP712Message)
	signature, err := eip712.SignTypedData(typedData, privateKeyECDSA)
	var signatureHash = hex.EncodeToString(signature)
	if err != nil {
		return "", "", fmt.Errorf("failed to sign EIP712 message: %w", err)
	}
	fmt.Println("Signature:", signatureHash)

	// verifyURL := fmt.Sprintf("%s/api/user/login/verify", c.APIURL)
	verifyURL := fmt.Sprintf("%s/api/v1/user/login/verify", c.APIURL)

	verifyFormData := url.Values{}
	verifyFormData.Set("addr", address)
	verifyFormData.Set("signature", signatureHash)
	verifyFormData.Set("nonceHash", loginData.NonceHash)

	verifyResp, err := c.Client.PostForm(verifyURL, verifyFormData)
	if err != nil {
		return "", "", fmt.Errorf("failed to send verification request: %w", err)
	}
	defer verifyResp.Body.Close()

	if verifyResp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(verifyResp.Body)
		return "", "", fmt.Errorf("verification failed with status %d: %s", verifyResp.StatusCode, string(body))
	}

	var verifyApiResponse ApiResponse
	if err := json.NewDecoder(verifyResp.Body).Decode(&verifyApiResponse); err != nil {
		return "", "", fmt.Errorf("failed to parse verification response: %w", err)
	}
	if !verifyApiResponse.Success {
		return "", "", fmt.Errorf("verification failed: response indicates failure")
	}

	var verifyData VerifyResponse
	if err := json.Unmarshal(verifyApiResponse.Data, &verifyData); err != nil {
		return "", "", fmt.Errorf("failed to parse verify data: %w", err)
	}

	c.AccessToken = verifyData.AccessToken.Token
	c.RefreshToken = c.extractRefreshToken(verifyResp)
	return c.AccessToken, c.RefreshToken, nil
}

// RefreshAccessToken refreshes the access token using the refresh token
func (c *ApiClient) RefreshAccessToken() error {
	// refreshURL := fmt.Sprintf("%s/api/user/login/refresh", c.APIURL)
	refreshURL := fmt.Sprintf("%s/api/v1/user/login/refresh", c.APIURL)

	// Prepare the form data
	formData := url.Values{}
	formData.Set("refreshToken", c.RefreshToken)

	// Send the refresh token request
	resp, err := c.Client.PostForm(refreshURL, formData)
	if err != nil {
		return fmt.Errorf("failed to send refresh token request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("refresh token failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Parse the refresh response
	var apiResponse ApiResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResponse); err != nil {
		return fmt.Errorf("failed to parse refresh token response: %w", err)
	}
	if !apiResponse.Success {
		return fmt.Errorf("refresh token failed: response indicates failure")
	}

	// Extract the verify data
	var verifyData VerifyResponse
	if err := json.Unmarshal(apiResponse.Data, &verifyData); err != nil {
		return fmt.Errorf("failed to parse verify data: %w", err)
	}

	// Save new access token
	c.AccessToken = verifyData.AccessToken.Token
	return nil
}

func (c *ApiClient) extractRefreshToken(resp *http.Response) string {
	for _, cookie := range resp.Cookies() {
		if cookie.Name == "x_auth_refresh_token" {
			return cookie.Value
		}
	}
	return ""
}

func InitLoginAndStartTokenRefresh() {
	// Perform the initial login to get tokens
	// TODO config

	// privateKey := "8ca6e6e33b2170de9e6ce76bbb5808f8d5ec3e112c2c72cd0b97614f00061f0e"

	accessToken, refreshToken, err := client.Login(exchangeLoginPrivateKey)
	if err != nil || accessToken == "" || refreshToken == "" {
		log.Printf("Failed to login during initialization: %v", err)
		return
	}

	// Start a goroutine to refresh the tokens every 30 minutes
	go client.startTokenRefreshLoop()
	go client.startDailyLoginLoop(exchangeLoginPrivateKey)
}

// startTokenRefreshLoop refreshes access tokens every 30 minutes
func (c *ApiClient) startTokenRefreshLoop() {
	log.Println("Starting access token refresh loop...")
	ticker := time.NewTicker(30 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		err := c.RefreshAccessToken()
		if err != nil {
			log.Printf("Failed to refresh access token: %v", err)
		}
	}
}

// startDailyLoginLoop logs in every 24 hours
func (c *ApiClient) startDailyLoginLoop(privateKey string) {
	log.Println("Starting daily login loop...")
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()

	for range ticker.C {
		accessToken, refreshToken, err := c.Login(privateKey)
		if err != nil || accessToken == "" || refreshToken == "" {
			log.Printf("Failed to login during initialization: %v", err)
			return
		}
	}
}
