package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	sftransform "github.com/figment-networks/firehose-cosmos/transform"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/transform"
	dauthAuthenticator "github.com/streamingfast/dauth/authenticator"
	_ "github.com/streamingfast/dauth/authenticator/null"
	"github.com/streamingfast/dlauncher/launcher"
	"github.com/streamingfast/dmetering"
	"github.com/streamingfast/dmetrics"
	"github.com/streamingfast/dstore"
	firehoseApp "github.com/streamingfast/firehose/app/firehose"
	"github.com/streamingfast/logging"
)

var (
	metricset           = dmetrics.NewSet()
	headBlockNumMetric  = metricset.NewHeadBlockNumber("firehose")
	headTimeDriftmetric = metricset.NewHeadTimeDrift("firehose")
)

func init() {
	appLogger := zap.NewNop()
	logging.Register("firehose", &appLogger)

	registerFlags := func(cmd *cobra.Command) error {
		cmd.Flags().String("firehose-grpc-listen-addr", FirehoseGRPCServingAddr, "Address on which the firehose will listen")
		cmd.Flags().StringSlice("firehose-blocks-store-urls", nil, "If non-empty, overrides common-blocks-store-url with a list of blocks stores")
		cmd.Flags().Duration("firehose-real-time-tolerance", 1*time.Minute, "Firehose will became alive if now - block time is smaller then tolerance")
		cmd.Flags().Uint64("firehose-tracker-offset", 100, "Number of blocks for the bstream block resolver")
		cmd.Flags().String("firehose-block-index-url", "", "If non-empty, will use this URL as a store to load index data used by some transforms")
		cmd.Flags().IntSlice("firehose-block-index-sizes", []int{100000, 10000, 1000, 100}, "List of sizes for block indices")
		cmd.Flags().String("firehose-rpc-head-tracker-url", "", "If non-empty, will use this URL to make RPC calls to status endpoint")
		cmd.Flags().String("firehose-static-head-tracker", "", "If non-empty, will use this static block height in tracker")
		return nil
	}

	factoryFunc := func(runtime *launcher.Runtime) (launcher.App, error) {
		sfDataDir := runtime.AbsDataDir

		tracker := runtime.Tracker.Clone()
		tracker.AddResolver(bstream.OffsetStartBlockResolver(viper.GetUint64("firehose-tracker-offset")))

		// Configure block stream connection (to node or relayer)
		blockstreamAddr := viper.GetString("common-blockstream-addr")
		if blockstreamAddr == "-" {
			blockstreamAddr = ""
		}
		if blockstreamAddr != "" {
			tracker.AddGetter(bstream.BlockStreamLIBTarget, bstream.StreamLIBBlockRefGetter(blockstreamAddr))
			tracker.AddGetter(bstream.BlockStreamHeadTarget, bstream.StreamHeadBlockRefGetter(blockstreamAddr))
		}

		// Enable HEAD tracker when block stream is not available, to allow firehose to serve static data
		rpcHeadTrackerURL := viper.GetString("firehose-rpc-head-tracker-url")
		if rpcHeadTrackerURL != "" && blockstreamAddr == "" {
			tracker.AddGetter(bstream.BlockStreamHeadTarget, rpcHeadTracker(rpcHeadTrackerURL))
		}

		// Enable HEAD tracker that returns a static block ref value
		staticHeadTrackerVal := viper.GetString("firehose-static-head-tracker")
		if staticHeadTrackerVal != "" && blockstreamAddr == "" && rpcHeadTrackerURL == "" {
			parts := strings.SplitN(staticHeadTrackerVal, ":", 2)

			height, err := strconv.Atoi(parts[0])
			if err != nil {
				return nil, err
			}

			tracker.AddGetter(bstream.BlockStreamHeadTarget, staticHeadTracker(uint64(height), parts[1]))
		}

		// Configure authentication (default is no auth)
		authenticator, err := dauthAuthenticator.New(viper.GetString("common-auth-plugin"))
		if err != nil {
			return nil, fmt.Errorf("unable to initialize dauth: %w", err)
		}

		// Metering? (TODO: what is this for?)
		metering, err := dmetering.New(viper.GetString("common-metering-plugin"))
		if err != nil {
			return nil, fmt.Errorf("unable to initialize dmetering: %w", err)
		}
		dmetering.SetDefaultMeter(metering)

		// Configure firehose data sources
		firehoseBlocksStoreURLs := viper.GetStringSlice("firehose-blocks-store-urls")
		if len(firehoseBlocksStoreURLs) == 0 {
			firehoseBlocksStoreURLs = []string{viper.GetString("common-blocks-store-url")}
		} else if len(firehoseBlocksStoreURLs) == 1 && strings.Contains(firehoseBlocksStoreURLs[0], ",") {
			firehoseBlocksStoreURLs = strings.Split(firehoseBlocksStoreURLs[0], ",")
		}
		for i, url := range firehoseBlocksStoreURLs {
			firehoseBlocksStoreURLs[i] = mustReplaceDataDir(sfDataDir, url)
		}

		// Configure graceful shutdown
		shutdownSignalDelay := viper.GetDuration("common-system-shutdown-signal-delay")
		grcpShutdownGracePeriod := time.Duration(0)
		if shutdownSignalDelay.Seconds() > 5 {
			grcpShutdownGracePeriod = shutdownSignalDelay - (5 * time.Second)
		}

		indexStoreUrl := viper.GetString("firehose-block-index-url")
		var indexStore dstore.Store
		if indexStoreUrl != "" {
			s, err := dstore.NewStore(indexStoreUrl, "", "", false)
			if err != nil {
				return nil, fmt.Errorf("couldn't create an index store: %w", err)
			}
			indexStore = s
		}

		var possibleIndexSizes []uint64
		for _, size := range viper.GetIntSlice("firehose-block-index-sizes") {
			if size < 0 {
				return nil, fmt.Errorf("invalid negative size for firehose-block-index-sizes: %d", size)
			}
			possibleIndexSizes = append(possibleIndexSizes, uint64(size))
		}

		registry := transform.NewRegistry()
		registry.Register(sftransform.EventOriginFilterFactory(indexStore, possibleIndexSizes))
		registry.Register(sftransform.EventTypeFilterFactory(indexStore, possibleIndexSizes))
		registry.Register(sftransform.MessageTypeFilterFactory(indexStore, possibleIndexSizes))
		registry.Register(sftransform.CombinedFilterFactory(indexStore, possibleIndexSizes))

		return firehoseApp.New(appLogger,
			&firehoseApp.Config{
				BlockStoreURLs:                  firehoseBlocksStoreURLs,
				BlockStreamAddr:                 blockstreamAddr,
				GRPCListenAddr:                  viper.GetString("firehose-grpc-listen-addr"),
				GRPCShutdownGracePeriod:         grcpShutdownGracePeriod,
				RealtimeTolerance:               viper.GetDuration("firehose-real-time-tolerance"),
				IrreversibleBlocksIndexStoreURL: indexStoreUrl,
				IrreversibleBlocksBundleSizes:   possibleIndexSizes,
			},
			&firehoseApp.Modules{
				Authenticator:         authenticator,
				HeadTimeDriftMetric:   headTimeDriftmetric,
				HeadBlockNumberMetric: headBlockNumMetric,
				Tracker:               tracker,
				TransformRegistry:     registry,
			}), nil
	}

	launcher.RegisterApp(&launcher.AppDef{
		ID:            "firehose",
		Title:         "Block Firehose",
		Description:   "Provides on-demand filtered blocks, depends on common-blocks-store-url and common-blockstream-addr",
		MetricsID:     "firehose",
		Logger:        launcher.NewLoggingDef("firehose.*", nil),
		RegisterFlags: registerFlags,
		FactoryFunc:   factoryFunc,
	})
}
