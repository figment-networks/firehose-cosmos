package tools

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/figment-networks/firehose-cosmos/transform"
	pbcosmos "github.com/figment-networks/proto-cosmos/pb/sf/cosmos/type/v1"
	"github.com/spf13/cobra"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/stream"
	bstransform "github.com/streamingfast/bstream/transform"
	"github.com/streamingfast/dstore"
	firehose "github.com/streamingfast/firehose"
	pbfirehose "github.com/streamingfast/pbgo/sf/firehose/v1"
	"go.uber.org/zap"
)

var generateCombinedFilterIdxCmd = &cobra.Command{
	Use:   "generate-combined-filter-index {index-url} {irr-index-url} {source-blocks-url} {start-block-num} {stop-block-num}",
	Short: "Generate index files for all filters present in blocks",
	Args:  cobra.RangeArgs(4, 5),
	RunE:  generateCombinedFilterIdxE,
}

func init() {
	generateCombinedFilterIdxCmd.Flags().Uint64("first-streamable-block", 0, "first streamable block of this chain")
	generateCombinedFilterIdxCmd.Flags().Uint64("indexes-size", 10000, "size of index bundles that will be created")
	generateCombinedFilterIdxCmd.Flags().IntSlice("lookup-indexes-sizes", []int{1000000, 100000, 10000, 1000}, "index bundle sizes that we will look for on start to find first unindexed block (should include indexes-size)")
	generateCombinedFilterIdxCmd.Flags().IntSlice("irreversible-indexes-sizes", []int{10000, 1000}, "size of irreversible indexes that will be used")
	generateCombinedFilterIdxCmd.Flags().Bool("create-irreversible-indexes", false, "if true, irreversible indexes will also be created")

	Cmd.AddCommand(generateCombinedFilterIdxCmd)
}

func generateCombinedFilterIdxE(cmd *cobra.Command, args []string) error {
	createIrr, err := cmd.Flags().GetBool("create-irreversible-indexes")
	if err != nil {
		return err
	}
	bstream.GetProtocolFirstStreamableBlock, err = cmd.Flags().GetUint64("first-streamable-block")
	if err != nil {
		return err
	}

	iis, err := cmd.Flags().GetIntSlice("irreversible-indexes-sizes")
	if err != nil {
		return err
	}
	var irrIdxSizes []uint64
	for _, size := range iis {
		if size < 0 {
			return fmt.Errorf("invalid negative size for bundle-sizes: %d", size)
		}
		irrIdxSizes = append(irrIdxSizes, uint64(size))
	}

	idxSize, err := cmd.Flags().GetUint64("indexes-size")
	if err != nil {
		return err
	}
	lais, err := cmd.Flags().GetIntSlice("lookup-indexes-sizes")
	if err != nil {
		return err
	}
	var lookupIdxSizes []uint64
	for _, size := range lais {
		if size < 0 {
			return fmt.Errorf("invalid negative size for bundle-sizes: %d", size)
		}
		lookupIdxSizes = append(lookupIdxSizes, uint64(size))
	}

	indexStoreURL := args[0]
	irrIndexStoreURL := args[1]
	blocksStoreURL := args[2]
	startBlockNum, err := strconv.ParseUint(args[3], 10, 64)
	if err != nil {
		return fmt.Errorf("unable to parse block number %q: %w", args[0], err)
	}
	var stopBlockNum uint64
	if len(args) == 5 {
		stopBlockNum, err = strconv.ParseUint(args[4], 10, 64)
		if err != nil {
			return fmt.Errorf("unable to parse block number %q: %w", args[0], err)
		}
	}

	blocksStore, err := dstore.NewDBinStore(blocksStoreURL)
	if err != nil {
		return fmt.Errorf("failed setting up block store from url %q: %w", blocksStoreURL, err)
	}

	// we are optionally reading info from the irrIndexStore
	irrIndexStore, err := dstore.NewStore(irrIndexStoreURL, "", "", false)
	if err != nil {
		return fmt.Errorf("failed setting up irreversible blocks index store from url %q: %w", irrIndexStoreURL, err)
	}

	indexStore, err := dstore.NewStore(indexStoreURL, "", "", false)
	if err != nil {
		return fmt.Errorf("failed setting up an index store from url %q: %w", indexStoreURL, err)
	}

	streamFactory := firehose.NewStreamFactory(
		[]dstore.Store{blocksStore},
		irrIndexStore,
		irrIdxSizes,
		nil,
		nil,
		nil,
		nil,
	)
	cmd.SilenceUsage = true

	ctx := context.Background()

	var irrStart uint64
	done := make(chan struct{})
	go func() { // both checks in parallel
		irrStart = bstransform.FindNextUnindexed(ctx, uint64(startBlockNum), irrIdxSizes, "irr", irrIndexStore)
		close(done)
	}()
	idxStart := bstransform.FindNextUnindexed(ctx, uint64(startBlockNum), lookupIdxSizes, transform.MessageTypeIndexShortName, indexStore)
	<-done

	if irrStart < idxStart {
		startBlockNum = irrStart
	} else {
		startBlockNum = idxStart
	}

	zlog.Info("resolved next unindexed regions", zap.Uint64("index_start", idxStart), zap.Uint64("irreversible_start", irrStart), zap.Uint64("resolved_start", startBlockNum))
	var irreversibleIndexer *bstransform.IrreversibleBlocksIndexer
	if createIrr {
		irreversibleIndexer = bstransform.NewIrreversibleBlocksIndexer(irrIndexStore, irrIdxSizes, bstransform.IrrWithDefinedStartBlock(startBlockNum))
	}

	t := transform.NewCombinedFilterIndexer(indexStore, idxSize, startBlockNum)

	handler := bstream.HandlerFunc(func(blk *bstream.Block, obj interface{}) error {
		if createIrr {
			irreversibleIndexer.Add(blk)
		}
		t.ProcessBlock(blk.ToProtocol().(*pbcosmos.Block))
		return nil
	})

	req := &pbfirehose.Request{
		StartBlockNum: int64(startBlockNum),
		StopBlockNum:  stopBlockNum,
		ForkSteps:     []pbfirehose.ForkStep{pbfirehose.ForkStep_STEP_IRREVERSIBLE},
	}

	s, err := streamFactory.New(
		ctx,
		handler,
		req,
		zlog,
	)
	if err != nil {
		return fmt.Errorf("getting firehose stream: %w", err)
	}

	if err := s.Run(ctx); err != nil {
		if !errors.Is(err, stream.ErrStopBlockReached) {
			return err
		}
	}
	zlog.Info("complete")
	return nil
}
