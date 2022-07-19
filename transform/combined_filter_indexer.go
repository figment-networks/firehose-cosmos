package transform

import (
	pbcosmos "github.com/figment-networks/proto-cosmos/pb/sf/cosmos/type/v1"
	"github.com/streamingfast/bstream/transform"
	"github.com/streamingfast/dstore"
)

type CombinedFilterIndexer struct {
	BlockIndexer blockIndexer
}

func NewCombinedFilterIndexer(indexStore dstore.Store, indexSize uint64, startBlock uint64) *CombinedFilterIndexer {
	bi := transform.NewBlockIndexer(
		indexStore,
		indexSize,
		CombinedFilterIndexShortName,
		transform.WithDefinedStartBlock(startBlock),
	)

	return &CombinedFilterIndexer{
		BlockIndexer: bi,
	}
}

func (i *CombinedFilterIndexer) ProcessBlock(block *pbcosmos.Block) {
	keyMap := make(map[string]bool)

	// TODO: dig into this
	for _, tx := range block.Transactions {
		processMessages(keyMap, tx.Tx.Body.Messages)
	}

	var keys []string
	for key := range keyMap {
		keys = append(keys, key)
	}

	i.BlockIndexer.Add(keys, block.Header.Height)
}

//func processMessages(keyMap map[string]bool, messages []*anypb.Any) {
//	for _, message := range messages {
//		keyMap[message.TypeUrl] = true
//	}
//}
