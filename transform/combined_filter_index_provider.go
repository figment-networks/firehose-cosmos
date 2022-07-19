package transform

import (
	"github.com/streamingfast/bstream/transform"
	"github.com/streamingfast/dstore"
)

const CombinedFilterIndexShortName = "combinedfilter"

func NewCombinedFilterIndexProvider(
	store dstore.Store,
	possibleIndexSizes []uint64,
	messageTypes map[string]bool,
) *transform.GenericBlockIndexProvider {
	return transform.NewGenericBlockIndexProvider(
		store,
		CombinedFilterIndexShortName,
		possibleIndexSizes,
		getFilterFunc(messageTypes),
	)
}
