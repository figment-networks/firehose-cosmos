package transform

import (
	"fmt"
	"github.com/RoaringBitmap/roaring/roaring64"
	"strings"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/transform"
	"github.com/streamingfast/dstore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	pbtransform "github.com/figment-networks/proto-cosmos/pb/sf/cosmos/transform/v1"
	pbcosmos "github.com/figment-networks/proto-cosmos/pb/sf/cosmos/type/v1"
)

var CombinedFilterMessageName = proto.MessageName(&pbtransform.CombinedFilter{})

const IdxPrefixEmpty = ""         //
const IdxPrefixEventType = "ET"   // event type prefix for combined index
const IdxPrefixEventOrigin = "EO" // event origin prefix for combined index
const IdxPrefixMessageType = "MT" // message type prefix for combined index

const CombinedIndexerShortName = "combined"

func CombinedFilterFactory(indexStore dstore.Store, possibleIndexSizes []uint64) *transform.Factory {
	return &transform.Factory{
		Obj: &pbtransform.CombinedFilter{},
		NewFunc: func(message *anypb.Any) (transform.Transform, error) {
			if message.MessageName() != CombinedFilterMessageName {
				return nil, fmt.Errorf("expected type url %q, received %q", CombinedFilterMessageName, message.TypeUrl)
			}

			filter := &pbtransform.CombinedFilter{}
			err := proto.Unmarshal(message.Value, filter)
			if err != nil {
				return nil, fmt.Errorf("unexpected unmarshal error: %w", err)
			}

			if len(filter.EventOriginFilters) == 0 && len(filter.EventOriginFilters) == 0 && len(filter.MessageTypeFilters) == 0 {
				return nil, fmt.Errorf("a combined filter requires at least one Event Type, Event Origin or Message Type filter")
			}

			return newCombinedFilter(filter.EventTypeFilters, filter.EventOriginFilters, filter.MessageTypeFilters, indexStore, possibleIndexSizes)
		},
	}
}

func newCombinedFilter(pbEventTypeFilter []*pbtransform.EventTypeFilter, pbEventOriginFilter []*pbtransform.EventOriginFilter, pbMessageTypeFilter []*pbtransform.MessageTypeFilter, indexStore dstore.Store, possibleIndexSizes []uint64) (*CombinedFilter, error) {
	var eventTypeFilters []*EventTypeFilter
	if l := len(pbEventTypeFilter); l > 0 {
		eventTypeFilters = make([]*EventTypeFilter, l)
		for i, in := range pbEventTypeFilter {
			// TODO: this should call a new filter check in event_type_filter.go or similar
			f, err := NewEventTypeFilter(in)
			if err != nil {
				return nil, err
			}
			eventTypeFilters[i] = f
		}
	}

	var eventOriginFilters []*EventOriginFilter
	if l := len(pbEventOriginFilter); l > 0 {
		eventOriginFilters = make([]*EventOriginFilter, l)
		//for i, in := range pbEventOriginFilter {
		//	f, err := NewEventOriginFilter(in)
		//	if err != nil {
		//		return nil, err
		//	}
		//	eventOriginFilters[i] = f
		//}
	}

	var messageTypeFilters []*MessageTypeFilter
	if l := len(pbMessageTypeFilter); l > 0 {
		messageTypeFilters = make([]*MessageTypeFilter, l)
		//for i, in := range pbMessageTypeFilter {
		//	f, err := NewMessageTypeFilter(in)
		//	if err != nil {
		//		return nil, err
		//	}
		//	messageTypeFilters[i] = f
		//}
	}

	f := &CombinedFilter{
		EventTypeFilters:   eventTypeFilters,
		EventOriginFilters: eventOriginFilters,
		MessageTypeFilters: messageTypeFilters,
		indexStore:         indexStore,
		possibleIndexSizes: possibleIndexSizes,
	}

	return f, nil
}

type CombinedFilter struct {
	EventTypeFilters   []*EventTypeFilter
	EventOriginFilters []*EventOriginFilter
	MessageTypeFilters []*MessageTypeFilter

	indexStore         dstore.Store
	possibleIndexSizes []uint64
}

func (f *CombinedFilter) String() string {
	eventTypeFilters := make([]string, len(f.EventTypeFilters))
	for i, f := range f.EventTypeFilters {
		// TODO: wat - double check these
		eventTypeFilters[i] = f.String()
	}
	eventOriginFilters := make([]string, len(f.EventOriginFilters))
	for i, f := range f.EventOriginFilters {
		// TODO: wat - double check these
		eventOriginFilters[i] = f.String()
	}
	messageTypeFilters := make([]string, len(f.MessageTypeFilters))
	for i, f := range f.MessageTypeFilters {
		// TODO: wat - double check these
		messageTypeFilters[i] = f.String()
	}

	return fmt.Sprintf("Combined filter: Event Type Filters:[%s], Event Origin Filters:[%s], Message Type Filters:[%s]", strings.Join(eventTypeFilters, ","), strings.Join(eventOriginFilters, ","), strings.Join(messageTypeFilters, ","))
}

func (p *CombinedFilter) Transform(readOnlyBlk *bstream.Block, in transform.Input) (transform.Output, error) {
	block := readOnlyBlk.ToProtocol().(*pbcosmos.Block)

	// TODO: Is this transforming all blocks to remove transactions? Maybe we need
	// to check what type of filter here and strip whats needed?

	//for _, tx := range block.Transactions {
	//	tx.Tx.Body.Messages = p.(tx.Tx.Body.Messages)
	//}

	return block, nil
}

//func (p *CombinedFilter) filterCombinedFilters(messages []*anypb.Any) []*anypb.Any {
//	var outMessages []*anypb.Any
//
//	for _, message := range messages {
//		if p.MessageTypes[message.TypeUrl] {
//			outMessages = append(outMessages, message)
//		}
//	}
//
//	return outMessages
//}

func (f *CombinedFilter) GetIndexProvider() bstream.BlockIndexProvider {
	if f.indexStore == nil {
		return nil
	}

	if len(f.EventTypeFilters) == 0 && len(f.EventOriginFilters) == 0 && len(f.MessageTypeFilters) == 0 {
		return nil
	}

	return transform.NewGenericBlockIndexProvider(
		f.indexStore,
		CombinedIndexerShortName,
		f.possibleIndexSizes,
		getCombinedFilterFunc(f.EventTypeFilters, f.EventOriginFilters, f.MessageTypeFilters),
	)
}

func getCombinedFilterFunc(EventTypeFilters []*EventTypeFilter, EventOriginFilters []*EventOriginFilter, MessageTypeFilters []*MessageTypeFilter) func(transform.BitmapGetter) []uint64 {
	return func(getBitmap transform.BitmapGetter) (matchingBlocks []uint64) {
		out := roaring64.NewBitmap()
		for _, f := range EventTypeFilters {
			for e := range f.EventTypes {
				if bm := getBitmap(e); bm != nil {
					out.Or(bm)
				}
			}
		}
		for _, f := range EventOriginFilters {
			for e := range f.EventOrigins {
				if bm := getBitmap(string(e)); bm != nil {
					out.Or(bm)
				}
			}
		}
		for _, f := range MessageTypeFilters {
			for e := range f.MessageTypes {
				if bm := getBitmap(e); bm != nil {
					out.Or(bm)
				}
			}
		}

		return nilIfEmpty(out.ToArray())
	}
}
