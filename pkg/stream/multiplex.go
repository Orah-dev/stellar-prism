package stream

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
)

// MultiplexSource merges multiple sources into a single ordered stream.
// It uses a min-heap to merge records by their ID (which is lexicographically ordered).
type MultiplexSource struct {
	sources []Source
	heap    *sourceHeap
	mu      sync.Mutex
	fetched map[int]Batch
	cursors map[int]string
	closed  bool
}

type sourceHeap struct {
	items []heapItem
}

type heapItem struct {
	record    Record
	sourceIdx int
	batchIdx  int
}

func (h *sourceHeap) Len() int { return len(h.items) }

func (h *sourceHeap) Less(i, j int) bool {
	return h.items[i].record.ID < h.items[j].record.ID
}

func (h *sourceHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *sourceHeap) Push(x interface{}) {
	// x is guaranteed to be heapItem by heap.Push caller
	if item, ok := x.(heapItem); ok {
		h.items = append(h.items, item)
	}
}

func (h *sourceHeap) Pop() interface{} {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[0 : n-1]
	// item is guaranteed to be heapItem by heap.Pop caller
	return item
}

func NewMultiplexSource(sources ...Source) *MultiplexSource {
	return &MultiplexSource{
		sources: sources,
		heap:    &sourceHeap{items: make([]heapItem, 0)},
		fetched: make(map[int]Batch),
		cursors: make(map[int]string),
	}
}

func (m *MultiplexSource) Name() string {
	names := make([]string, len(m.sources))
	for i, s := range m.sources {
		names[i] = s.Name()
	}
	return "multiplex(" + fmt.Sprintf("%v", names) + ")"
}

func (m *MultiplexSource) Fetch(ctx context.Context, cursor string) (Batch, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return Batch{}, fmt.Errorf("multiplex source closed")
	}

	// If we have a merged cursor, distribute it to all sources
	// For now, we treat the cursor as applying to all sources equally
	// In a real implementation, each source might need its own cursor

	// Fetch from all sources if we don't have data
	for i, src := range m.sources {
		if _, ok := m.fetched[i]; !ok {
			batch, err := src.Fetch(ctx, m.cursors[i])
			if err != nil {
				return Batch{}, err
			}
			m.fetched[i] = batch
			if batch.NextCursor != "" {
				m.cursors[i] = batch.NextCursor
			}
		}
	}

	// Initialize heap with first record from each source
	for i := range m.sources {
		if batch, ok := m.fetched[i]; ok && len(batch.Records) > 0 {
			heap.Push(m.heap, heapItem{
				record:    batch.Records[0],
				sourceIdx: i,
				batchIdx:  0,
			})
		}
	}

	// Merge records in order
	var mergedRecords []Record
	for m.heap.Len() > 0 {
		// item is guaranteed to be heapItem by heap.Pop
		if item, ok := heap.Pop(m.heap).(heapItem); ok {
			mergedRecords = append(mergedRecords, item.record)

			// Push next record from same source
			batch := m.fetched[item.sourceIdx]
			nextIdx := item.batchIdx + 1
			if nextIdx < len(batch.Records) {
				heap.Push(m.heap, heapItem{
					record:    batch.Records[nextIdx],
					sourceIdx: item.sourceIdx,
					batchIdx:  nextIdx,
				})
			}
		}
	}

	// Clear fetched batches since we've consumed them
	m.fetched = make(map[int]Batch)

	// Determine overall Latest and Oldest
	var latest, oldest uint32
	for _, batch := range m.fetched {
		if batch.Latest > latest {
			latest = batch.Latest
		}
		if oldest == 0 || (batch.Oldest > 0 && batch.Oldest < oldest) {
			oldest = batch.Oldest
		}
	}

	// For now, use the last cursor as the next cursor
	var nextCursor string
	for _, c := range m.cursors {
		if c != "" {
			nextCursor = c
			break
		}
	}

	return Batch{
		Records:    mergedRecords,
		NextCursor: nextCursor,
		Latest:     latest,
		Oldest:     oldest,
		Full:       false,
	}, nil
}

func (m *MultiplexSource) CursorLedger(cursor string) (uint32, bool) {
	// For multiplex, we can't easily determine the ledger from a merged cursor
	// Delegate to the first source
	if len(m.sources) > 0 {
		return m.sources[0].CursorLedger(cursor)
	}
	return 0, false
}

// Close releases any resources held by the multiplexer.
func (m *MultiplexSource) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}
