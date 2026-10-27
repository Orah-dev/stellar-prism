package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

// ObjectStore is the conditional object-storage seam used by S3 and GCS
// adapters. Generation is an ETag for S3 or object generation for GCS.
type ObjectStore interface {
	Get(context.Context, string) ([]byte, string, error)
	Put(context.Context, string, []byte, string) (string, error)
}

// CloudCursorStore persists one cursor per object key with conditional writes.
type CloudCursorStore struct {
	store    ObjectStore
	prefix   string
	mu       sync.Mutex
	versions map[string]string
}

func NewCloudCursorStore(store ObjectStore, prefix string) (*CloudCursorStore, error) {
	if store == nil {
		return nil, fmt.Errorf("cloud state store is nil")
	}
	return &CloudCursorStore{store: store, prefix: prefix, versions: map[string]string{}}, nil
}
func (s *CloudCursorStore) object(key string) string { return s.prefix + key + ".json" }
func (s *CloudCursorStore) Load(ctx context.Context, key string) (CursorState, error) {
	data, version, err := s.store.Get(ctx, s.object(key))
	if err != nil {
		return CursorState{}, err
	}
	var state CursorState
	if err := json.Unmarshal(data, &state); err != nil {
		return CursorState{}, err
	}
	s.mu.Lock()
	s.versions[key] = version
	s.mu.Unlock()
	return state, nil
}
func (s *CloudCursorStore) Save(ctx context.Context, key string, state CursorState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	s.mu.Lock()
	version := s.versions[key]
	s.mu.Unlock()
	next, err := s.store.Put(ctx, s.object(key), data, version)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.versions[key] = next
	s.mu.Unlock()
	return nil
}
