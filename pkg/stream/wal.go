package stream

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// WAL durably journals records before sink delivery. Replay returns records
// left by a process that died before Commit.
type WAL struct {
	path string
	mu   sync.Mutex
}

// OpenWAL opens the single append-only journal in dir.
func OpenWAL(dir string) (*WAL, error) {
	if dir == "" {
		return nil, fmt.Errorf("wal directory is empty")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &WAL{path: filepath.Join(dir, "pending.jsonl")}, nil
}

// Append fsyncs r before it is delivered to a sink.
func (w *WAL) Append(r Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := json.NewEncoder(f).Encode(r); err != nil {
		return err
	}
	return f.Sync()
}

// Replay returns every uncommitted record in journal order.
func (w *WAL) Replay() ([]Record, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	f, err := os.Open(w.path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var records []Record
	decoder := json.NewDecoder(bufio.NewReader(f))
	for decoder.More() {
		var r Record
		if err := decoder.Decode(&r); err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, nil
}

// Commit clears records only after the downstream sink has flushed.
func (w *WAL) Commit() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return os.WriteFile(w.path, nil, 0o600)
}
