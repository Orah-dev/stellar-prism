package stream

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// StateVersion is the on-disk schema version this build writes. A reader
// refuses any other version rather than guessing at unknown fields, so a file
// written by a newer prism fails loudly instead of resuming from a
// misinterpreted position.
const StateVersion = 1

// State is the durable position of a stream: everything needed to resume
// where a previous run stopped, and nothing else. It is stored as one small
// JSON document so that a human can read it and a diff can explain it.
type State struct {
	// Version is the schema version; written as StateVersion.
	Version int `json:"version"`
	// Source names the stream that wrote the state ("events", "ledgers",
	// ...). A position from one stream is meaningless to another, so it is
	// recorded and checked rather than assumed.
	Source string `json:"source"`
	// Ledger is the sequence of the last successfully emitted record.
	Ledger uint32 `json:"ledger"`
	// PagingToken resumes the stream immediately after the last emitted
	// record. It is the server's own cursor format: the emitted record's ID
	// for events, refined to the batch cursor once a page has drained.
	PagingToken string `json:"paging_token"`
	// Cursor is an alias for PagingToken.
	Cursor string `json:"cursor,omitempty"`
	// LastID is the highest Record.ID emitted, so the tailer's
	// de-duplication high-water mark survives a restart and any replayed
	// page is still suppressed.
	LastID string `json:"last_id"`
	// UpdatedAt is when the position was last persisted.
	UpdatedAt time.Time `json:"updated_at"`
}

// StateFile persists a State as a JSON file, replacing it atomically.
//
// Each Save writes a complete snapshot to a temporary file in the
// destination directory, fsyncs it, and renames it over the destination, so
// a concurrent reader only ever observes a complete snapshot — either the
// previous state or the new one, never a torn file. If a crash lands between
// the data sync and the rename, the destination reverts to the previous
// position; that can only re-emit records on the next run, never skip them,
// so the at-least-once delivery guarantee holds unconditionally.
//
// A StateFile holds no open file descriptors between operations and spawns
// no goroutines, so there is nothing to clean up when a run terminates: the
// tailer's single goroutine calls it inline, synchronously, after each
// successfully emitted record.
type StateFile struct {
	path string

	// mu serialises Save, so two tailers pointed at the same file cannot
	// interleave writes and lose one update's result.
	mu sync.Mutex
}

// OpenStateFile prepares path for use as a stream position file, creating
// the parent directory if needed.
//
// An existing file is read and validated immediately, so a corrupt or
// version-incompatible state file fails here — before any streaming or RPC
// work starts — rather than in the middle of a run. A missing file is not an
// error: it simply means nothing has been persisted yet.
func OpenStateFile(path string) (*StateFile, error) {
	if path == "" {
		return nil, errors.New("state file path is empty")
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating state file directory %q: %w", dir, err)
	}
	s := &StateFile{path: path}
	if _, err := s.Load(); err != nil {
		return nil, err
	}
	return s, nil
}

// Path reports the file this StateFile persists to.
func (s *StateFile) Path() string { return s.path }

// Rewind atomically replaces the saved position with target and keeps a
// timestamped backup of the previous complete state. Cursor and high-water
// identifiers are cleared so replayed records are not de-duplicated away.
func (s *StateFile) Rewind(target uint32) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, err := s.Load()
	if err != nil {
		return "", err
	}
	if current.Source == "" {
		return "", errors.New("cannot rewind an empty state file")
	}
	data, err := os.ReadFile(s.path)
	if err != nil {
		return "", fmt.Errorf("reading state backup: %w", err)
	}
	backup := fmt.Sprintf("%s.backup-%d", s.path, time.Now().UTC().UnixNano())
	if err := os.WriteFile(backup, data, 0o600); err != nil {
		return "", fmt.Errorf("writing state backup: %w", err)
	}
	current.Ledger, current.PagingToken, current.Cursor, current.LastID = target, "", "", ""
	current.UpdatedAt = time.Now().UTC()
	current.Version = StateVersion
	encoded, err := json.Marshal(current)
	if err != nil {
		return "", err
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.path), filepath.Base(s.path)+".rewind-*")
	if err != nil {
		return "", err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if _, err = tmp.Write(encoded); err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return "", err
	}
	if err = os.Rename(name, s.path); err != nil {
		return "", err
	}
	return backup, nil
}

// Load reads the persisted state.
//
// A missing or empty file yields the zero State, which the tailer reads as
// "nothing persisted yet": the stream starts from wherever it was configured
// to start. A file that exists but cannot be read, parsed or understood is
// reported as an error wrapping the underlying cause — silently starting
// over would re-emit the whole stream, which is exactly what durable
// cursors exist to prevent.
func (s *StateFile) Load() (State, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, fs.ErrNotExist) {
		return State{}, nil
	}
	if err != nil {
		return State{}, fmt.Errorf("reading state file %q: %w", s.path, err)
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return State{}, nil
	}
	var st State
	if err := json.Unmarshal(data, &st); err != nil {
		return State{}, fmt.Errorf("state file %q is not valid JSON: %w", s.path, err)
	}
	if st.Version != StateVersion {
		return State{}, fmt.Errorf(
			"state file %q has schema version %d, but this build reads version %d",
			s.path, st.Version, StateVersion)
	}
	if st.PagingToken == "" && st.Cursor != "" {
		st.PagingToken = st.Cursor
	}
	if st.Cursor == "" && st.PagingToken != "" {
		st.Cursor = st.PagingToken
	}
	return st, nil
}

// Save atomically replaces the persisted state.
//
// It is called by the tailer after each successfully emitted record, which
// is what narrows the restart window from "the whole run" to "one record":
// the file can never run ahead of the stream, because it is only written
// once the record it describes has been handed to the sink.
func (s *StateFile) Save(st State) error {
	if st.Source == "" {
		return errors.New("stream state must record which stream it belongs to")
	}
	st.Version = StateVersion
	data, err := json.Marshal(st)
	if err != nil {
		return fmt.Errorf("encoding stream state: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, filepath.Base(s.path)+".tmp*")
	if err != nil {
		return fmt.Errorf("creating temporary file next to %q: %w", s.path, err)
	}
	tmpName := tmp.Name()
	// Runs only when the rename below did not consume the temporary file,
	// i.e. when Save is already failing. A leftover temp file would be
	// harmless, but removing it keeps the directory honest.
	defer func() {
		if tmpName != "" {
			os.Remove(tmpName)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("writing state to %q: %w", tmpName, err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("syncing state to %q: %w", tmpName, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing state file %q: %w", tmpName, err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		return fmt.Errorf("replacing state file %q: %w", s.path, err)
	}
	tmpName = ""
	return nil
}
