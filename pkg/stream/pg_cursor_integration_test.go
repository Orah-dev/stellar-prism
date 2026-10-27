package stream_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // the driver --pg-dsn uses in production

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// This file covers the tailer and the PostgreSQL store together: what a
// restart resumes, what it must not replay, and what happens when persistence
// itself fails. Everything except the final test runs offline against the
// in-memory driver.

// pgTestMemoryStore is a CursorStore that records what it was asked to keep, for
// asserting tailer behaviour without a SQL layer in the way.
type pgTestMemoryStore struct {
	mu       sync.Mutex
	state    stream.CursorState
	saved    bool
	saves    []stream.CursorState
	loadKeys []string
	saveKeys []string

	loadErr error
	saveErr error
	// saveErrOn makes the nth Save (1-based) fail; zero never fails.
	saveErrOn int
}

func (m *pgTestMemoryStore) Load(_ context.Context, key string) (stream.CursorState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.loadKeys = append(m.loadKeys, key)
	if m.loadErr != nil {
		return stream.CursorState{}, m.loadErr
	}
	if !m.saved {
		return stream.CursorState{}, fmt.Errorf("cursor %q: %w", key, stream.ErrNoCursor)
	}
	return m.state, nil
}

func (m *pgTestMemoryStore) Save(_ context.Context, key string, state stream.CursorState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.saves)+1 == m.saveErrOn {
		return m.saveErr
	}
	m.saveKeys = append(m.saveKeys, key)
	m.saves = append(m.saves, state)
	m.state, m.saved = state, true
	return nil
}

func (m *pgTestMemoryStore) recorded() (saves []stream.CursorState, loadKeys, saveKeys []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]stream.CursorState(nil), m.saves...),
		append([]string(nil), m.loadKeys...),
		append([]string(nil), m.saveKeys...)
}

// runPersistentTail builds a tailer over srv with the given options and runs
// it to completion.
func runPersistentTail(t *testing.T, srv *fakerpc.Server, opts stream.Options, q stream.EventQuery) (*stream.Tailer, *collector, error) {
	t.Helper()

	src, err := stream.NewEventSource(srv, q)
	if err != nil {
		t.Fatalf("NewEventSource() error = %v", err)
	}
	sink := &collector{}
	if opts.Clock == nil {
		opts.Clock = newFakeClock()
	}
	tl := stream.NewTailer(src, sink, opts)
	runErr := tl.Run(context.Background())
	return tl, sink, runErr
}

// TestTailPersistsPositionAfterEveryRecord pins the durability contract the
// state machine now makes: one Save per emitted record (at the cursor the
// record was fetched from) plus one per page boundary, and never a resume
// point that sits beyond what the paired LastID says was emitted.
//
// If persistence ever regresses to "once per page", the Save count and the
// Cursor <= LastID invariant below both fail.
func TestTailPersistsPositionAfterEveryRecord(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(100, 20)
	srv := fakerpc.NewServer(events, fakerpc.WithPageLimit(5))
	store := &pgTestMemoryStore{}

	tl, sink, err := runPersistentTail(t, srv, stream.Options{
		Store:    store,
		StoreKey: "events/persist",
	}, stream.EventQuery{StartLedger: 100, PageLimit: 5})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	assertStreamInvariants(t, sink.ids, idsOf(events))

	saves, _, saveKeys := store.recorded()
	// 20 records, 4 pages of 5: one Save per record plus one per page
	// boundary.
	if want := len(events) + len(events)/5; len(saves) != want {
		t.Fatalf("recorded %d Save(s), want %d", len(saves), want)
	}
	for i, key := range saveKeys {
		if key != "events/persist" {
			t.Fatalf("Save #%d used key %q, want %q", i, key, "events/persist")
		}
	}

	lastID := ""
	for i, st := range saves {
		if st.Cursor != "" && st.Cursor > st.LastID {
			t.Errorf("Save #%d = %+v: resume point is past the emitted high-water mark", i, st)
		}
		if i > 0 && st.Cursor < saves[i-1].Cursor {
			t.Errorf("Save #%d cursor %q moved backwards from %q", i, st.Cursor, saves[i-1].Cursor)
		}
		if st.LastID < lastID {
			t.Errorf("Save #%d LastID %q went backwards from %q", i, st.LastID, lastID)
		}
		lastID = st.LastID
	}

	final := saves[len(saves)-1]
	if final.Cursor != tl.Cursor() {
		t.Errorf("final persisted cursor = %q, want the tailer's %q", final.Cursor, tl.Cursor())
	}
	if final.LastID != events[len(events)-1].ID {
		t.Errorf("final persisted LastID = %q, want the last record %q", final.LastID, events[len(events)-1].ID)
	}
}

// TestTailResumesAcrossRestartWithoutGapOrDuplicate is the end-to-end claim:
// run one stops part-way through a page, run two picks up from the persisted
// position, and the union of both runs is exactly the stream — no gap, no
// replay — even though both runs went through a real *sql.DB.
func TestTailResumesAcrossRestartWithoutGapOrDuplicate(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(100, 20)
	srv := fakerpc.NewServer(events, fakerpc.WithPageLimit(5))
	store, f := newTestPGStore(t, stream.PGStoreConfig{})

	// First run: stop after 7 records, i.e. mid-page and after the crash the
	// process never reached the page boundary.
	_, first, err := runPersistentTail(t, srv, stream.Options{
		Limit:    7,
		Store:    store,
		StoreKey: "events/resume",
		Clock:    newFakeClock(),
	}, stream.EventQuery{StartLedger: 100, PageLimit: 5})
	if err != nil {
		t.Fatalf("first Run() error = %v", err)
	}
	if len(first.ids) != 7 {
		t.Fatalf("first run emitted %d record(s), want 7", len(first.ids))
	}

	// Second run: a fresh tailer over the same store, as a restart would be.
	_, second, err := runPersistentTail(t, srv, stream.Options{
		Store:    store,
		StoreKey: "events/resume",
	}, stream.EventQuery{StartLedger: 100, PageLimit: 5})
	if err != nil {
		t.Fatalf("second Run() error = %v", err)
	}

	combined := append(append([]string(nil), first.ids...), second.ids...)
	assertStreamInvariants(t, combined, idsOf(events))
	if got := len(second.ids); got != len(events)-7 {
		t.Errorf("second run emitted %d record(s), want the %d remaining", got, len(events)-7)
	}

	// The persisted position must be exactly where the stream got to: the
	// final page's cursor, which is the last record's ID.
	state, err := store.Load(context.Background(), "events/resume")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	wantCursor := events[len(events)-1].ID
	if state.Cursor != wantCursor {
		t.Errorf("persisted cursor = %q, want %q", state.Cursor, wantCursor)
	}
	if state.LastID != events[len(events)-1].ID {
		t.Errorf("persisted LastID = %q, want %q", state.LastID, events[len(events)-1].ID)
	}
	// And the whole path must really have gone through the row lock.
	if !statementsContain(f.statementsLog(), "FOR UPDATE") {
		t.Error("no SELECT ... FOR UPDATE was issued; the row lock is not being taken")
	}
}

// TestTailRestoredPositionSuppressesReplay covers the other direction: a row
// written by a previous process (the crash case) is honoured before the first
// fetch, so nothing at or before the saved high-water mark is emitted again.
func TestTailRestoredPositionSuppressesReplay(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(100, 20)
	srv := fakerpc.NewServer(events, fakerpc.WithPageLimit(5))
	store, _ := newTestPGStore(t, stream.PGStoreConfig{})

	// As if run one had emitted seven records and died before finishing the
	// page: resume from the end of page one, high-water mark at record seven.
	seed := stream.CursorState{Cursor: events[4].ID, LastID: events[6].ID}
	if err := store.Save(context.Background(), "events/seed", seed); err != nil {
		t.Fatalf("seeding the store: %v", err)
	}

	_, sink, err := runPersistentTail(t, srv, stream.Options{
		Store:    store,
		StoreKey: "events/seed",
	}, stream.EventQuery{StartLedger: 100, PageLimit: 5})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	// Everything after the saved high-water mark, and nothing before it.
	assertStreamInvariants(t, sink.ids, idsOf(events)[7:])
}

func TestTailFirstRunWithNoSavedCursorStartsAsConfigured(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(100, 8)
	srv := fakerpc.NewServer(events, fakerpc.WithPageLimit(4))
	store := &pgTestMemoryStore{}

	_, sink, err := runPersistentTail(t, srv, stream.Options{
		Store:    store,
		StoreKey: "events/fresh",
	}, stream.EventQuery{StartLedger: 100, PageLimit: 4})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	assertStreamInvariants(t, sink.ids, idsOf(events))

	_, loadKeys, _ := store.recorded()
	if len(loadKeys) != 1 || loadKeys[0] != "events/fresh" {
		t.Errorf("Load keys = %q, want exactly one lookup of %q", loadKeys, "events/fresh")
	}
}

func TestTailStoreKeyDefaultsToTheSourceName(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(100, 4)
	srv := fakerpc.NewServer(events, fakerpc.WithPageLimit(4))
	store := &pgTestMemoryStore{}

	_, _, err := runPersistentTail(t, srv, stream.Options{Store: store},
		stream.EventQuery{StartLedger: 100, PageLimit: 4})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	_, loadKeys, saveKeys := store.recorded()
	for _, keys := range [][]string{loadKeys, saveKeys} {
		for _, k := range keys {
			if k != "events" {
				t.Errorf("key = %q, want the Source name %q when StoreKey is empty", k, "events")
			}
		}
	}
}

// TestTailRestoreFailureStopsBeforeFetching: if the position cannot be read,
// continuing would silently start from the wrong place, so the run must fail
// before touching the stream.
func TestTailRestoreFailureStopsBeforeFetching(t *testing.T) {
	t.Parallel()

	srv := fakerpc.NewServer(fakerpc.MakeEvents(100, 4), fakerpc.WithPageLimit(4))
	store := &pgTestMemoryStore{loadErr: errors.New("connection refused")}

	_, sink, err := runPersistentTail(t, srv, stream.Options{
		Store:    store,
		StoreKey: "events/broken",
	}, stream.EventQuery{StartLedger: 100, PageLimit: 4})
	if !errors.Is(err, store.loadErr) {
		t.Fatalf("Run() error = %v, want it to wrap %v", err, store.loadErr)
	}
	if len(sink.ids) != 0 {
		t.Errorf("emitted %d record(s) despite an unreadable cursor, want 0", len(sink.ids))
	}
	if got := srv.Calls(); got != 0 {
		t.Errorf("made %d RPC call(s) despite an unreadable cursor, want 0", got)
	}
}

// TestTailPersistFailureStopsTheStream: once durability is promised, a Save
// that fails must stop the run rather than continue in a state the operator
// believes is persisted. The resume point must stay where it was safe.
func TestTailPersistFailureStopsTheStream(t *testing.T) {
	t.Parallel()

	srv := fakerpc.NewServer(fakerpc.MakeEvents(100, 10), fakerpc.WithPageLimit(5))
	store := &pgTestMemoryStore{saveErrOn: 3, saveErr: errors.New("database is gone")}

	tl, sink, err := runPersistentTail(t, srv, stream.Options{
		Store:    store,
		StoreKey: "events/persist-failure",
	}, stream.EventQuery{StartLedger: 100, PageLimit: 5})
	if !errors.Is(err, store.saveErr) {
		t.Fatalf("Run() error = %v, want it to wrap %v", err, store.saveErr)
	}
	if !strings.Contains(err.Error(), "persisting cursor") {
		t.Errorf("error = %q, want it to name the persistence failure", err)
	}
	// Three records were emitted; the third's Save failed, so the run stopped
	// there with only two positions durable.
	if got := len(sink.ids); got != 3 {
		t.Errorf("emitted %d record(s), want 3 — the one whose Save failed plus the two persisted", got)
	}
	if got := tl.Cursor(); got != "" {
		t.Errorf("Cursor() = %q, want the page boundary to be untouched after a mid-page failure", got)
	}
	saves, _, _ := store.recorded()
	if len(saves) != 2 {
		t.Errorf("persisted %d state(s), want 2", len(saves))
	}
}

// TestTailCancellationDuringPersistIsACleanStop: Ctrl-C in the middle of a
// Save is an interrupted run, not a crash — the same contract cancellation has
// everywhere else in the tailer.
func TestTailCancellationDuringPersistIsACleanStop(t *testing.T) {
	t.Parallel()

	srv := fakerpc.NewServer(fakerpc.MakeEvents(100, 4), fakerpc.WithPageLimit(4))
	store := &pgTestMemoryStore{saveErrOn: 1, saveErr: context.Canceled}

	_, sink, err := runPersistentTail(t, srv, stream.Options{
		Store:    store,
		StoreKey: "events/cancel",
	}, stream.EventQuery{StartLedger: 100, PageLimit: 4})
	if err != nil {
		t.Fatalf("Run() after cancellation = %v, want nil (a clean stop)", err)
	}
	if got := len(sink.ids); got != 1 {
		t.Errorf("emitted %d record(s), want the 1 written before the interruption", got)
	}
}

// TestTailWithoutStoreIsUnchanged guards the default path: with no store
// configured nothing is loaded, nothing is saved, and behaviour is exactly
// what it was before durable cursors existed.
func TestTailWithoutStoreIsUnchanged(t *testing.T) {
	t.Parallel()

	events := fakerpc.MakeEvents(100, 10)
	srv := fakerpc.NewServer(events, fakerpc.WithPageLimit(4))

	_, sink, err := runPersistentTail(t, srv, stream.Options{},
		stream.EventQuery{StartLedger: 100, PageLimit: 4})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	assertStreamInvariants(t, sink.ids, idsOf(events))
}

func statementsContain(stmts []string, fragment string) bool {
	for _, s := range stmts {
		if strings.Contains(s, fragment) {
			return true
		}
	}
	return false
}

// TestPGCursorStoreAgainstRealPostgreSQL is the real-world integration test.
// It needs an actual PostgreSQL server and is skipped unless PRISM_TEST_PG_DSN
// is set, because the committed suite must run with no network at all:
//
//	PRISM_TEST_PG_DSN='postgres://user:pass@127.0.0.1:5432/prism_test?sslmode=disable' \
//	  go test -race -run TestPGCursorStoreAgainstRealPostgreSQL ./pkg/stream/
//
// What it proves that an in-memory fake cannot: that SELECT ... FOR UPDATE
// really does block a second writer until the first transaction ends, and
// that concurrent first-writers neither collide nor lose a position.
func TestPGCursorStoreAgainstRealPostgreSQL(t *testing.T) {
	dsn := os.Getenv("PRISM_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("PRISM_TEST_PG_DSN is not set; the PostgreSQL integration test needs a live server")
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	db.SetMaxOpenConns(4)
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("closing the database: %v", err)
		}
	}()

	// A table unique to this run, so parallel CI jobs sharing a server cannot
	// collide and a leftover from a killed run is never reused.
	table := fmt.Sprintf("prism_cursors_test_%d", time.Now().UnixNano())
	store, err := stream.NewPGCursorStore(db, stream.PGStoreConfig{
		Table:       table,
		LockTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewPGCursorStore() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatalf("EnsureSchema() error = %v", err)
	}
	defer func() {
		if _, err := db.ExecContext(context.Background(),
			`DROP TABLE IF EXISTS "`+table+`"`); err != nil {
			t.Errorf("dropping %s: %v", table, err)
		}
	}()

	key := "events/real-db"

	t.Run("round trip", func(t *testing.T) {
		if _, err := store.Load(ctx, key); !errors.Is(err, stream.ErrNoCursor) {
			t.Fatalf("Load() before any save = %v, want ErrNoCursor", err)
		}
		want := stream.CursorState{
			Cursor: "3966812-0000000000",
			LastID: "0017037362169118720-0000000000",
		}
		if err := store.Save(ctx, key, want); err != nil {
			t.Fatalf("Save() error = %v", err)
		}
		got, err := store.Load(ctx, key)
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if got.Cursor != want.Cursor || got.LastID != want.LastID {
			t.Errorf("Load() = %+v, want %+v", got, want)
		}
		if got.UpdatedAt.IsZero() {
			t.Error("Load().UpdatedAt is zero, want a written timestamp")
		}
	})

	t.Run("row lock blocks a concurrent save until commit", func(t *testing.T) {
		holder, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("BeginTx() error = %v", err)
		}
		var held string
		if err := holder.QueryRowContext(ctx,
			`SELECT name FROM "`+table+`" WHERE name = $1 FOR UPDATE`, key).Scan(&held); err != nil {
			t.Fatalf("SELECT ... FOR UPDATE error = %v", err)
		}

		later := stream.CursorState{Cursor: "3966813-0000000000", LastID: "0017037362169118721-0000000000"}
		done := make(chan error, 1)
		go func() {
			done <- store.Save(context.Background(), key, later)
		}()

		select {
		case err := <-done:
			t.Fatalf("Save() returned %v while another transaction held the row lock", err)
		case <-time.After(400 * time.Millisecond):
			// Blocked, which is what FOR UPDATE is for.
		}

		if err := holder.Rollback(); err != nil {
			t.Fatalf("Rollback() error = %v", err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Save() after the lock was released = %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("Save() never resumed after the lock was released")
		}

		got, err := store.Load(ctx, key)
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if got.Cursor != later.Cursor {
			t.Errorf("Load().Cursor = %q, want the later writer's %q", got.Cursor, later.Cursor)
		}
	})

	t.Run("concurrent first writers neither collide nor lose a position", func(t *testing.T) {
		const writers = 8
		shared := "events/real-db-contended"
		var wg sync.WaitGroup
		errs := make([]error, writers)
		for i := range writers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs[i] = store.Save(ctx, shared, stream.CursorState{
					Cursor: fmt.Sprintf("cursor-%02d", i),
					LastID: fmt.Sprintf("last-%02d", i),
				})
			}()
		}
		wg.Wait()

		for i, err := range errs {
			if err != nil {
				t.Errorf("Save() #%d error = %v — the insert race was not resolved", i, err)
			}
		}
		got, err := store.Load(ctx, shared)
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		// The survivor must be one writer's complete pair.
		suffix := strings.TrimPrefix(got.Cursor, "cursor-")
		if suffix == got.Cursor || got.LastID != "last-"+suffix {
			t.Errorf("row = %+v, want a complete state from a single writer", got)
		}
	})

	t.Run("tailer resumes after a restart", func(t *testing.T) {
		events := fakerpc.MakeEvents(100, 20)
		srv := fakerpc.NewServer(events, fakerpc.WithPageLimit(5))
		key := "events/real-db-resume"

		_, first, err := runPersistentTail(t, srv, stream.Options{
			Limit:    7,
			Store:    store,
			StoreKey: key,
		}, stream.EventQuery{StartLedger: 100, PageLimit: 5})
		if err != nil {
			t.Fatalf("first Run() error = %v", err)
		}

		_, second, err := runPersistentTail(t, srv, stream.Options{
			Store:    store,
			StoreKey: key,
		}, stream.EventQuery{StartLedger: 100, PageLimit: 5})
		if err != nil {
			t.Fatalf("second Run() error = %v", err)
		}

		combined := append(append([]string(nil), first.ids...), second.ids...)
		assertStreamInvariants(t, combined, idsOf(events))
	})
}
