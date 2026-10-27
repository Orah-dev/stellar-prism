package stream_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// This file exercises PGCursorStore through a real *sql.DB, so transaction
// handling, row scanning, rollback and context cancellation behave exactly as
// they do in production — only the wire is replaced by an in-memory driver.
// Nothing here opens a socket: the suite must stay offline.

const fakePGDriverName = "prism-fake-pg"

func init() {
	sql.Register(fakePGDriverName, fakePGDriver{})
}

// testNow is the injected clock, so updated_at assertions are exact rather
// than approximate.
var testNow = time.Date(2026, 10, 4, 14, 35, 53, 0, time.UTC)

var (
	fakePGRegistryMu sync.Mutex
	fakePGRegistry   = map[string]*fakePG{}
	fakePGSerial     int64
)

// fakePGDriver hands each sql.Open its registered fake database.
type fakePGDriver struct{}

func (fakePGDriver) Open(dsn string) (driver.Conn, error) {
	fakePGRegistryMu.Lock()
	f, ok := fakePGRegistry[dsn]
	fakePGRegistryMu.Unlock()
	if !ok {
		return nil, fmt.Errorf("fake-pg: no database registered as %q", dsn)
	}
	return &fakePGConn{f: f}, nil
}

// fakePG is the "database": keyed cursor rows plus a log of every statement
// and transaction the store issued. It implements the small SQL surface
// PGCursorStore uses and rejects anything else, so a statement change cannot
// pass unnoticed.
//
// Uncommitted writes live in a per-transaction overlay and only merge on
// commit, which is what makes "rolled back means not written" true here too.
//
// Failure and blocking knobs must be set before the store starts using it.
type fakePG struct {
	mu   sync.Mutex
	dsn  string
	rows map[string]fakeCursorRow

	statements    []fakeStatement
	txOps         []string
	schemaCreated bool
	openTx        int

	// Injection knobs.
	beginErr   error
	commitErr  error
	lockErr    error
	queryErr   error
	queryErrOn string
	execErr    error
	execErrOn  string
	holdLock   bool
	skipInsert bool
}

type fakeCursorRow struct {
	cursor    string
	lastID    string
	updatedAt time.Time
}

type fakeStatement struct {
	sql  string
	inTx bool
}

// newFakePG registers a fresh fake database and closes it — plus asserts that
// no transaction was left open — when the test finishes. The open-transaction
// check is the leak assertion: a Save that forgot its rollback would fail
// here rather than in production.
func newFakePG(t *testing.T) (*fakePG, *sql.DB) {
	t.Helper()

	f := &fakePG{rows: map[string]fakeCursorRow{}}
	dsn := fmt.Sprintf("fake-pg://%s/%d", t.Name(), atomic.AddInt64(&fakePGSerial, 1))
	f.dsn = dsn

	fakePGRegistryMu.Lock()
	fakePGRegistry[dsn] = f
	fakePGRegistryMu.Unlock()

	db, err := sql.Open(fakePGDriverName, dsn)
	if err != nil {
		t.Fatalf("sql.Open(%q) error = %v", dsn, err)
	}

	t.Cleanup(func() {
		f.mu.Lock()
		leaked := f.openTx
		f.mu.Unlock()
		if leaked != 0 {
			t.Errorf("%s: fake database has %d transaction(s) still open", t.Name(), leaked)
		}
		if err := db.Close(); err != nil {
			t.Errorf("closing the fake database: %v", err)
		}
		fakePGRegistryMu.Lock()
		delete(fakePGRegistry, dsn)
		fakePGRegistryMu.Unlock()
	})
	return f, db
}

// newTestPGStore builds a store over a fresh fake, with a fixed clock unless
// the test overrides it.
func newTestPGStore(t *testing.T, cfg stream.PGStoreConfig) (*stream.PGCursorStore, *fakePG) {
	t.Helper()

	f, db := newFakePG(t)
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return testNow }
	}
	store, err := stream.NewPGCursorStore(db, cfg)
	if err != nil {
		t.Fatalf("NewPGCursorStore() error = %v", err)
	}
	return store, f
}

// --- statements -----------------------------------------------------------

func normaliseSQL(q string) string { return strings.Join(strings.Fields(q), " ") }

func (f *fakePG) statementsLog() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.statements))
	for _, s := range f.statements {
		out = append(out, s.sql)
	}
	return out
}

// txStatementsLog reports only the statements issued inside a transaction,
// which is how "the write happens under the lock" is asserted.
func (f *fakePG) txStatementsLog() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, s := range f.statements {
		if s.inTx {
			out = append(out, s.sql)
		}
	}
	return out
}

func (f *fakePG) txOpsLog() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.txOps...)
}

func (f *fakePG) rowFor(key string) (fakeCursorRow, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.rows[key]
	return r, ok
}

// awaitTransactionsClosed waits until no transaction is open. The wait exists
// because database/sql finishes a context-cancelled transaction
// asynchronously, so a caller that just saw the cancellation error may still
// be one scheduling tick away from the rollback. The bound turns a genuine
// leak into a failure instead of a hang.
func (f *fakePG) awaitTransactionsClosed(t *testing.T, within time.Duration) {
	t.Helper()

	deadline := time.Now().Add(within)
	for {
		f.mu.Lock()
		open := f.openTx
		f.mu.Unlock()
		if open == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Errorf("fake database still has %d transaction(s) open after %v", open, within)
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// --- driver plumbing ------------------------------------------------------

type fakePGConn struct {
	f  *fakePG
	tx *fakePGTx // guarded by f.mu
}

func (c *fakePGConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("fake-pg: prepared statements are unsupported; use ExecerContext/QueryerContext")
}

func (c *fakePGConn) Close() error { return nil }

func (c *fakePGConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *fakePGConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	c.f.mu.Lock()
	defer c.f.mu.Unlock()
	if c.f.beginErr != nil {
		return nil, c.f.beginErr
	}
	c.f.txOps = append(c.f.txOps, "begin")
	c.f.openTx++
	tx := &fakePGTx{f: c.f, conn: c, pending: map[string]fakeCursorRow{}}
	c.tx = tx
	return tx, nil
}

func (c *fakePGConn) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	c.f.mu.Lock()
	tx := c.tx
	c.f.mu.Unlock()
	return c.f.exec(ctx, tx, q, args)
}

func (c *fakePGConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	c.f.mu.Lock()
	tx := c.tx
	c.f.mu.Unlock()
	return c.f.query(ctx, tx, q, args)
}

type fakePGTx struct {
	f       *fakePG
	conn    *fakePGConn
	pending map[string]fakeCursorRow
	done    bool
}

func (t *fakePGTx) Commit() error {
	t.f.mu.Lock()
	defer t.f.mu.Unlock()
	if t.done {
		return errors.New("fake-pg: transaction already finished")
	}
	t.done = true
	t.f.openTx--
	t.conn.tx = nil
	t.f.txOps = append(t.f.txOps, "commit")
	if t.f.commitErr != nil {
		return t.f.commitErr
	}
	for k, v := range t.pending {
		t.f.rows[k] = v
	}
	return nil
}

func (t *fakePGTx) Rollback() error {
	t.f.mu.Lock()
	defer t.f.mu.Unlock()
	if t.done {
		return nil
	}
	t.done = true
	t.f.openTx--
	t.conn.tx = nil
	t.f.txOps = append(t.f.txOps, "rollback")
	return nil
}

// readLocked and writeLocked resolve a key through the transaction overlay,
// so an uncommitted write is visible to its own transaction and a rollback
// discards it.
func (f *fakePG) readLocked(tx *fakePGTx, key string) (fakeCursorRow, bool) {
	if tx != nil {
		if r, ok := tx.pending[key]; ok {
			return r, true
		}
	}
	r, ok := f.rows[key]
	return r, ok
}

func (f *fakePG) writeLocked(tx *fakePGTx, key string, row fakeCursorRow) {
	if tx != nil {
		tx.pending[key] = row
		return
	}
	f.rows[key] = row
}

func (f *fakePG) exec(ctx context.Context, tx *fakePGTx, query string, args []driver.NamedValue) (driver.Result, error) {
	norm := normaliseSQL(query)
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	f.mu.Lock()
	f.statements = append(f.statements, fakeStatement{sql: norm, inTx: tx != nil})
	if f.execErrOn != "" && strings.Contains(norm, f.execErrOn) {
		err := f.execErr
		f.mu.Unlock()
		return nil, err
	}
	f.mu.Unlock()

	switch {
	case strings.HasPrefix(norm, "CREATE TABLE"):
		f.mu.Lock()
		f.schemaCreated = true
		f.mu.Unlock()
		return fakePGResult{}, nil

	case strings.HasPrefix(norm, "INSERT"):
		return f.insert(tx, args)

	case strings.HasPrefix(norm, "UPDATE"):
		return f.update(tx, args)
	}
	return nil, fmt.Errorf("fake-pg: unsupported statement %q", norm)
}

func (f *fakePG) query(ctx context.Context, tx *fakePGTx, query string, args []driver.NamedValue) (driver.Rows, error) {
	norm := normaliseSQL(query)
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	f.mu.Lock()
	f.statements = append(f.statements, fakeStatement{sql: norm, inTx: tx != nil})
	if f.queryErrOn != "" && strings.Contains(norm, f.queryErrOn) {
		err := f.queryErr
		f.mu.Unlock()
		return nil, err
	}
	hold := f.holdLock && strings.Contains(norm, "FOR UPDATE")
	lockErr := f.lockErr
	f.mu.Unlock()

	if hold {
		// The row is locked by another connection: block until this caller's
		// context expires, the way a real FOR UPDATE would. Waiting happens
		// with no lock held, so the test can still observe the fake.
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if lockErr != nil && strings.Contains(norm, "FOR UPDATE") {
		// The lock could not be acquired (deadlock timeout, connection drop).
		return nil, lockErr
	}

	key, err := stringArg(args, 0)
	if err != nil {
		return nil, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch {
	case strings.Contains(norm, "FOR UPDATE"):
		if _, ok := f.readLocked(tx, key); !ok {
			return &fakePGRows{cols: []string{"name"}}, nil
		}
		return &fakePGRows{cols: []string{"name"}, data: [][]driver.Value{{key}}}, nil

	case strings.HasPrefix(norm, "SELECT"):
		row, ok := f.readLocked(tx, key)
		if !ok {
			return &fakePGRows{cols: []string{"cursor", "last_id", "updated_at"}}, nil
		}
		return &fakePGRows{
			cols: []string{"cursor", "last_id", "updated_at"},
			data: [][]driver.Value{{row.cursor, row.lastID, row.updatedAt}},
		}, nil
	}
	return nil, fmt.Errorf("fake-pg: unsupported statement %q", norm)
}

func (f *fakePG) insert(tx *fakePGTx, args []driver.NamedValue) (driver.Result, error) {
	key, err := stringArg(args, 0)
	if err != nil {
		return nil, err
	}
	cursor, err := stringArg(args, 1)
	if err != nil {
		return nil, err
	}
	lastID, err := stringArg(args, 2)
	if err != nil {
		return nil, err
	}
	at, err := timeArg(args, 3)
	if err != nil {
		return nil, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.skipInsert {
		// As if the row were deleted between the lock and the insert.
		return fakePGResult{}, nil
	}
	if _, ok := f.readLocked(tx, key); ok {
		// ON CONFLICT DO NOTHING: a concurrent first writer already won.
		return fakePGResult{}, nil
	}
	f.writeLocked(tx, key, fakeCursorRow{cursor: cursor, lastID: lastID, updatedAt: at})
	return fakePGResult{affected: 1}, nil
}

func (f *fakePG) update(tx *fakePGTx, args []driver.NamedValue) (driver.Result, error) {
	key, err := stringArg(args, 0)
	if err != nil {
		return nil, err
	}
	cursor, err := stringArg(args, 1)
	if err != nil {
		return nil, err
	}
	lastID, err := stringArg(args, 2)
	if err != nil {
		return nil, err
	}
	at, err := timeArg(args, 3)
	if err != nil {
		return nil, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.readLocked(tx, key); !ok {
		return fakePGResult{}, nil
	}
	f.writeLocked(tx, key, fakeCursorRow{cursor: cursor, lastID: lastID, updatedAt: at})
	return fakePGResult{affected: 1}, nil
}

func stringArg(args []driver.NamedValue, i int) (string, error) {
	if i >= len(args) {
		return "", fmt.Errorf("fake-pg: expected argument %d, got %d argument(s)", i+1, len(args))
	}
	v, ok := args[i].Value.(string)
	if !ok {
		return "", fmt.Errorf("fake-pg: argument %d is %T, want a string", i+1, args[i].Value)
	}
	return v, nil
}

func timeArg(args []driver.NamedValue, i int) (time.Time, error) {
	if i >= len(args) {
		return time.Time{}, fmt.Errorf("fake-pg: expected argument %d, got %d argument(s)", i+1, len(args))
	}
	v, ok := args[i].Value.(time.Time)
	if !ok {
		return time.Time{}, fmt.Errorf("fake-pg: argument %d is %T, want a time.Time", i+1, args[i].Value)
	}
	return v, nil
}

type fakePGResult struct{ affected int64 }

func (r fakePGResult) LastInsertId() (int64, error) {
	return 0, errors.New("fake-pg: LastInsertId is unsupported")
}

func (r fakePGResult) RowsAffected() (int64, error) { return r.affected, nil }

type fakePGRows struct {
	cols []string
	data [][]driver.Value
	pos  int
}

func (r *fakePGRows) Columns() []string { return r.cols }

func (r *fakePGRows) Close() error { return nil }

func (r *fakePGRows) Next(dest []driver.Value) error {
	if r.pos >= len(r.data) {
		return io.EOF
	}
	copy(dest, r.data[r.pos])
	r.pos++
	return nil
}

// --- assertions -----------------------------------------------------------

func assertStatementSequence(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("issued %d statement(s), want %d\n got: %q\nwant: %q", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("statement %d =\n %q\nwant\n %q", i, got[i], want[i])
		}
	}
}

// --- construction ---------------------------------------------------------

func TestNewPGCursorStoreValidatesConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		table   string
		wantErr string
	}{
		{name: "default is accepted", table: ""},
		{name: "plain identifier is accepted", table: "cursors_v2"},
		{name: "statement injection is rejected", table: "cursors; DROP TABLE users", wantErr: "identifier"},
		{name: "qualified name is rejected", table: "public.cursors", wantErr: "identifier"},
		{name: "leading digit is rejected", table: "1cursors", wantErr: "identifier"},
		{name: "quoted name is rejected", table: `"cursors"`, wantErr: "identifier"},
		{name: "over the identifier limit", table: strings.Repeat("a", 64), wantErr: "63-byte"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, db := newFakePG(t)
			store, err := stream.NewPGCursorStore(db, stream.PGStoreConfig{Table: tc.table})
			if tc.wantErr != "" {
				if err == nil {
					t.Fatal("NewPGCursorStore() error = nil, want a rejection")
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("error = %q, want it to mention %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewPGCursorStore() error = %v", err)
			}
			if store == nil {
				t.Fatal("NewPGCursorStore() returned a nil store with no error")
			}
		})
	}
}

func TestNewPGCursorStoreRequiresADatabase(t *testing.T) {
	t.Parallel()

	if _, err := stream.NewPGCursorStore(nil, stream.PGStoreConfig{}); err == nil {
		t.Fatal("NewPGCursorStore(nil) error = nil, want a rejection")
	}
}

func TestPGCursorStoreEnsureSchemaCreatesTheTable(t *testing.T) {
	t.Parallel()

	store, f := newTestPGStore(t, stream.PGStoreConfig{})
	if err := store.EnsureSchema(context.Background()); err != nil {
		t.Fatalf("EnsureSchema() error = %v", err)
	}
	if err := store.EnsureSchema(context.Background()); err != nil {
		t.Fatalf("EnsureSchema() a second time = %v, want idempotent", err)
	}

	stmts := f.statementsLog()
	if len(stmts) != 2 {
		t.Fatalf("issued %d statement(s), want 2: %q", len(stmts), stmts)
	}
	if !strings.HasPrefix(stmts[0], `CREATE TABLE IF NOT EXISTS "prism_cursors"`) {
		t.Errorf("statement = %q, want it to create the default table", stmts[0])
	}
	for _, want := range []string{"name TEXT PRIMARY KEY", "cursor TEXT NOT NULL", "last_id TEXT NOT NULL"} {
		if !strings.Contains(stmts[0], want) {
			t.Errorf("schema statement is missing %q: %s", want, stmts[0])
		}
	}
	if !f.schemaCreated {
		t.Error("schemaCreated = false, want true")
	}
}

func TestPGCursorStoreEnsureSchemaReportsFailure(t *testing.T) {
	t.Parallel()

	store, f := newTestPGStore(t, stream.PGStoreConfig{})
	wantErr := errors.New("permission denied for schema public")
	f.execErrOn = "CREATE TABLE"
	f.execErr = wantErr

	err := store.EnsureSchema(context.Background())
	if !errors.Is(err, wantErr) {
		t.Fatalf("EnsureSchema() error = %v, want it to wrap %v", err, wantErr)
	}
}

// --- Save -----------------------------------------------------------------

// TestPGCursorStoreSaveLocksBeforeItWrites is the central contract: a fresh
// key must go lock → insert → re-lock → update inside one transaction, so the
// existence check and the write are one critical section.
func TestPGCursorStoreSaveLocksBeforeItWrites(t *testing.T) {
	t.Parallel()

	store, f := newTestPGStore(t, stream.PGStoreConfig{})
	want := stream.CursorState{
		Cursor: "3966812-0000000000",
		LastID: "0017037362169118720-0000000000",
	}
	if err := store.Save(context.Background(), "events/test", want); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	assertStatementSequence(t, f.statementsLog(), []string{
		`SELECT name FROM "prism_cursors" WHERE name = $1 FOR UPDATE`,
		`INSERT INTO "prism_cursors" (name, cursor, last_id, updated_at) VALUES ($1, $2, $3, $4) ON CONFLICT (name) DO NOTHING`,
		`SELECT name FROM "prism_cursors" WHERE name = $1 FOR UPDATE`,
		`UPDATE "prism_cursors" SET cursor = $2, last_id = $3, updated_at = $4 WHERE name = $1`,
	})
	// Every statement must be inside the one transaction, or the lock would
	// not protect the write.
	assertStatementSequence(t, f.txStatementsLog(), f.statementsLog())

	if got, want := f.txOpsLog(), []string{"begin", "commit"}; !equalStrings(got, want) {
		t.Errorf("transaction log = %q, want %q", got, want)
	}

	row, ok := f.rowFor("events/test")
	if !ok {
		t.Fatal("row was not written")
	}
	if row.cursor != want.Cursor || row.lastID != want.LastID {
		t.Errorf("row = %+v, want cursor %q and last ID %q", row, want.Cursor, want.LastID)
	}
	if !row.updatedAt.Equal(testNow) {
		t.Errorf("updated_at = %v, want the injected clock %v", row.updatedAt, testNow)
	}
}

// TestPGCursorStoreSaveUpdatesAnExistingRow checks the other half of the
// branch: a known key is never re-inserted, and is still locked first.
func TestPGCursorStoreSaveUpdatesAnExistingRow(t *testing.T) {
	t.Parallel()

	store, f := newTestPGStore(t, stream.PGStoreConfig{})
	ctx := context.Background()
	if err := store.Save(ctx, "events/test", stream.CursorState{Cursor: "1", LastID: "a"}); err != nil {
		t.Fatalf("first Save() error = %v", err)
	}
	before := len(f.statementsLog())

	if err := store.Save(ctx, "events/test", stream.CursorState{Cursor: "2", LastID: "b"}); err != nil {
		t.Fatalf("second Save() error = %v", err)
	}

	assertStatementSequence(t, f.statementsLog()[before:], []string{
		`SELECT name FROM "prism_cursors" WHERE name = $1 FOR UPDATE`,
		`UPDATE "prism_cursors" SET cursor = $2, last_id = $3, updated_at = $4 WHERE name = $1`,
	})

	row, ok := f.rowFor("events/test")
	if !ok {
		t.Fatal("row disappeared")
	}
	if row.cursor != "2" || row.lastID != "b" {
		t.Errorf("row = %+v, want cursor 2 and last ID b", row)
	}
}

func TestPGCursorStoreSaveRejectsAnEmptyKey(t *testing.T) {
	t.Parallel()

	store, f := newTestPGStore(t, stream.PGStoreConfig{})
	if err := store.Save(context.Background(), "", stream.CursorState{}); err == nil {
		t.Fatal("Save(\"\") error = nil, want a rejection")
	}
	if got := len(f.statementsLog()); got != 0 {
		t.Errorf("issued %d statement(s) for an empty key, want 0", got)
	}
}

// TestPGCursorStoreSaveReportsLockFailures covers the error path where the
// "SELECT ... FOR UPDATE" that opens the transaction fails: the save must
// return that error untouched so callers can classify it with errors.Is.
func TestPGCursorStoreSaveReportsLockFailures(t *testing.T) {
	t.Parallel()

	store, f := newTestPGStore(t, stream.PGStoreConfig{})
	f.lockErr = errors.New("connection refused")

	err := store.Save(context.Background(), "events/test", stream.CursorState{Cursor: "3966812-0000000000"})
	if !errors.Is(err, f.lockErr) {
		t.Fatalf("Save() error = %v, want lock error %v", err, f.lockErr)
	}
	if got, want := f.txOpsLog(), []string{"begin", "rollback"}; !equalStrings(got, want) {
		t.Errorf("transaction log = %q, want %q — a failed lock must roll back", got, want)
	}
}

func TestPGCursorStoreSaveRollsBackWhenTheUpdateFails(t *testing.T) {
	t.Parallel()

	store, f := newTestPGStore(t, stream.PGStoreConfig{})
	wantErr := errors.New("connection lost mid-transaction")
	f.execErrOn = "UPDATE"
	f.execErr = wantErr

	err := store.Save(context.Background(), "events/test",
		stream.CursorState{Cursor: "3966812-0000000000", LastID: "0017037362169118720-0000000000"})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Save() error = %v, want it to wrap %v", err, wantErr)
	}
	if got, want := f.txOpsLog(), []string{"begin", "rollback"}; !equalStrings(got, want) {
		t.Errorf("transaction log = %q, want %q — an uncommitted write must not survive", got, want)
	}
	if _, ok := f.rowFor("events/test"); ok {
		t.Error("a rolled-back Save left its row behind")
	}
}

func TestPGCursorStoreSaveBeginFailureIsReported(t *testing.T) {
	t.Parallel()

	store, f := newTestPGStore(t, stream.PGStoreConfig{})
	wantErr := errors.New("too many connections")
	f.beginErr = wantErr

	err := store.Save(context.Background(), "events/test", stream.CursorState{Cursor: "1"})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Save() error = %v, want it to wrap %v", err, wantErr)
	}
	if got := f.txOpsLog(); len(got) != 0 {
		t.Errorf("transaction log = %q, want nothing to have started", got)
	}
}

func TestPGCursorStoreSaveCommitFailureIsReported(t *testing.T) {
	t.Parallel()

	store, f := newTestPGStore(t, stream.PGStoreConfig{})
	wantErr := errors.New("server closed the connection")
	f.commitErr = wantErr

	err := store.Save(context.Background(), "events/test", stream.CursorState{Cursor: "1", LastID: "a"})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Save() error = %v, want it to wrap %v", err, wantErr)
	}
	// A failed commit is not durable, so nothing may be visible afterwards.
	if _, ok := f.rowFor("events/test"); ok {
		t.Error("a failed commit left its row behind")
	}
}

// TestPGCursorStoreSaveGivesUpWaitingForTheLock covers the stalled-peer case:
// another connection holds the row, and Save must fail with the context's
// error instead of hanging the stream forever.
func TestPGCursorStoreSaveGivesUpWaitingForTheLock(t *testing.T) {
	t.Parallel()

	store, f := newTestPGStore(t, stream.PGStoreConfig{LockTimeout: 50 * time.Millisecond})
	f.holdLock = true

	err := store.Save(context.Background(), "events/test", stream.CursorState{Cursor: "1"})
	if err == nil {
		t.Fatal("Save() error = nil, want a lock timeout")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Save() error = %v, want it to wrap context.DeadlineExceeded", err)
	}
	if !strings.Contains(err.Error(), "FOR UPDATE") && !strings.Contains(err.Error(), "locking") {
		t.Errorf("error = %q, want it to point at the row lock", err)
	}
	// database/sql rolls a cancelled transaction back on its own goroutine,
	// which can land just after Save returns; wait for it rather than racing
	// it, then check it really happened.
	f.awaitTransactionsClosed(t, 2*time.Second)
	stmts := f.statementsLog()
	if len(stmts) == 0 || !strings.Contains(stmts[0], "FOR UPDATE") {
		t.Errorf("statements = %q, want the lock to have been attempted", stmts)
	}
	// The transaction must never have committed: a timed-out wait leaves the
	// previous position untouched.
	for _, op := range f.txOpsLog() {
		if op == "commit" {
			t.Errorf("transaction log = %q, want no commit after a lock timeout", f.txOpsLog())
		}
	}
}

// TestPGCursorStoreSaveSurvivesALostInsertRace covers the row appearing
// between the lock and the insert: the re-lock must find it, and if it does
// not, Save must fail loudly rather than write a row nobody can see.
func TestPGCursorStoreSaveSurvivesALostInsertRace(t *testing.T) {
	t.Parallel()

	store, f := newTestPGStore(t, stream.PGStoreConfig{})
	f.skipInsert = true

	err := store.Save(context.Background(), "events/test", stream.CursorState{Cursor: "1", LastID: "a"})
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("Save() error = %v, want it to wrap sql.ErrNoRows", err)
	}
	if got, want := f.txOpsLog(), []string{"begin", "rollback"}; !equalStrings(got, want) {
		t.Errorf("transaction log = %q, want %q", got, want)
	}
	if _, ok := f.rowFor("events/test"); ok {
		t.Error("a failed Save left a row behind")
	}
}

// --- Load -----------------------------------------------------------------

func TestPGCursorStoreLoadReturnsWhatWasSaved(t *testing.T) {
	t.Parallel()

	store, _ := newTestPGStore(t, stream.PGStoreConfig{})
	ctx := context.Background()
	want := stream.CursorState{Cursor: "3966812-0000000000", LastID: "0017037362169118720-0000000000"}
	if err := store.Save(ctx, "events/test", want); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	got, err := store.Load(ctx, "events/test")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got.Cursor != want.Cursor || got.LastID != want.LastID {
		t.Errorf("Load() = %+v, want %+v", got, want)
	}
	if !got.UpdatedAt.Equal(testNow) {
		t.Errorf("Load().UpdatedAt = %v, want %v", got.UpdatedAt, testNow)
	}
}

// TestPGCursorStoreLoadReportsAMissingRow: a first run is not a failure, and
// the caller must be able to tell "nothing saved yet" from "the database is
// broken" with errors.Is.
func TestPGCursorStoreLoadReportsAMissingRow(t *testing.T) {
	t.Parallel()

	store, _ := newTestPGStore(t, stream.PGStoreConfig{})
	_, err := store.Load(context.Background(), "never-written")
	if !errors.Is(err, stream.ErrNoCursor) {
		t.Fatalf("Load() error = %v, want it to match ErrNoCursor", err)
	}
	if !strings.Contains(err.Error(), "never-written") {
		t.Errorf("error = %q, want it to name the key", err)
	}
}

func TestPGCursorStoreLoadReportsQueryFailures(t *testing.T) {
	t.Parallel()

	store, f := newTestPGStore(t, stream.PGStoreConfig{})
	wantErr := errors.New("database is asleep")
	f.queryErrOn = "SELECT cursor"
	f.queryErr = wantErr

	_, err := store.Load(context.Background(), "events/test")
	if !errors.Is(err, wantErr) {
		t.Fatalf("Load() error = %v, want it to wrap %v", err, wantErr)
	}
}

func TestPGCursorStoreLoadHonoursCancellation(t *testing.T) {
	t.Parallel()

	store, _ := newTestPGStore(t, stream.PGStoreConfig{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := store.Load(ctx, "events/test")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Load() error = %v, want it to wrap context.Canceled", err)
	}
}

func TestPGCursorStoreRejectsAnEmptyKeyOnLoad(t *testing.T) {
	t.Parallel()

	store, f := newTestPGStore(t, stream.PGStoreConfig{})
	if _, err := store.Load(context.Background(), ""); err == nil {
		t.Fatal("Load(\"\") error = nil, want a rejection")
	}
	if got := len(f.statementsLog()); got != 0 {
		t.Errorf("issued %d statement(s) for an empty key, want 0", got)
	}
}

// TestPGCursorStoreConcurrentSavesKeepStatesConsistent hammers one key from
// several goroutines. The fake does not emulate PostgreSQL's lock waits, so
// the assertion is deliberately weaker than the real-database one: no race,
// and the surviving row must be one writer's complete state rather than a
// torn mixture of two. Mutual exclusion itself is proven against a real
// server in the integration test.
func TestPGCursorStoreConcurrentSavesKeepStatesConsistent(t *testing.T) {
	t.Parallel()

	store, _ := newTestPGStore(t, stream.PGStoreConfig{})
	ctx := context.Background()

	const writers = 8
	states := make([]stream.CursorState, writers)
	for i := range states {
		states[i] = stream.CursorState{
			Cursor: fmt.Sprintf("cursor-%02d", i),
			LastID: fmt.Sprintf("last-%02d", i),
		}
	}

	var wg sync.WaitGroup
	errs := make([]error, writers)
	for i, st := range states {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = store.Save(ctx, "events/shared", st)
		}()
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("Save() #%d error = %v", i, err)
		}
	}
	row, ok := loadRow(store, "events/shared")
	if !ok {
		t.Fatal("no row survived the concurrent saves")
	}
	// The surviving row must be one writer's pair, never the cursor from one
	// and the last ID from another.
	suffix := strings.TrimPrefix(row.cursor, "cursor-")
	if suffix == row.cursor || row.lastID != "last-"+suffix {
		t.Errorf("row = %+v, want a complete state from a single writer", row)
	}
}

// loadRow reads a row back through the store, so tests assert what the store
// itself can see rather than reaching into the driver's memory.
func loadRow(store *stream.PGCursorStore, key string) (fakeCursorRow, bool) {
	st, err := store.Load(context.Background(), key)
	if err != nil {
		return fakeCursorRow{}, false
	}
	return fakeCursorRow{cursor: st.Cursor, lastID: st.LastID, updatedAt: st.UpdatedAt}, true
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
