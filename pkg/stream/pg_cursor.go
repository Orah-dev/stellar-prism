package stream

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"time"
)

// ErrNoCursor reports that nothing has been persisted for a key yet.
//
// A first run is a normal condition rather than a failure, so Load returns
// this instead of a bare sql.ErrNoRows and callers branch with
// errors.Is(err, ErrNoCursor) without having to know which driver is behind
// the store.
var ErrNoCursor = errors.New("no saved cursor")

// DefaultPGCursorTable is the table PGCursorStore writes to when no other is
// configured.
const DefaultPGCursorTable = "prism_cursors"

// DefaultCursorLockTimeout bounds how long a Save waits for the row lock
// before giving up. A peer holding the lock far longer than this means
// something is wrong, and prism should say so rather than stall a stream
// that looks hung.
const DefaultCursorLockTimeout = 5 * time.Second

// maxSQLIdentifierLen is PostgreSQL's identifier limit, beyond which a name
// is silently truncated — a table-name bug that would only show up as
// confusing "relation does not exist" errors, so it is rejected up front.
const maxSQLIdentifierLen = 63

// sqlIdentifier matches a plain, unquoted SQL identifier. Table names cannot
// be bound as query parameters, so this is what stands between a flag value
// and an injection hole.
var sqlIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// CursorState is a persisted stream position.
type CursorState struct {
	// Cursor resumes the stream immediately after the last emitted record.
	// Empty means the stream had not produced a cursor yet and the Source's
	// configured start position still applies.
	Cursor string
	// LastID is the high-water mark of what has been emitted. Because record
	// IDs are fixed-width and zero-padded, restoring it lets a restarted run
	// suppress anything it would otherwise replay from the resume point.
	LastID string
	// UpdatedAt is when the store last wrote the row. It is diagnostic only;
	// nothing in the state machine reads it.
	UpdatedAt time.Time
}

// CursorStore persists stream positions across process restarts.
//
// It is the seam behind "a restart resumes where the last run stopped". The
// tailer writes after every emit, so a store that keeps its promise bounds
// the replay window of a crash to the record in flight.
//
// Implementations must make Save durable — when it returns nil the position
// must be recoverable after a process death — and atomic with respect to
// other writers of the same key, so that two prism processes watching one
// stream cannot interleave writes and lose each other's position.
type CursorStore interface {
	// Load returns the position saved under key. When nothing has been saved
	// it returns an error satisfying errors.Is(err, ErrNoCursor).
	Load(ctx context.Context, key string) (CursorState, error)

	// Save records state under key, durably and atomically.
	Save(ctx context.Context, key string, state CursorState) error
}

// PGStoreConfig configures a PGCursorStore. The zero value is valid and
// means "default table, default lock timeout, wall clock".
type PGStoreConfig struct {
	// Table is the table holding cursor rows. It must be a plain SQL
	// identifier (letters, digits and underscore, not starting with a digit,
	// at most 63 bytes); anything else is rejected at construction because a
	// table name cannot be bound as a parameter. Defaults to
	// DefaultPGCursorTable.
	Table string

	// LockTimeout bounds how long Save waits for the row lock. Zero means
	// DefaultCursorLockTimeout.
	LockTimeout time.Duration

	// Now is the clock behind updated_at. Nil means time.Now. It exists so
	// tests are deterministic rather than dependent on the wall clock.
	Now func() time.Time
}

// PGCursorStore persists stream positions in PostgreSQL.
//
// # Why SELECT ... FOR UPDATE
//
// A Save has to answer "does this row exist yet?" before it can write, and
// that read-then-write window is exactly where two processes sharing a key
// would race: one would see no row and INSERT while the other did the same,
// and the loser would fail with a duplicate-key error (or worse, a blind
// UPDATE would overwrite a position that had already moved on). Every Save
// therefore runs in one transaction that takes an exclusive lock on the row
// first and only then decides and writes:
//
//	BEGIN
//	SELECT name FROM prism_cursors WHERE name = $1 FOR UPDATE  -- the lock
//	INSERT ... ON CONFLICT DO NOTHING                           -- only if absent
//	UPDATE prism_cursors SET ... WHERE name = $1               -- under the lock
//	COMMIT
//
// Whichever transaction holds the lock writes last and wins, so positions
// from concurrent writers are serialised rather than lost, and the first-ever
// save of a key cannot collide. A plain UPDATE would take the row lock by
// itself; the explicit lock is what makes the existence check and the insert
// that follows it part of the same critical section.
//
// # Scope
//
// The store does not own the *sql.DB it is given: opening and closing the
// pool is the caller's job, which keeps connection lifetime tied to the
// command that asked for it rather than to a package-level singleton.
// PGCursorStore starts no goroutines of its own.
type PGCursorStore struct {
	db          *sql.DB
	lockTimeout time.Duration
	now         func() time.Time

	createQuery string
	loadQuery   string
	lockQuery   string
	insertQuery string
	updateQuery string
}

// NewPGCursorStore returns a cursor store over db.
//
// It performs no I/O; call EnsureSchema to create the table before first
// use. An error here is a configuration problem (a nil handle or an
// unusable table name), not a connectivity one, so it is safe to call before
// any network exists.
func NewPGCursorStore(db *sql.DB, cfg PGStoreConfig) (*PGCursorStore, error) {
	if db == nil {
		return nil, errors.New("postgres cursor store: a *sql.DB is required")
	}

	table := cfg.Table
	if table == "" {
		table = DefaultPGCursorTable
	}
	if len(table) > maxSQLIdentifierLen {
		return nil, fmt.Errorf(
			"postgres cursor store: table name %q exceeds the %d-byte identifier limit",
			table, maxSQLIdentifierLen)
	}
	if !sqlIdentifier.MatchString(table) {
		return nil, fmt.Errorf(
			"postgres cursor store: table name %q is not a plain SQL identifier", table)
	}

	lockTimeout := cfg.LockTimeout
	if lockTimeout <= 0 {
		lockTimeout = DefaultCursorLockTimeout
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}

	// The identifier has been validated above, so quoting it is safe: it is
	// the one part of these statements that cannot be a bound parameter.
	quoted := `"` + table + `"`
	return &PGCursorStore{
		db:          db,
		lockTimeout: lockTimeout,
		now:         now,
		createQuery: `CREATE TABLE IF NOT EXISTS ` + quoted + ` (
    name       TEXT        PRIMARY KEY,
    cursor     TEXT        NOT NULL,
    last_id    TEXT        NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
)`,
		loadQuery:   `SELECT cursor, last_id, updated_at FROM ` + quoted + ` WHERE name = $1`,
		lockQuery:   `SELECT name FROM ` + quoted + ` WHERE name = $1 FOR UPDATE`,
		insertQuery: `INSERT INTO ` + quoted + ` (name, cursor, last_id, updated_at) VALUES ($1, $2, $3, $4) ON CONFLICT (name) DO NOTHING`,
		updateQuery: `UPDATE ` + quoted + ` SET cursor = $2, last_id = $3, updated_at = $4 WHERE name = $1`,
	}, nil
}

// EnsureSchema creates the cursor table if it does not exist.
//
// prism creates its own table so that "--pg-dsn points at a database and it
// works" is all an operator has to do. The connecting role needs CREATE on
// the schema; without it, pre-create the table using the statement in
// docs/architecture.md — EnsureSchema is then a no-op.
func (s *PGCursorStore) EnsureSchema(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, s.createQuery); err != nil {
		return fmt.Errorf("creating cursor table: %w", err)
	}
	return nil
}

// Load returns the position saved under key.
//
// It takes no lock: a read observes one committed row, and holding a lock
// while the caller merely looks at a position would make the tailer block
// behind an unrelated writer for no benefit.
func (s *PGCursorStore) Load(ctx context.Context, key string) (CursorState, error) {
	if key == "" {
		return CursorState{}, errors.New("cursor key must not be empty")
	}

	var st CursorState
	err := s.db.QueryRowContext(ctx, s.loadQuery, key).Scan(&st.Cursor, &st.LastID, &st.UpdatedAt)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return CursorState{}, fmt.Errorf("cursor %q: %w", key, ErrNoCursor)
	case err != nil:
		return CursorState{}, fmt.Errorf("loading cursor %q: %w", key, err)
	}
	return st, nil
}

// Save writes state under key in a single transaction that first takes the
// row lock.
//
// The whole transaction is bounded by the configured lock timeout (and by
// ctx, whichever is sooner), so a peer that never releases the lock surfaces
// as a clear error instead of a stream that has quietly stopped. If Save
// fails, nothing was committed and the previous position still stands.
func (s *PGCursorStore) Save(ctx context.Context, key string, state CursorState) (err error) {
	if key == "" {
		return errors.New("cursor key must not be empty")
	}

	// Bound the transaction rather than only the lock wait: a Save that
	// cannot finish must fail, not hold the stream open indefinitely.
	lockCtx, cancel := context.WithTimeout(ctx, s.lockTimeout)
	defer cancel()

	tx, err := s.db.BeginTx(lockCtx, nil)
	if err != nil {
		return fmt.Errorf("beginning cursor transaction: %w", err)
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		// Ending an uncommitted transaction is best effort: sql.ErrTxDone
		// means the driver already released it (a cancelled context), which
		// is not a second failure. Anything else is joined rather than
		// dropped, so the caller keeps both the cause and the cleanup error
		// through errors.Is.
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, sql.ErrTxDone) {
			err = errors.Join(err, fmt.Errorf("rolling back cursor %q: %w", key, rbErr))
		}
	}()

	now := s.now()
	state.UpdatedAt = now

	// Lock first, write second. See the type documentation for why the
	// ordering matters.
	var held string
	err = tx.QueryRowContext(lockCtx, s.lockQuery, key).Scan(&held)
	switch {
	case err == nil:
		// The row exists and we hold it: fall through to the update.

	case errors.Is(err, sql.ErrNoRows):
		// First writer for this key. ON CONFLICT DO NOTHING means a
		// concurrent first writer turns this into a no-op rather than a
		// duplicate-key failure, and re-selecting under FOR UPDATE afterwards
		// leaves one path that always ends holding the lock, whoever won the
		// insert.
		if _, insErr := tx.ExecContext(lockCtx, s.insertQuery,
			key, state.Cursor, state.LastID, now); insErr != nil {
			return fmt.Errorf("inserting cursor %q: %w", key, insErr)
		}
		if err = tx.QueryRowContext(lockCtx, s.lockQuery, key).Scan(&held); err != nil {
			return fmt.Errorf("locking inserted cursor %q: %w", key, err)
		}

	case err != nil:
		return fmt.Errorf("locking cursor %q: %w", key, err)
	}

	if _, updErr := tx.ExecContext(lockCtx, s.updateQuery,
		key, state.Cursor, state.LastID, now); updErr != nil {
		return fmt.Errorf("updating cursor %q: %w", key, updErr)
	}

	if commitErr := tx.Commit(); commitErr != nil {
		return fmt.Errorf("committing cursor %q: %w", key, commitErr)
	}
	committed = true
	return nil
}
