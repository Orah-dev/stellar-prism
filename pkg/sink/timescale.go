package sink

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// Defaults for the TimescaleDB sink. The batch is large enough that a backfill
// is a handful of round trips, and small enough that an interrupted run loses
// at most a second or so of rows.
const (
	// DefaultTimescaleBatchSize is the number of rows copied per transaction.
	DefaultTimescaleBatchSize = 500
	// DefaultTimescaleFlushInterval bounds how long a partial batch waits.
	DefaultTimescaleFlushInterval = 2 * time.Second
	// DefaultTimescaleFlushTimeout bounds a single COPY, so a stalled
	// connection cannot block Close forever.
	DefaultTimescaleFlushTimeout = 30 * time.Second
	// DefaultTimescaleTable is the table rows are written to.
	DefaultTimescaleTable = "prism_records"
	// timescaleChunkInterval is one hypertable chunk, in ledgers. At roughly
	// five seconds per ledger that is about three days per chunk.
	timescaleChunkInterval = 50000
)

// ErrTimescaleClosed is returned by Emit once the sink has been closed.
var ErrTimescaleClosed = errors.New("timescale: sink is closed")

// TimescaleDatabase is the narrow slice of a PostgreSQL driver the sink needs.
//
// Keeping it this small is what lets the batching, retry and shutdown
// behaviour be tested without a database: a fake only has to record rows. The
// production implementation wraps pgxpool.Pool.
//
// CopyRows must be atomic per call. The sink keeps a failed batch buffered for
// retry, which is only correct if a failed COPY inserted nothing — PostgreSQL
// gives that guarantee inside a transaction.
//
// CopyRows must also tolerate rows that are already present. prism restarts
// re-emit the last record or two by design, and an ingestion sink must treat
// that as a no-op rather than a failure.
type TimescaleDatabase interface {
	// Exec runs a statement that returns no rows, used for schema setup.
	Exec(ctx context.Context, sql string) error
	// CopyRows inserts rows, returning how many were written.
	CopyRows(ctx context.Context, table string, columns []string, rows [][]any) (int64, error)
}

// TimescaleConfig configures a Timescale sink. The zero value is usable and
// means "write to the default table with the default batch size".
type TimescaleConfig struct {
	// Table is the destination, optionally schema-qualified as schema.table.
	Table string
	// BatchSize is how many rows are copied per transaction.
	BatchSize int
	// FlushInterval bounds how long a partial batch waits before a background
	// flush, so a slow stream still lands promptly.
	FlushInterval time.Duration
	// FlushTimeout bounds a single COPY.
	FlushTimeout time.Duration
	// NoHypertable leaves the table as a plain PostgreSQL table instead of
	// promoting it to a TimescaleDB hypertable. The sink also falls back to a
	// plain table automatically when TimescaleDB is not installed or the
	// caller may not create the extension, so this only matters when the
	// fallback should be chosen without probing.
	NoHypertable bool
}

func (c TimescaleConfig) withDefaults() TimescaleConfig {
	if c.Table == "" {
		c.Table = DefaultTimescaleTable
	}
	if c.BatchSize == 0 {
		c.BatchSize = DefaultTimescaleBatchSize
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = DefaultTimescaleFlushInterval
	}
	if c.FlushTimeout <= 0 {
		c.FlushTimeout = DefaultTimescaleFlushTimeout
	}
	return c
}

func (c TimescaleConfig) validate() error {
	if c.BatchSize < 0 {
		return fmt.Errorf("timescale: batch size must not be negative, got %d", c.BatchSize)
	}
	if _, err := parseTableName(c.Table); err != nil {
		return err
	}
	return nil
}

// Timescale writes records into a PostgreSQL table using the COPY protocol,
// promoting it to a TimescaleDB hypertable partitioned by ledger when the
// extension is available.
//
// Rows are buffered and flushed either when the batch fills, when the flush
// interval elapses, or on Flush/Close. A background goroutine performs the
// timed flushes; Close stops it and waits for it to finish, so a terminated
// process leaves no goroutine behind.
type Timescale struct {
	db   TimescaleDatabase
	cfg  TimescaleConfig
	pool *pgxpool.Pool // non-nil only when the sink opened the pool itself

	mu         sync.Mutex
	rows       [][]any
	pendingErr error
	closed     bool
	written    int64
	flushes    int

	hypertable bool

	stop      chan struct{}
	done      chan struct{}
	closeOnce sync.Once
	closeErr  error
}

// NewTimescale connects to dsn, creates the destination table if needed, and
// returns a sink that batches records into it.
//
// The sink owns the connection pool and releases it on Close.
func NewTimescale(ctx context.Context, dsn string, cfg TimescaleConfig) (*Timescale, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("timescale: a connection string is required")
	}
	// Validate before dialling: a bad table name should cost no network
	// round trip, and should not leave a pool to clean up.
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("timescale: connecting: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("timescale: connecting: %w", err)
	}

	t, err := newTimescale(ctx, pgxDatabase{pool: pool}, pool, cfg)
	if err != nil {
		pool.Close()
		return nil, err
	}
	return t, nil
}

// NewTimescaleWithDatabase returns a sink writing through db, which the caller
// owns and must keep open until Close has returned.
//
// It is the seam tests use, and the way an embedding application can share a
// pool it already manages.
func NewTimescaleWithDatabase(ctx context.Context, db TimescaleDatabase, cfg TimescaleConfig) (*Timescale, error) {
	return newTimescale(ctx, db, nil, cfg)
}

func newTimescale(ctx context.Context, db TimescaleDatabase, pool *pgxpool.Pool, cfg TimescaleConfig) (*Timescale, error) {
	if db == nil {
		return nil, fmt.Errorf("timescale: a database is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	hypertable, err := ensureTimescaleSchema(ctx, db, cfg)
	if err != nil {
		return nil, err
	}

	t := &Timescale{
		db:         db,
		cfg:        cfg,
		pool:       pool,
		hypertable: hypertable,
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
	}
	go t.run()
	return t, nil
}

// Hypertable reports whether the destination is a TimescaleDB hypertable
// rather than a plain PostgreSQL table.
func (t *Timescale) Hypertable() bool { return t.hypertable }

// Emit implements stream.Sink, buffering the row and copying the batch once it
// reaches the configured size.
//
// A background flush failure is surfaced here rather than discarded, so a
// database that has gone away stops the stream instead of silently dropping
// rows.
func (t *Timescale) Emit(r stream.Record) error {
	row, err := timescaleRow(r)
	if err != nil {
		return err
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return ErrTimescaleClosed
	}
	if t.pendingErr != nil {
		err := t.pendingErr
		t.pendingErr = nil
		return err
	}

	t.rows = append(t.rows, row)
	if len(t.rows) < t.cfg.BatchSize {
		return nil
	}
	return t.flushLocked(context.Background())
}

// Flush implements stream.Sink by copying any buffered rows immediately.
func (t *Timescale) Flush() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil
	}
	err := t.flushLocked(context.Background())
	if err == nil {
		t.pendingErr = nil
	}
	return err
}

// Close flushes the final batch, stops the background flusher and releases the
// connection pool. It is safe to call more than once, and always waits for the
// goroutine it starts, so no goroutine outlives the sink.
func (t *Timescale) Close() error {
	t.closeOnce.Do(func() {
		close(t.stop)
		<-t.done

		t.mu.Lock()
		t.closed = true
		err := t.flushLocked(context.Background())
		t.mu.Unlock()

		if t.pool != nil {
			t.pool.Close()
		}
		t.closeErr = err
	})
	return t.closeErr
}

// Written reports how many rows have been copied to the database.
func (t *Timescale) Written() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.written
}

// Flushes reports how many COPY transactions have succeeded.
func (t *Timescale) Flushes() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.flushes
}

// run performs the periodic flush. It owns no state beyond calling
// flushLocked, and returns as soon as Close signals it, which is what makes
// shutdown deterministic.
func (t *Timescale) run() {
	defer close(t.done)

	ticker := time.NewTicker(t.cfg.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-t.stop:
			return
		case <-ticker.C:
			t.mu.Lock()
			if err := t.flushLocked(context.Background()); err != nil && t.pendingErr == nil {
				// No caller to return to from a timer; remember it so the
				// next Emit can fail the stream instead of hiding the error.
				t.pendingErr = err
			}
			t.mu.Unlock()
		}
	}
}

// flushLocked copies the buffered rows. The caller must hold t.mu.
//
// A failed COPY leaves the buffer untouched: the batch inserted nothing, so
// retrying it cannot duplicate rows.
func (t *Timescale) flushLocked(ctx context.Context) error {
	if len(t.rows) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, t.cfg.FlushTimeout)
	defer cancel()

	rows := t.rows
	n, err := t.db.CopyRows(ctx, t.cfg.Table, timescaleColumns, rows)
	if err != nil {
		return fmt.Errorf("timescale: writing %d rows to %s: %w", len(rows), t.cfg.Table, err)
	}
	if n != int64(len(rows)) {
		return fmt.Errorf("timescale: wrote %d of %d rows to %s", n, len(rows), t.cfg.Table)
	}

	// Drop the references so a long-lived sink does not pin flushed rows.
	for i := range rows {
		rows[i] = nil
	}
	t.rows = rows[:0]
	t.written += n
	t.flushes++
	return nil
}

// timescaleColumns is the COPY column order, matched by timescaleRow.
var timescaleColumns = []string{
	"id",
	"ledger",
	"kind",
	"closed_at",
	"hash",
	"contract_id",
	"tx_hash",
	"tx_index",
	"op_index",
	"successful",
	"topics_xdr",
	"value_xdr",
	"envelope_xdr",
	"result_xdr",
	"result_meta_xdr",
	"header_xdr",
	"metadata_xdr",
}

// timescaleRow flattens a record into the column order above.
//
// Fields that do not apply to the record's kind are left NULL rather than
// zero-valued, so an analytical query can tell "no transaction here" from
// "a transaction that failed".
func timescaleRow(r stream.Record) ([]any, error) {
	row := make([]any, len(timescaleColumns))
	row[0] = r.ID
	row[1] = int64(r.Sequence)
	row[2] = string(r.Kind)
	if !r.Time.IsZero() {
		row[3] = r.Time.UTC()
	}

	switch r.Kind {
	case stream.KindEvent:
		e := r.Event
		if e == nil {
			return nil, fmt.Errorf("timescale: event record %s has no payload", r.ID)
		}
		row[5] = nullString(e.ContractID)
		row[6] = nullString(e.TxHash)
		row[7] = int64(e.TxIndex)
		row[8] = int64(e.OpIndex)
		if len(e.TopicXDR) > 0 {
			row[10] = e.TopicXDR
		}
		row[11] = nullString(e.ValueXDR)

	case stream.KindLedger:
		c := r.Close
		if c == nil {
			return nil, fmt.Errorf("timescale: ledger record %s has no payload", r.ID)
		}
		row[4] = nullString(c.Hash)
		row[15] = nullString(c.HeaderXDR)
		row[16] = nullString(c.MetadataXDR)

	case stream.KindTransaction:
		tx := r.Transaction
		if tx == nil {
			return nil, fmt.Errorf("timescale: transaction record %s has no payload", r.ID)
		}
		row[6] = nullString(tx.Hash)
		row[7] = int64(tx.Index)
		row[9] = tx.Successful
		row[12] = nullString(tx.EnvelopeXDR)
		row[13] = nullString(tx.ResultXDR)
		row[14] = nullString(tx.ResultMetaXDR)

	default:
		return nil, fmt.Errorf("timescale: unknown record kind %q", r.Kind)
	}

	return row, nil
}

// nullString maps an absent string to SQL NULL, keeping "no value" distinct
// from an empty one in analytical queries.
func nullString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// ensureTimescaleSchema creates the destination table and, when possible,
// turns it into a hypertable partitioned by ledger. It reports whether the
// table is a hypertable.
func ensureTimescaleSchema(ctx context.Context, db TimescaleDatabase, cfg TimescaleConfig) (bool, error) {
	ident, err := parseTableName(cfg.Table)
	if err != nil {
		return false, err
	}

	if err := db.Exec(ctx, createTableSQL(ident)); err != nil {
		return false, fmt.Errorf("timescale: creating table %s: %w", cfg.Table, err)
	}
	if cfg.NoHypertable {
		return false, nil
	}

	// On a plain PostgreSQL server create_hypertable does not exist at all.
	// That is a supported deployment, not a failure, so the sink degrades to
	// a plain table and says so through Hypertable.
	if err := db.Exec(ctx, hypertableSQL(cfg.Table)); err != nil {
		if timescaleUnavailable(err) {
			return false, nil
		}
		return false, fmt.Errorf("timescale: creating hypertable on %s: %w", cfg.Table, err)
	}
	return true, nil
}

func createTableSQL(ident pgx.Identifier) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	id            TEXT        NOT NULL,
	ledger        BIGINT      NOT NULL,
	kind          TEXT        NOT NULL,
	closed_at     TIMESTAMPTZ,
	hash          TEXT,
	contract_id   TEXT,
	tx_hash       TEXT,
	tx_index      INTEGER,
	op_index      INTEGER,
	successful    BOOLEAN,
	topics_xdr    TEXT[],
	value_xdr     TEXT,
	envelope_xdr  TEXT,
	result_xdr    TEXT,
	result_meta_xdr TEXT,
	header_xdr    TEXT,
	metadata_xdr  TEXT,
	PRIMARY KEY (id, ledger)
)`, ident.Sanitize())
}

// hypertableSQL partitions by ledger rather than time: the ledger sequence is
// always present, is the natural query dimension for chain data, and never
// needs a fabricated timestamp for a record the server did not date.
func hypertableSQL(table string) string {
	return fmt.Sprintf(
		`SELECT create_hypertable('%s', 'ledger', chunk_time_interval => %d, if_not_exists => TRUE)`,
		escapeLiteral(table), timescaleChunkInterval)
}

// timescaleUnavailable distinguishes "this server is not TimescaleDB" from a
// genuine failure. Anything else is reported.
func timescaleUnavailable(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	switch pgErr.Code {
	case "42883", // undefined_function: create_hypertable is absent
		"58P01", // undefined_file: the extension is not installed
		"0A000", // feature_not_supported
		"42501": // insufficient_privilege: cannot install the extension
		return true
	}
	return false
}

// parseTableName validates a table reference and turns it into a safely
// quoted identifier. Only letters, digits and underscores are allowed, so a
// hostile --timescale-table value cannot reach the generated DDL as syntax.
func parseTableName(table string) (pgx.Identifier, error) {
	parts := strings.Split(table, ".")
	if len(parts) > 2 {
		return nil, fmt.Errorf("timescale: table %q must be a name or schema.name", table)
	}
	for _, p := range parts {
		if !validIdentifier(p) {
			return nil, fmt.Errorf("timescale: table %q is not a valid identifier", table)
		}
	}
	return pgx.Identifier(parts), nil
}

func validIdentifier(s string) bool {
	if s == "" || len(s) > 63 {
		return false
	}
	for i, r := range s {
		if r == '_' || unicode.IsLetter(r) {
			continue
		}
		if unicode.IsDigit(r) && i > 0 {
			continue
		}
		return false
	}
	return true
}

func escapeLiteral(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// pgxDatabase adapts a pgx pool to TimescaleDatabase.
type pgxDatabase struct {
	pool *pgxpool.Pool
}

func (d pgxDatabase) Exec(ctx context.Context, sql string) error {
	if _, err := d.pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("timescale: exec: %w", err)
	}
	return nil
}

// timescaleStageTable is the per-transaction landing table for COPY. It lives
// in pg_temp and is dropped on commit.
const timescaleStageTable = "prism_copy_stage"

// CopyRows copies the batch inside a transaction, so a failure part way
// through inserts nothing and the sink can safely retry the same rows.
//
// COPY cannot express ON CONFLICT, so rows land in a temporary table first and
// are merged with ON CONFLICT DO NOTHING. That is what makes a replayed record
// after a restart a no-op instead of a fatal duplicate-key error.
func (d pgxDatabase) CopyRows(ctx context.Context, table string, columns []string, rows [][]any) (n int64, err error) {
	ident, err := parseTableName(table)
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}

	quotedCols := make([]string, len(columns))
	for i, c := range columns {
		if !validIdentifier(c) {
			return 0, fmt.Errorf("timescale: invalid column name %q", c)
		}
		quotedCols[i] = pgx.Identifier{c}.Sanitize()
	}
	colList := strings.Join(quotedCols, ", ")

	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("timescale: begin: %w", err)
	}
	defer func() {
		// A rollback after a successful commit returns ErrTxClosed, which is
		// expected. A genuine rollback failure only matters when nothing else
		// has already gone wrong, so it never masks the original error.
		if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) && err == nil {
			err = fmt.Errorf("timescale: rollback: %w", rbErr)
		}
	}()

	stage := pgx.Identifier{timescaleStageTable}.Sanitize()
	dest := ident.Sanitize()
	if _, err := tx.Exec(ctx, fmt.Sprintf(
		"CREATE TEMP TABLE %s (LIKE %s INCLUDING DEFAULTS) ON COMMIT DROP", stage, dest)); err != nil {
		return 0, fmt.Errorf("timescale: creating staging table: %w", err)
	}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{timescaleStageTable}, columns, pgx.CopyFromRows(rows)); err != nil {
		return 0, fmt.Errorf("timescale: copy: %w", err)
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s (%s) SELECT %s FROM %s ON CONFLICT DO NOTHING",
		dest, colList, colList, stage)); err != nil {
		return 0, fmt.Errorf("timescale: merge: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("timescale: commit: %w", err)
	}

	// Rows already present are counted as processed, not as an error: the
	// sink's contract is at-least-once delivery.
	return int64(len(rows)), nil
}
