package cli

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" driver used by --pg-dsn

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// pgDriverName is the database/sql driver behind --pg-dsn, registered by the
// blank import above.
const pgDriverName = "pgx"

// envPGDSN supplies a default for --pg-dsn, mirroring $SOROBAN_RPC_URL for
// --rpc-url, so an ingestion host can be configured once instead of in every
// invocation.
const envPGDSN = "PRISM_PG_DSN"

// openCursorStore opens the durable cursor store implied by the flags.
//
// It is a variable so tests can substitute an in-memory implementation and
// exercise restart behaviour end to end with no database and no network;
// production code never reassigns it.
//
// Persistence is strictly opt-in: with no DSN nothing is opened, no
// connection is attempted, and the command behaves exactly as it did before
// durable cursors existed. When it is enabled, the table is created up front
// so a misconfigured store fails at startup rather than after the first page.
//
// The returned closer releases the connection pool and must be called; it is
// nil when persistence is disabled.
var openCursorStore = func(ctx context.Context, dsn string) (stream.CursorStore, func() error, error) {
	if dsn == "" {
		return nil, nil, nil
	}

	db, err := sql.Open(pgDriverName, dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("opening the PostgreSQL cursor store: %w", err)
	}
	// A tailer writes from one goroutine, so a small pool is plenty: one idle
	// connection stays warm, the pool can never grow without bound, and idle
	// connections are reaped so a long-lived follow does not pin sockets.
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)
	db.SetConnMaxIdleTime(time.Minute)

	store, err := stream.NewPGCursorStore(db, stream.PGStoreConfig{})
	if err != nil {
		return nil, nil, closeAndJoin(db, err)
	}
	if err := store.EnsureSchema(ctx); err != nil {
		return nil, nil, closeAndJoin(db, fmt.Errorf("preparing the PostgreSQL cursor store: %w", err))
	}
	return store, db.Close, nil
}

// closeAndJoin releases db and folds any close failure into cause, so an
// error path neither leaks the pool nor swallows a second failure.
func closeAndJoin(db *sql.DB, cause error) error {
	if err := db.Close(); err != nil {
		return errors.Join(cause, fmt.Errorf("closing the PostgreSQL cursor store: %w", err))
	}
	return cause
}

// cursorKey derives the row name a stream's position is stored under.
//
// The command and everything that selects records are folded in, so two runs
// resume each other only when they describe the same stream: watching a
// different contract — or the same contract under a different topic — gets its
// own row instead of silently inheriting another stream's position. Parts are
// tagged and joined with a byte that cannot occur in a contract ID, a topic
// filter or an event type, so one stream's parts can never re-spell another's.
//
// The result is deliberately opaque. Filters are long base64 blobs, and a slug
// built from them would be neither stable nor readable; --pg-cursor-key is the
// escape hatch for a name a human chose.
func cursorKey(command string, parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(append([]string{command}, parts...), "\x00")))
	return command + "/" + hex.EncodeToString(sum[:8])
}

// fromKeyPart tags --from for cursorKey. It is the raw flag value, not the
// resolved ledger: an unset --from means "start at the head", which is a
// different request from a concrete ledger and must not share its row.
func fromKeyPart(from uint32) string {
	return "from=" + strconv.FormatUint(uint64(from), 10)
}
