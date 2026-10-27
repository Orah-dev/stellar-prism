package sink

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// TestPgxDatabaseReportsFailure exercises the production adapter's error paths
// without a server: connection refused is still a real PostgreSQL failure, and
// the invalid-input paths must reject before dialling at all.
func TestPgxDatabaseReportsFailure(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx,
		"postgres://prism:prism@127.0.0.1:1/prism?sslmode=disable&connect_timeout=1")
	if err != nil {
		t.Fatalf("pgxpool.New() error = %v", err)
	}
	defer pool.Close()

	db := pgxDatabase{pool: pool}

	if err := db.Exec(ctx, "SELECT 1"); err == nil {
		t.Error("Exec() error = nil against an unreachable server")
	}
	if _, err := db.CopyRows(ctx, "prism_records", timescaleColumns, [][]any{{"id", int64(1), "event"}}); err == nil {
		t.Error("CopyRows() error = nil against an unreachable server")
	}
	if _, err := db.CopyRows(ctx, "bad;name", timescaleColumns, [][]any{{1}}); err == nil {
		t.Error("CopyRows() accepted an invalid table name")
	}
	if _, err := db.CopyRows(ctx, "prism_records", []string{"bad;col"}, [][]any{{1}}); err == nil {
		t.Error("CopyRows() accepted an invalid column name")
	}
	if n, err := db.CopyRows(ctx, "prism_records", timescaleColumns, nil); err != nil || n != 0 {
		t.Errorf("CopyRows(no rows) = %d, %v, want 0, nil", n, err)
	}
}

// TestTimescaleIntegration runs against a real TimescaleDB when
// PRISM_TEST_TIMESCALE_DSN is set, for example:
//
//	PRISM_TEST_TIMESCALE_DSN='postgres://prism:prism@127.0.0.1:55432/prism?sslmode=disable' \
//	    go test -run TestTimescaleIntegration ./pkg/sink
//
// It is skipped otherwise, so the default suite stays offline.
func TestTimescaleIntegration(t *testing.T) {
	dsn := os.Getenv("PRISM_TEST_TIMESCALE_DSN")
	if dsn == "" {
		t.Skip("set PRISM_TEST_TIMESCALE_DSN to run against a TimescaleDB instance")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connecting to the test database: %v", err)
	}
	defer pool.Close()

	table := fmt.Sprintf("prism_it_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+table)
	})

	s, err := NewTimescale(ctx, dsn, TimescaleConfig{
		Table:         table,
		BatchSize:     2,
		FlushInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewTimescale() error = %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if !s.Hypertable() {
		t.Fatal("Hypertable() = false against a TimescaleDB server")
	}

	event := stream.Record{
		ID:       "0000000100-0000000001",
		Sequence: 100,
		Time:     time.Unix(1700000100, 0).UTC(),
		Kind:     stream.KindEvent,
		Event: &stream.Event{
			Type:       "contract",
			ContractID: "CBGSBAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAKKY3",
			TxHash:     "abc123",
			TxIndex:    1,
			TopicXDR:   []string{"AAAAAwAAAAc="},
			ValueXDR:   "AAAAAwAAAAc=",
		},
	}
	ledger := stream.Record{
		ID:       "0000000100",
		Sequence: 100,
		Time:     time.Unix(1700000100, 0).UTC(),
		Kind:     stream.KindLedger,
		Close:    &stream.LedgerClose{Hash: "deadbeef", MetadataXDR: "meta"},
	}
	tx := stream.Record{
		ID:       "0000000100-0002",
		Sequence: 100,
		Time:     time.Unix(1700000100, 0).UTC(),
		Kind:     stream.KindTransaction,
		Transaction: &stream.Transaction{
			Hash:       "feedface",
			Index:      2,
			Successful: false,
			ResultXDR:  "res",
		},
	}

	for _, r := range []stream.Record{event, ledger, tx} {
		if err := s.Emit(r); err != nil {
			t.Fatalf("Emit(%s) error = %v", r.ID, err)
		}
	}
	// A replay must merge rather than abort the batch.
	if err := s.Emit(event); err != nil {
		t.Fatalf("Emit(replayed event) error = %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&count); err != nil {
		t.Fatalf("counting rows: %v", err)
	}
	if count != 3 {
		t.Errorf("table holds %d rows, want 3 — the replayed record must be ignored", count)
	}

	var (
		contract string
		topics   []string
		value    string
	)
	if err := pool.QueryRow(ctx,
		"SELECT contract_id, topics_xdr, value_xdr FROM "+table+" WHERE id = $1", event.ID,
	).Scan(&contract, &topics, &value); err != nil {
		t.Fatalf("reading the event row: %v", err)
	}
	if contract != event.Event.ContractID {
		t.Errorf("contract_id = %q, want %q", contract, event.Event.ContractID)
	}
	if len(topics) != 1 || topics[0] != "AAAAAwAAAAc=" {
		t.Errorf("topics_xdr = %v, want the base64 topic array", topics)
	}
	if value != "AAAAAwAAAAc=" {
		t.Errorf("value_xdr = %q, want the base64 payload", value)
	}

	var hypertableName string
	if err := pool.QueryRow(ctx,
		`SELECT hypertable_name FROM timescaledb_information.hypertables WHERE hypertable_name = $1`, table,
	).Scan(&hypertableName); err != nil {
		t.Errorf("the table was not registered as a hypertable: %v", err)
	}

	// A fresh sink for the same table simulates a restart, where the tailer
	// legitimately re-emits the last records.
	restarted, err := NewTimescale(ctx, dsn, TimescaleConfig{Table: table, BatchSize: 100})
	if err != nil {
		t.Fatalf("NewTimescale() on restart error = %v", err)
	}
	defer func() { _ = restarted.Close() }()
	for _, r := range []stream.Record{event, ledger, tx} {
		if err := restarted.Emit(r); err != nil {
			t.Fatalf("Emit(%s) after restart error = %v", r.ID, err)
		}
	}
	if err := restarted.Flush(); err != nil {
		t.Fatalf("Flush() after restart error = %v", err)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&count); err != nil {
		t.Fatalf("counting rows after restart: %v", err)
	}
	if count != 3 {
		t.Errorf("table holds %d rows after a restart, want 3 — re-emission must be idempotent", count)
	}
}
