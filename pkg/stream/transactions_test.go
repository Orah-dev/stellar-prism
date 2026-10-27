package stream_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/toid"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar-optics/stellar-prism/pkg/rpc"
	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// makeTransactions builds per transactions in each of ledgers consecutive
// ledgers starting at startLedger, in the order the server serves them.
func makeTransactions(startLedger uint32, ledgers, per int) []protocol.TransactionInfo {
	out := make([]protocol.TransactionInfo, 0, ledgers*per)
	for l := range ledgers {
		ledger := startLedger + uint32(l) //nolint:gosec // test data
		for order := 1; order <= per; order++ {
			out = append(out, protocol.TransactionInfo{
				TransactionDetails: protocol.TransactionDetails{
					Status:           protocol.TransactionStatusSuccess,
					TransactionHash:  fmt.Sprintf("%064x", uint64(ledger)<<20|uint64(order)), //nolint:gosec // test data
					ApplicationOrder: int32(order),                                           //nolint:gosec // test data
					Ledger:           ledger,
				},
				LedgerCloseTime: int64(1700000000 + ledger),
			})
		}
	}
	return out
}

// transactionRecordID is the ID the source must synthesise for a record: the
// same fixed-width ledger-application-order pair the source uses internally.
func transactionRecordID(tx protocol.TransactionInfo) string {
	return fmt.Sprintf("%010d-%07d", tx.Ledger, tx.ApplicationOrder)
}

func transactionRecordIDs(txns []protocol.TransactionInfo) []string {
	out := make([]string, 0, len(txns))
	for _, tx := range txns {
		out = append(out, transactionRecordID(tx))
	}
	return out
}

// toidCursor is the cursor the real RPC returns for a transaction: a SEP-35
// total-order ID in decimal, with no separator.
func toidCursor(ledger uint32, applicationOrder int32) string {
	return strconv.FormatInt(toid.New(int32(ledger), applicationOrder, 1).ToInt64(), 10) //nolint:gosec // test data
}

func runTransactionTail(t *testing.T, srv *fakerpc.Server, opts stream.Options, q stream.TransactionQuery) (*collector, *fakeClock, error) {
	t.Helper()

	src := stream.NewTransactionSource(srv, q)
	sink := &collector{}
	clock := newFakeClock()
	opts.Clock = clock

	tl := stream.NewTailer(src, sink, opts)
	runErr := tl.Run(context.Background())
	return sink, clock, runErr
}

// TestTransactionCursorLedger covers the cursor format gap detection depends
// on. The RPC cursor is a SEP-35 total-order ID, not a ledger sequence, and
// reading it as one disables gap detection without failing anything visible.
func TestTransactionCursorLedger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		cursor string
		want   uint32
		ok     bool
	}{
		{name: "empty", cursor: "", ok: false},
		{name: "junk", cursor: "not-a-cursor", ok: false},
		{name: "negative", cursor: "-1", ok: false},
		{name: "zero", cursor: "0", ok: false},
		{name: "first transaction", cursor: toidCursor(1234, 1), want: 1234, ok: true},
		{name: "later transaction", cursor: toidCursor(1234, 57), want: 1234, ok: true},
		{name: "mainnet-sized ledger", cursor: toidCursor(4000000, 3), want: 4000000, ok: true},
		{name: "plain ledger sequence", cursor: "4000000", want: 4000000, ok: true},
		{name: "compound cursor", cursor: "0039660000-0000001", want: 39660000, ok: true},
	}

	src := stream.NewTransactionSource(fakerpc.NewTransactionServer(nil), stream.TransactionQuery{})
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, ok := src.CursorLedger(tc.cursor)
			if ok != tc.ok {
				t.Fatalf("CursorLedger(%q) ok = %v, want %v", tc.cursor, ok, tc.ok)
			}
			if ok && got != tc.want {
				t.Errorf("CursorLedger(%q) = %d, want %d", tc.cursor, got, tc.want)
			}
		})
	}
}

func TestTransactionFetchStartsAtLedger(t *testing.T) {
	t.Parallel()

	txns := makeTransactions(500, 4, 2)
	srv := fakerpc.NewTransactionServer(txns, fakerpc.WithPageLimit(10))
	src := stream.NewTransactionSource(srv, stream.TransactionQuery{StartLedger: 501, PageLimit: 10})

	batch, err := src.Fetch(context.Background(), "")
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if len(batch.Records) == 0 {
		t.Fatal("fetched no records")
	}
	for _, r := range batch.Records {
		if r.Sequence < 501 {
			t.Errorf("record from ledger %d, want ledger >= 501", r.Sequence)
		}
	}
	if batch.Oldest != 500 || batch.Latest != 503 {
		t.Errorf("bounds = oldest %d latest %d, want 500/503", batch.Oldest, batch.Latest)
	}
}

// TestTransactionFetchResumesFromCursor checks the cursor-only path, which is
// what every call after the first uses.
func TestTransactionFetchResumesFromCursor(t *testing.T) {
	t.Parallel()

	txns := makeTransactions(700, 3, 4)
	srv := fakerpc.NewTransactionServer(txns, fakerpc.WithPageLimit(10))
	src := stream.NewTransactionSource(srv, stream.TransactionQuery{StartLedger: 700, PageLimit: 4})

	first, err := src.Fetch(context.Background(), "")
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if first.NextCursor == "" {
		t.Fatal("first page returned no cursor")
	}
	if !first.Full {
		t.Error("first page reported not full, but four more records are waiting")
	}
	// The cursor must be the server's own token shape, or the source is
	// testing pagination against a fiction.
	if _, err := strconv.ParseInt(first.NextCursor, 10, 64); err != nil {
		t.Errorf("cursor %q is not the RPC's decimal paging token", first.NextCursor)
	}

	second, err := src.Fetch(context.Background(), first.NextCursor)
	if err != nil {
		t.Fatalf("Fetch() with cursor error = %v", err)
	}
	if len(second.Records) == 0 {
		t.Fatal("second page returned no records")
	}
	if second.Records[0].ID <= first.Records[len(first.Records)-1].ID {
		t.Errorf("second page starts at %s, which is not after %s",
			second.Records[0].ID, first.Records[len(first.Records)-1].ID)
	}
}

// TestTransactionFetchRejectsBadCursor keeps a corrupt cursor out of the retry
// loop: no amount of backoff can turn it into a valid one.
func TestTransactionFetchRejectsBadCursor(t *testing.T) {
	t.Parallel()

	tests := []string{"not-a-cursor", "-17", " "}
	for _, cursor := range tests {
		t.Run(cursor, func(t *testing.T) {
			t.Parallel()

			srv := fakerpc.NewTransactionServer(makeTransactions(100, 1, 1))
			src := stream.NewTransactionSource(srv, stream.TransactionQuery{StartLedger: 100})

			_, err := src.Fetch(context.Background(), cursor)
			if err == nil {
				t.Fatalf("Fetch(%q) error = nil, want a rejection", cursor)
			}
			if !rpc.IsPermanent(err) {
				t.Errorf("Fetch(%q) error = %v, want it marked permanent", cursor, err)
			}
			if rpc.IsRetryable(err) {
				t.Errorf("Fetch(%q) error = %v, want it non-retryable", cursor, err)
			}
		})
	}
}

func TestTransactionFetchPropagatesServerErrors(t *testing.T) {
	t.Parallel()

	srv := fakerpc.NewTransactionServer(makeTransactions(100, 1, 1),
		fakerpc.WithScript(map[int]fakerpc.Behaviour{0: {Err: fakerpc.ErrDropped}}))
	src := stream.NewTransactionSource(srv, stream.TransactionQuery{StartLedger: 100})

	if _, err := src.Fetch(context.Background(), ""); !errors.Is(err, fakerpc.ErrDropped) {
		t.Fatalf("Fetch() error = %v, want it to wrap %v", err, fakerpc.ErrDropped)
	}
}

// TestTransactionRecordMapping pins the neutral Record the source produces.
func TestTransactionRecordMapping(t *testing.T) {
	t.Parallel()

	closeTime := int64(1751955324)
	txns := []protocol.TransactionInfo{{
		TransactionDetails: protocol.TransactionDetails{
			Status:           protocol.TransactionStatusFailed,
			TransactionHash:  "abc123",
			ApplicationOrder: 3,
			Ledger:           337272,
			EnvelopeXDR:      "env",
			ResultXDR:        "res",
			ResultMetaXDR:    "meta",
		},
		LedgerCloseTime: closeTime,
	}}
	srv := fakerpc.NewTransactionServer(txns)
	src := stream.NewTransactionSource(srv, stream.TransactionQuery{StartLedger: 337272})

	batch, err := src.Fetch(context.Background(), "")
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if len(batch.Records) != 1 {
		t.Fatalf("fetched %d records, want 1", len(batch.Records))
	}

	record := batch.Records[0]
	if record.Kind != stream.KindTransaction {
		t.Errorf("Kind = %q, want %q", record.Kind, stream.KindTransaction)
	}
	if record.Sequence != 337272 {
		t.Errorf("Sequence = %d, want 337272", record.Sequence)
	}
	if !record.Time.Equal(time.Unix(closeTime, 0).UTC()) {
		t.Errorf("Time = %v, want %v", record.Time, time.Unix(closeTime, 0).UTC())
	}
	if got, want := record.ID, transactionRecordID(txns[0]); got != want {
		t.Errorf("ID = %q, want %q", got, want)
	}

	payload := record.Transaction
	if payload == nil {
		t.Fatal("Transaction payload is nil")
	}
	if payload.Hash != "abc123" {
		t.Errorf("Hash = %q, want abc123", payload.Hash)
	}
	if payload.Successful {
		t.Error("Successful = true for a failed transaction")
	}
	if payload.EnvelopeXDR != "env" || payload.ResultXDR != "res" || payload.ResultMetaXDR != "meta" {
		t.Errorf("XDR payloads were not passed through: %+v", payload)
	}
}

// TestTransactionIDsFixedWidthAcrossPages is the ordering contract the
// de-duplication scheme rests on: constant width, lexicographic order equal to
// chronological order, including across a page boundary.
func TestTransactionIDsFixedWidthAcrossPages(t *testing.T) {
	t.Parallel()

	txns := makeTransactions(1000, 6, 5)
	srv := fakerpc.NewTransactionServer(txns, fakerpc.WithPageLimit(7))
	src := stream.NewTransactionSource(srv, stream.TransactionQuery{StartLedger: 1000, PageLimit: 7})

	var ids []string
	cursor := ""
	for page := 0; page < 20; page++ {
		batch, err := src.Fetch(context.Background(), cursor)
		if err != nil {
			t.Fatalf("Fetch() error = %v", err)
		}
		if len(batch.Records) == 0 {
			break
		}
		for _, r := range batch.Records {
			if len(r.ID) != len(transactionRecordID(txns[0])) {
				t.Fatalf("ID %q is not fixed-width", r.ID)
			}
			ids = append(ids, r.ID)
		}
		cursor = batch.NextCursor
	}

	if len(ids) != len(txns) {
		t.Fatalf("drained %d records, want %d", len(ids), len(txns))
	}
	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			t.Fatalf("IDs are not strictly increasing at %d: %q then %q", i, ids[i-1], ids[i])
		}
	}
}

func TestTransactionTailDeliversEveryRecord(t *testing.T) {
	t.Parallel()

	txns := makeTransactions(200, 8, 3)
	srv := fakerpc.NewTransactionServer(txns, fakerpc.WithPageLimit(5))

	sink, _, err := runTransactionTail(t, srv, stream.Options{},
		stream.TransactionQuery{StartLedger: 200, PageLimit: 5})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	assertStreamInvariants(t, sink.ids, transactionRecordIDs(txns))
}

// TestTransactionTailSurvivesDrops covers the retry path: the cursor is reused
// unchanged, and a replayed page is suppressed.
func TestTransactionTailSurvivesDrops(t *testing.T) {
	t.Parallel()

	txns := makeTransactions(300, 10, 3)
	srv := fakerpc.NewTransactionServer(txns,
		fakerpc.WithPageLimit(4),
		fakerpc.WithScript(map[int]fakerpc.Behaviour{
			0: {Err: fakerpc.ErrDropped},
			3: {Err: fakerpc.ErrDropped},
			6: {ReplayLast: true},
		}),
	)

	sink, clock, err := runTransactionTail(t, srv, stream.Options{},
		stream.TransactionQuery{StartLedger: 300, PageLimit: 4})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	assertStreamInvariants(t, sink.ids, transactionRecordIDs(txns))
	if len(clock.slept) == 0 {
		t.Error("no backoff was applied despite dropped connections")
	}
}

// TestTransactionTailContinuesShortPageBeforeHead covers the RPC's ledger scan
// cap: a short page that has not reached the head is not the end of the
// stream, and treating it as one silently cuts a backfill short.
func TestTransactionTailContinuesShortPageBeforeHead(t *testing.T) {
	t.Parallel()

	txns := []protocol.TransactionInfo{
		{
			TransactionDetails: protocol.TransactionDetails{Ledger: 100, ApplicationOrder: 1, Status: protocol.TransactionStatusSuccess},
			LedgerCloseTime:    1700000100,
		},
		{
			TransactionDetails: protocol.TransactionDetails{Ledger: 900, ApplicationOrder: 1, Status: protocol.TransactionStatusSuccess},
			LedgerCloseTime:    1700000900,
		},
	}
	// One record per call, mimicking a server whose scan cap is smaller than
	// the client's requested page.
	srv := fakerpc.NewTransactionServer(txns, fakerpc.WithPageLimit(1))

	sink, clock, err := runTransactionTail(t, srv, stream.Options{},
		stream.TransactionQuery{StartLedger: 100, PageLimit: 10})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	assertStreamInvariants(t, sink.ids, transactionRecordIDs(txns))
	if len(clock.slept) != 0 {
		t.Errorf("slept %d time(s) while more records were waiting, want 0", len(clock.slept))
	}
	if calls := srv.Calls(); calls != 2 {
		t.Errorf("server saw %d calls, want 2 (one short page, then the page at the head)", calls)
	}
}

// TestTransactionTailDetectsRetentionGap is the regression test for the cursor
// format: with the cursor read as a plain ledger sequence, the retention check
// compares a 10^13-scale value against a ledger floor and never fires.
func TestTransactionTailDetectsRetentionGap(t *testing.T) {
	t.Parallel()

	txns := makeTransactions(1000, 30, 2)
	srv := fakerpc.NewTransactionServer(txns,
		fakerpc.WithPageLimit(2),
		fakerpc.WithScript(map[int]fakerpc.Behaviour{
			1: {OldestLedger: 1010},
		}),
	)

	src := stream.NewTransactionSource(srv, stream.TransactionQuery{StartLedger: 1000, PageLimit: 2})
	sink := &collector{}
	clock := newFakeClock()
	clock.maxSleeps = 50
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clock.onBudget = cancel

	err := stream.NewTailer(src, sink, stream.Options{Follow: true, Clock: clock}).Run(ctx)

	var gap *stream.GapError
	if !errors.As(err, &gap) {
		t.Fatalf("Run() error = %v, want a *stream.GapError", err)
	}
	if gap.Source != "transactions" {
		t.Errorf("GapError.Source = %q, want transactions", gap.Source)
	}
	if gap.Oldest != 1010 {
		t.Errorf("GapError.Oldest = %d, want 1010", gap.Oldest)
	}
	if gap.From != 1000 {
		t.Errorf("GapError.From = %d, want 1000", gap.From)
	}
}

// TestTransactionTailResumesAfterGap checks the same scenario under the resume
// policy: the source is repositioned and the stream continues from the oldest
// retained ledger, with the skipped range announced.
func TestTransactionTailResumesAfterGap(t *testing.T) {
	t.Parallel()

	txns := makeTransactions(2000, 20, 2)
	srv := fakerpc.NewTransactionServer(txns,
		fakerpc.WithPageLimit(2),
		fakerpc.WithScript(map[int]fakerpc.Behaviour{
			1: {OldestLedger: 2005},
		}),
	)

	src := stream.NewTransactionSource(srv, stream.TransactionQuery{StartLedger: 2000, PageLimit: 2})
	sink := &collector{}

	var events []stream.GapEvent
	opts := stream.Options{
		Clock:       newFakeClock(),
		GapStrategy: stream.GapResume,
		OnGap:       func(e stream.GapEvent) { events = append(events, e) },
	}
	if err := stream.NewTailer(src, sink, opts).Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	// The first page was emitted before the gap; the stream then resumes from
	// the retained floor, and nothing before the floor is re-fetched.
	all := transactionRecordIDs(txns)
	want := append([]string{}, all[:2]...)
	for i, tx := range txns {
		if tx.Ledger >= 2005 {
			want = append(want, all[i])
		}
	}
	assertStreamInvariants(t, sink.ids, want)

	if len(events) == 0 || !events[0].Recovered {
		t.Fatalf("OnGap = %+v, want a recovered gap event", events)
	}
	if events[0].Gap.Oldest != 2005 {
		t.Errorf("recovered gap reports oldest %d, want 2005", events[0].Gap.Oldest)
	}
}

func TestTransactionTailRespectsLimit(t *testing.T) {
	t.Parallel()

	txns := makeTransactions(100, 10, 4)
	srv := fakerpc.NewTransactionServer(txns, fakerpc.WithPageLimit(6))

	sink, _, err := runTransactionTail(t, srv, stream.Options{Limit: 9},
		stream.TransactionQuery{StartLedger: 100, PageLimit: 6})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	assertStreamInvariants(t, sink.ids, transactionRecordIDs(txns)[:9])
}

// TestTransactionDrainerMergesConcurrentWorkers exercises --backfill-workers
// with the transactions source: workers fetch disjoint ranges and the merge
// restores global order.
func TestTransactionDrainerMergesConcurrentWorkers(t *testing.T) {
	t.Parallel()

	txns := makeTransactions(400, 12, 3)
	srv := fakerpc.NewTransactionServer(txns, fakerpc.WithPageLimit(4))

	sink := &collector{}
	drainer, err := stream.NewDrainer(stream.DrainerConfig{
		NewSource: func(startLedger uint32) (stream.Source, error) {
			return stream.NewTransactionSource(srv, stream.TransactionQuery{
				StartLedger: startLedger,
				PageLimit:   4,
			}), nil
		},
		Sink:    sink,
		From:    400,
		Workers: 3,
		Clock:   newFakeClock(),
	})
	if err != nil {
		t.Fatalf("NewDrainer() error = %v", err)
	}

	stats, err := drainer.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	assertStreamInvariants(t, sink.ids, transactionRecordIDs(txns))
	if stats.Emitted != len(txns) {
		t.Errorf("Stats().Emitted = %d, want %d", stats.Emitted, len(txns))
	}
}

func TestTransactionSourceRewind(t *testing.T) {
	t.Parallel()

	txns := makeTransactions(600, 6, 1)
	srv := fakerpc.NewTransactionServer(txns)
	src := stream.NewTransactionSource(srv, stream.TransactionQuery{StartLedger: 600})

	src.Rewind(603)
	batch, err := src.Fetch(context.Background(), "")
	if err != nil {
		t.Fatalf("Fetch() error = %v", err)
	}
	if len(batch.Records) == 0 {
		t.Fatal("rewound source fetched nothing")
	}
	if batch.Records[0].Sequence != 603 {
		t.Errorf("first record after Rewind(603) is from ledger %d, want 603", batch.Records[0].Sequence)
	}
}
