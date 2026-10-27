package cli

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
)

// streamTransaction builds one record for a fake transaction universe, with an
// explicit application order so a ledger can hold several.
func streamTransaction(ledger uint32, order int32, hash, status string) protocol.TransactionInfo {
	return protocol.TransactionInfo{
		TransactionDetails: protocol.TransactionDetails{
			Status:           status,
			TransactionHash:  hash,
			ApplicationOrder: order,
			Ledger:           ledger,
		},
		LedgerCloseTime: int64(1700000000 + ledger),
	}
}

// streamTransactions builds per transactions in each of ledgers consecutive
// ledgers, in the order the server serves them.
func streamTransactions(startLedger uint32, ledgers, per int) []protocol.TransactionInfo {
	out := make([]protocol.TransactionInfo, 0, ledgers*per)
	for l := range ledgers {
		ledger := startLedger + uint32(l) //nolint:gosec // test data
		for order := 1; order <= per; order++ {
			out = append(out, streamTransaction(ledger, int32(order), //nolint:gosec // test data
				fmt.Sprintf("%064x", uint64(ledger)<<20|uint64(order)), //nolint:gosec // test data
				protocol.TransactionStatusSuccess))
		}
	}
	return out
}

// streamRecordID is the ID the source synthesises for a record.
func streamRecordID(ledger uint32, order int32) string {
	return fmt.Sprintf("%010d-%07d", ledger, order)
}

// collectTransactionRecords decodes a stream and returns its ids, asserting the
// ordering and uniqueness every stream promises along the way.
func collectTransactionRecords(t *testing.T, stdout string) []string {
	t.Helper()

	lines := nonEmptyLines(stdout)
	ids := make([]string, 0, len(lines))
	for i, line := range lines {
		var rec struct {
			Kind   string `json:"kind"`
			ID     string `json:"id"`
			Ledger uint32 `json:"ledger"`
			Hash   string `json:"hash"`
		}
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("line %d is not valid JSON: %v\n%s", i, err, line)
		}
		if rec.Kind != "transaction" {
			t.Errorf("line %d kind = %q, want transaction", i, rec.Kind)
		}
		if rec.ID == "" || rec.Hash == "" {
			t.Errorf("line %d has no id or hash: %s", i, line)
		}
		if len(ids) > 0 && rec.ID <= ids[len(ids)-1] {
			t.Fatalf("line %d (%s) does not increase past %s", i, rec.ID, ids[len(ids)-1])
		}
		ids = append(ids, rec.ID)
	}
	return ids
}

// TestTransactionsCommandPaginatesThroughBackfill covers the paging loop end to
// end: many pages, no duplicates, no gaps.
func TestTransactionsCommandPaginatesThroughBackfill(t *testing.T) {
	txns := streamTransactions(9000, 10, 2)
	srv := fakerpc.NewTransactionServer(txns, fakerpc.WithPageLimit(3))
	withFake(t, srv)

	stdout, _, err := run(t, "transactions", "--from", "9000", "--json", "--page-size", "3")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	ids := collectTransactionRecords(t, stdout)
	if len(ids) != len(txns) {
		t.Fatalf("emitted %d records, want %d\n%s", len(ids), len(txns), stdout)
	}
	for i, tx := range txns {
		if want := streamRecordID(tx.Ledger, tx.ApplicationOrder); ids[i] != want {
			t.Errorf("record %d = %q, want %q", i, ids[i], want)
		}
	}
}

func TestTransactionsCommandLimit(t *testing.T) {
	srv := fakerpc.NewTransactionServer(streamTransactions(5000, 20, 3), fakerpc.WithPageLimit(10))
	withFake(t, srv)

	stdout, _, err := run(t, "transactions", "--from", "5000", "--json", "--limit", "4")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := len(collectTransactionRecords(t, stdout)); got != 4 {
		t.Errorf("emitted %d records, want 4", got)
	}
}

// TestTransactionsCommandStartsAtHead covers the no---from path.
func TestTransactionsCommandStartsAtHead(t *testing.T) {
	txns := streamTransactions(8000, 4, 2)
	srv := fakerpc.NewTransactionServer(txns, fakerpc.WithPageLimit(10))
	withFake(t, srv)

	stdout, _, err := run(t, "transactions", "--json")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	lines := nonEmptyLines(stdout)
	if len(lines) != 2 {
		t.Fatalf("emitted %d records starting at head, want the head ledger's 2\n%s", len(lines), stdout)
	}
	for _, line := range lines {
		var rec struct {
			Ledger uint32 `json:"ledger"`
		}
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("not valid JSON: %v", err)
		}
		if rec.Ledger != 8003 {
			t.Errorf("record from ledger %d, want 8003", rec.Ledger)
		}
	}
}

// TestTransactionsCommandSurvivesDrops covers reconnection at the CLI level: a
// dropped connection and a replayed page must not lose or duplicate anything.
func TestTransactionsCommandSurvivesDrops(t *testing.T) {
	txns := streamTransactions(6000, 8, 3)
	srv := fakerpc.NewTransactionServer(txns,
		fakerpc.WithPageLimit(4),
		fakerpc.WithScript(map[int]fakerpc.Behaviour{
			1: {Err: fakerpc.ErrDropped},
			4: {ReplayLast: true},
		}),
	)
	withFake(t, srv)

	stdout, _, err := run(t, "transactions", "--from", "6000", "--json")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	ids := collectTransactionRecords(t, stdout)
	if len(ids) != len(txns) {
		t.Fatalf("emitted %d records after a drop, want %d\n%s", len(ids), len(txns), stdout)
	}
}

// TestTransactionsCommandGapExitsTwo covers the documented exit-code contract
// for the one case where transaction records are genuinely lost.
func TestTransactionsCommandGapExitsTwo(t *testing.T) {
	srv := fakerpc.NewTransactionServer(streamTransactions(7000, 20, 2),
		fakerpc.WithPageLimit(2),
		fakerpc.WithScript(map[int]fakerpc.Behaviour{
			1: {OldestLedger: 9000000},
		}),
	)
	withFake(t, srv)

	_, stderr, err := run(t, "transactions", "--from", "7000", "--json", "--page-size", "2")
	if err == nil {
		t.Fatal("Execute() error = nil, want a gap failure")
	}
	if code, printable := ExitCode(err); code != 2 || printable {
		t.Errorf("ExitCode() = (%d, %v), want (2, false)", code, printable)
	}
	for _, want := range []string{"gap", "missed", "--from"} {
		if !strings.Contains(stderr, want) {
			t.Errorf("stderr does not mention %q\n%s", want, stderr)
		}
	}
}

func TestTransactionsCommandBackfillWorkers(t *testing.T) {
	txns := streamTransactions(4000, 10, 3)
	srv := fakerpc.NewTransactionServer(txns, fakerpc.WithPageLimit(4))
	withFake(t, srv)

	stdout, _, err := run(t, "transactions", "--from", "4000", "--json",
		"--page-size", "4", "--backfill-workers", "3")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	if got := len(collectTransactionRecords(t, stdout)); got != len(txns) {
		t.Errorf("emitted %d records with backfill workers, want %d\n%s", got, len(txns), stdout)
	}
}

// TestTransactionsCommandShortPageKeepsDraining covers the server's ledger scan
// cap: a page smaller than the requested size, that has not reached the head,
// must not end a backfill.
func TestTransactionsCommandShortPageKeepsDraining(t *testing.T) {
	txns := []protocol.TransactionInfo{
		streamTransaction(100, 1, "early", protocol.TransactionStatusSuccess),
		streamTransaction(900, 1, "late", protocol.TransactionStatusSuccess),
	}
	// One record per call, independently of what the client asks for.
	srv := fakerpc.NewTransactionServer(txns, fakerpc.WithPageLimit(1))
	withFake(t, srv)

	stdout, _, err := run(t, "transactions", "--from", "100", "--json", "--page-size", "10")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	ids := collectTransactionRecords(t, stdout)
	want := []string{streamRecordID(100, 1), streamRecordID(900, 1)}
	if len(ids) != len(want) {
		t.Fatalf("drained %d records, want the short page to keep draining\n%s", len(ids), stdout)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Errorf("record %d = %q, want %q", i, ids[i], want[i])
		}
	}
}
