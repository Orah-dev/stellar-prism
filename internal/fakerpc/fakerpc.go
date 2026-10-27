// Package fakerpc provides a scriptable, in-memory Soroban RPC used to test
// the streaming state machine without a network.
//
// The fake holds a fixed universe of events and serves them through the same
// cursor semantics as the real server, so tests describe failures ("drop the
// third call", "replay the last page") rather than hand-assembling responses.
// That keeps the scenarios readable and makes it hard to accidentally write a
// test that passes against a fake the real server would never produce.
package fakerpc

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/toid"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// ErrDropped simulates a connection failure. It matches prism's retryable
// classification, so the tailer will back off and retry.
var ErrDropped = errors.New("connection reset by peer")

// Behaviour scripts how the fake misbehaves on a given call. The zero value
// serves the request normally.
type Behaviour struct {
	// Err is returned instead of a response.
	Err error
	// ReplayLast re-serves the previous page, simulating a server that
	// resends after a reconnect.
	ReplayLast bool
	// Reorder shuffles the page into descending order, simulating a server
	// that violates its ordering contract.
	Reorder bool
	// Empty serves an empty page while still reporting bounds, simulating a
	// caught-up stream.
	Empty bool
	// OldestLedger overrides the reported retention floor, which is how a
	// retention overrun (a real gap) is simulated.
	OldestLedger uint32
	// LatestLedger overrides the reported head, which is how a lagging
	// backend in a load-balanced pool is simulated.
	LatestLedger uint32
}

// Server is a fake Soroban RPC.
type Server struct {
	mu sync.Mutex

	// events is the full ordered universe of events the server knows about.
	events []protocol.EventInfo
	// ledgers is the full ordered universe of ledgers.
	ledgers []protocol.LedgerInfo

	// pageLimit caps how many records a single call returns, mimicking the
	// server-side limit independently of what the client asks for.
	pageLimit int
	// oldest and latest are the reported retention bounds.
	oldest, latest uint32

	// script maps a call index (starting at 0) to a Behaviour.
	script map[int]Behaviour
	// calls counts GetEvents/GetLedgers invocations.
	calls int
	// lastPage remembers what was served, for ReplayLast.
	lastEvents       []protocol.EventInfo
	lastLedgers      []protocol.LedgerInfo
	lastTransactions []protocol.TransactionInfo

	// tx maps a hash to a canned transaction response.
	tx map[string]protocol.GetTransactionResponse
	// transactions is the ordered universe served by a transaction fake.
	transactions []protocol.TransactionInfo
	// txs is a canned response for getTransactions, used when transactions
	// is empty.
	txs protocol.GetTransactionsResponse
	// entries maps an encoded ledger key to its canned result.
	entries         map[string]protocol.LedgerEntryResult
	simulations     []protocol.SimulateTransactionResponse
	simulationCalls int

	// health is the explicit getHealth response; healthSet distinguishes it
	// from an unconfigured zero value.
	health      protocol.GetHealthResponse
	healthSet   bool
	healthErr   error
	healthCalls int
	// healthScript stages a sequence of responses for periodic-check tests.
	healthScript []HealthBehaviour
}

// HealthBehaviour scripts one getHealth response. A non-nil Err takes
// precedence over Response.
type HealthBehaviour struct {
	// Response is returned as-is when Err is nil.
	Response protocol.GetHealthResponse
	// Err is returned instead of Response.
	Err error
}

// Option configures a Server.
type Option func(*Server)

// WithPageLimit caps the number of records returned per call.
func WithPageLimit(n int) Option {
	return func(s *Server) { s.pageLimit = n }
}

// WithScript installs per-call behaviours, keyed by zero-based call index.
func WithScript(script map[int]Behaviour) Option {
	return func(s *Server) { s.script = script }
}

// WithRetention sets the reported oldest retained ledger.
func WithRetention(oldest uint32) Option {
	return func(s *Server) { s.oldest = oldest }
}

// WithHealthResponse makes GetHealth return resp.
func WithHealthResponse(resp protocol.GetHealthResponse) Option {
	return func(s *Server) {
		s.health = resp
		s.healthSet = true
		s.healthErr = nil
		s.healthScript = nil
	}
}

// WithHealthError makes GetHealth return err. A nil error is ignored, so a
// table-driven test can pass an empty failure without clearing its response.
func WithHealthError(err error) Option {
	return func(s *Server) {
		if err == nil {
			return
		}
		s.healthErr = err
		s.healthSet = false
		s.healthScript = nil
	}
}

// WithHealthScript installs getHealth behaviours keyed by zero-based call
// index. The final entry repeats once the script is exhausted, which makes a
// periodic test terminate without timing guesswork.
func WithHealthScript(script []HealthBehaviour) Option {
	return func(s *Server) {
		s.healthScript = make([]HealthBehaviour, len(script))
		copy(s.healthScript, script)
		s.healthSet = false
		s.healthErr = nil
	}
}

// NewServer returns a fake serving the given events.
func NewServer(events []protocol.EventInfo, opts ...Option) *Server {
	s := &Server{
		events:    events,
		pageLimit: 100,
		script:    map[int]Behaviour{},
		tx:        map[string]protocol.GetTransactionResponse{},
		entries:   map[string]protocol.LedgerEntryResult{},
	}
	for _, o := range opts {
		o(s)
	}
	if len(events) > 0 {
		s.latest = uint32(events[len(events)-1].Ledger) //nolint:gosec // test data
		if s.oldest == 0 {
			s.oldest = uint32(events[0].Ledger) //nolint:gosec // test data
		}
	}
	return s
}

// NewTransactionServer returns a fake serving the given transactions with the
// same cursor semantics as the real getTransactions endpoint.
func NewTransactionServer(txs []protocol.TransactionInfo, opts ...Option) *Server {
	s := &Server{
		transactions: txs,
		pageLimit:    100,
		script:       map[int]Behaviour{},
		tx:           map[string]protocol.GetTransactionResponse{},
	}
	for _, o := range opts {
		o(s)
	}
	if len(txs) > 0 {
		s.latest = txs[len(txs)-1].Ledger
		if s.oldest == 0 {
			s.oldest = txs[0].Ledger
		}
	}
	return s
}

// NewLedgerServer returns a fake serving the given ledgers.
func NewLedgerServer(ledgers []protocol.LedgerInfo, opts ...Option) *Server {
	s := &Server{
		ledgers:   ledgers,
		pageLimit: 100,
		script:    map[int]Behaviour{},
		tx:        map[string]protocol.GetTransactionResponse{},
		entries:   map[string]protocol.LedgerEntryResult{},
	}
	for _, o := range opts {
		o(s)
	}
	if len(ledgers) > 0 {
		s.latest = ledgers[len(ledgers)-1].Sequence
		if s.oldest == 0 {
			s.oldest = ledgers[0].Sequence
		}
	}
	return s
}

// SetTransaction installs a canned response for GetTransaction.
func (s *Server) SetTransaction(hash string, resp protocol.GetTransactionResponse) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tx[hash] = resp
}

// SetTransactions installs a canned response for GetTransactions.
func (s *Server) SetTransactions(resp protocol.GetTransactionsResponse) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.txs = resp
}

// SetSimulations installs responses returned in order by simulateTransaction.
// The final response is reused after the sequence is exhausted.
func (s *Server) SetSimulations(responses []protocol.SimulateTransactionResponse) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.simulations = append([]protocol.SimulateTransactionResponse(nil), responses...)
	s.simulationCalls = 0
}

// SetLedgerEntries installs canned results keyed by encoded ledger key.
func (s *Server) SetLedgerEntries(entries map[string]protocol.LedgerEntryResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = entries
}

// SetHealth installs an explicit getHealth response.
func (s *Server) SetHealth(resp protocol.GetHealthResponse) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.health = resp
	s.healthSet = true
	s.healthErr = nil
	s.healthScript = nil
}

// SetHealthError makes every subsequent GetHealth call fail with err.
func (s *Server) SetHealthError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.healthErr = err
	s.healthSet = false
	s.healthScript = nil
}

// HealthCalls reports how many getHealth calls the fake has served.
func (s *Server) HealthCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.healthCalls
}

// SetContractWasm installs the instance and code entries for a contract whose
// WASM bytecode is wasm. It is the fixture that lets ABI resolution be tested
// without a network.
func (s *Server) SetContractWasm(contractID string, wasm []byte) error {
	entries, err := ContractWasmEntries(contractID, wasm)
	if err != nil {
		return err
	}
	s.SetLedgerEntries(entries)
	return nil
}

// ContractWasmEntries builds the two ledger entries a contract ABI resolver
// reads: the instance entry naming the WASM hash, and the code entry carrying
// the bytecode.
func ContractWasmEntries(contractID string, wasm []byte) (map[string]protocol.LedgerEntryResult, error) {
	raw, err := strkey.Decode(strkey.VersionByteContract, contractID)
	if err != nil {
		return nil, fmt.Errorf("decoding contract ID: %w", err)
	}
	if len(raw) != len(xdr.Hash{}) {
		return nil, fmt.Errorf("contract ID decodes to %d bytes, want %d", len(raw), len(xdr.Hash{}))
	}

	var id xdr.ContractId
	copy(id[:], raw)
	address := xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &id}
	hash := xdr.Hash(sha256.Sum256(wasm))

	instance := xdr.LedgerEntry{
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeContractData,
			ContractData: &xdr.ContractDataEntry{
				Contract:   address,
				Key:        xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyContractInstance},
				Durability: xdr.ContractDataDurabilityPersistent,
				Val: xdr.ScVal{
					Type: xdr.ScValTypeScvContractInstance,
					Instance: &xdr.ScContractInstance{
						Executable: xdr.ContractExecutable{
							Type:     xdr.ContractExecutableTypeContractExecutableWasm,
							WasmHash: &hash,
						},
					},
				},
			},
		},
	}

	code := xdr.LedgerEntry{
		Data: xdr.LedgerEntryData{
			Type: xdr.LedgerEntryTypeContractCode,
			ContractCode: &xdr.ContractCodeEntry{
				Hash: hash,
				Code: wasm,
			},
		},
	}

	instanceKey := xdr.LedgerKey{
		Type: xdr.LedgerEntryTypeContractData,
		ContractData: &xdr.LedgerKeyContractData{
			Contract:   address,
			Key:        xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyContractInstance},
			Durability: xdr.ContractDataDurabilityPersistent,
		},
	}
	codeKey := xdr.LedgerKey{
		Type:         xdr.LedgerEntryTypeContractCode,
		ContractCode: &xdr.LedgerKeyContractCode{Hash: hash},
	}

	entries := make(map[string]protocol.LedgerEntryResult, 2)
	for _, item := range []struct {
		key   xdr.LedgerKey
		entry xdr.LedgerEntry
	}{
		{key: instanceKey, entry: instance},
		{key: codeKey, entry: code},
	} {
		keyXDR, err := xdr.MarshalBase64(item.key)
		if err != nil {
			return nil, fmt.Errorf("encoding ledger key: %w", err)
		}
		dataXDR, err := xdr.MarshalBase64(item.entry)
		if err != nil {
			return nil, fmt.Errorf("encoding ledger entry: %w", err)
		}
		entries[keyXDR] = protocol.LedgerEntryResult{KeyXDR: keyXDR, DataXDR: dataXDR}
	}
	return entries, nil
}

// Calls reports how many stream calls have been made, so tests can assert on
// retry and drain behaviour.
func (s *Server) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// GetEvents implements rpc.Client.
func (s *Server) GetEvents(ctx context.Context, req protocol.GetEventsRequest) (protocol.GetEventsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return protocol.GetEventsResponse{}, err
	}

	call := s.calls
	s.calls++
	b := s.script[call]

	if b.Err != nil {
		return protocol.GetEventsResponse{}, b.Err
	}

	// Mirror the real server's rule that a cursor and a ledger range are
	// mutually exclusive. A tailer bug that sets both would otherwise pass
	// silently against the fake and fail only in production.
	hasCursor := req.Pagination != nil && req.Pagination.Cursor != nil
	if hasCursor && req.StartLedger != 0 {
		return protocol.GetEventsResponse{}, errors.New("ledger ranges and cursor cannot both be set")
	}

	oldest, latest := s.oldest, s.latest
	if b.OldestLedger != 0 {
		oldest = b.OldestLedger
	}
	if b.LatestLedger != 0 {
		latest = b.LatestLedger
	}

	respond := func(page []protocol.EventInfo) protocol.GetEventsResponse {
		s.lastEvents = page
		resp := protocol.GetEventsResponse{
			Events:       page,
			LatestLedger: latest,
			OldestLedger: oldest,
		}
		if len(page) > 0 {
			resp.Cursor = page[len(page)-1].ID
		} else if hasCursor {
			resp.Cursor = req.Pagination.Cursor.String()
		}
		return resp
	}

	if b.Empty {
		return respond(nil), nil
	}
	if b.ReplayLast {
		return respond(s.lastEvents), nil
	}

	// Select everything strictly after the cursor, or from the start ledger.
	var out []protocol.EventInfo
	for _, e := range s.events {
		if hasCursor {
			if e.ID > req.Pagination.Cursor.String() {
				out = append(out, e)
			}
			continue
		}
		if req.StartLedger == 0 || uint32(e.Ledger) >= req.StartLedger { //nolint:gosec // test data
			out = append(out, e)
		}
	}

	limit := s.pageLimit
	if req.Pagination != nil && req.Pagination.Limit > 0 && int(req.Pagination.Limit) < limit {
		limit = int(req.Pagination.Limit)
	}
	if len(out) > limit {
		out = out[:limit]
	}

	if b.Reorder {
		reversed := make([]protocol.EventInfo, len(out))
		copy(reversed, out)
		sort.Slice(reversed, func(i, j int) bool { return reversed[i].ID > reversed[j].ID })
		// Cursor still reflects the true high-water mark; only the page order
		// is wrong, which is exactly the hazard being simulated.
		s.lastEvents = reversed
		resp := protocol.GetEventsResponse{
			Events:       reversed,
			LatestLedger: latest,
			OldestLedger: oldest,
		}
		if len(out) > 0 {
			resp.Cursor = out[len(out)-1].ID
		}
		return resp, nil
	}

	return respond(out), nil
}

// GetEventsBatch implements rpc.Client, serving every request in order and
// stopping at the first scripted failure — the responses gathered before it
// are returned with the error so the caller can see how far the batch got.
func (s *Server) GetEventsBatch(ctx context.Context, reqs []protocol.GetEventsRequest) ([]protocol.GetEventsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(reqs) == 0 {
		return nil, nil
	}

	responses := make([]protocol.GetEventsResponse, 0, len(reqs))
	for _, req := range reqs {
		// Each entry in the batch is one call as far as the script is
		// concerned, so the indices advance in step with the requests.
		call := s.calls
		s.calls++
		b := s.script[call]

		if b.Err != nil {
			return responses, b.Err
		}

		hasCursor := req.Pagination != nil && req.Pagination.Cursor != nil
		if hasCursor && req.StartLedger != 0 {
			return responses, errors.New("ledger ranges and cursor cannot both be set")
		}

		oldest, latest := s.oldest, s.latest
		if b.OldestLedger != 0 {
			oldest = b.OldestLedger
		}
		if b.LatestLedger != 0 {
			latest = b.LatestLedger
		}

		respond := func(page []protocol.EventInfo) protocol.GetEventsResponse {
			s.lastEvents = page
			resp := protocol.GetEventsResponse{
				Events:       page,
				LatestLedger: latest,
				OldestLedger: oldest,
			}
			if len(page) > 0 {
				resp.Cursor = page[len(page)-1].ID
			} else if hasCursor {
				resp.Cursor = req.Pagination.Cursor.String()
			}
			return resp
		}

		if b.Empty {
			responses = append(responses, respond(nil))
			continue
		}
		if b.ReplayLast {
			responses = append(responses, respond(s.lastEvents))
			continue
		}

		// Select everything strictly after the cursor, or from the start ledger.
		var out []protocol.EventInfo
		for _, e := range s.events {
			if hasCursor {
				if e.ID > req.Pagination.Cursor.String() {
					out = append(out, e)
				}
				continue
			}
			if req.StartLedger == 0 || uint32(e.Ledger) >= req.StartLedger { //nolint:gosec // test data
				out = append(out, e)
			}
		}

		limit := s.pageLimit
		if req.Pagination != nil && req.Pagination.Limit > 0 && int(req.Pagination.Limit) < limit {
			limit = int(req.Pagination.Limit)
		}
		if len(out) > limit {
			out = out[:limit]
		}

		if b.Reorder {
			reversed := make([]protocol.EventInfo, len(out))
			copy(reversed, out)
			sort.Slice(reversed, func(i, j int) bool { return reversed[i].ID > reversed[j].ID })
			// Cursor still reflects the true high-water mark; only the page order
			// is wrong, which is exactly the hazard being simulated.
			s.lastEvents = reversed
			resp := protocol.GetEventsResponse{
				Events:       reversed,
				LatestLedger: latest,
				OldestLedger: oldest,
			}
			if len(out) > 0 {
				resp.Cursor = out[len(out)-1].ID
			}
			responses = append(responses, resp)
			continue
		}

		responses = append(responses, respond(out))
	}
	return responses, nil
}

// GetLedgers implements rpc.Client.
func (s *Server) GetLedgers(ctx context.Context, req protocol.GetLedgersRequest) (protocol.GetLedgersResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return protocol.GetLedgersResponse{}, err
	}

	call := s.calls
	s.calls++
	b := s.script[call]

	if b.Err != nil {
		return protocol.GetLedgersResponse{}, b.Err
	}

	oldest, latest := s.oldest, s.latest
	if b.OldestLedger != 0 {
		oldest = b.OldestLedger
	}
	if b.LatestLedger != 0 {
		latest = b.LatestLedger
	}

	hasCursor := req.Pagination != nil && req.Pagination.Cursor != ""

	respond := func(page []protocol.LedgerInfo) protocol.GetLedgersResponse {
		s.lastLedgers = page
		resp := protocol.GetLedgersResponse{
			Ledgers:      page,
			LatestLedger: latest,
			OldestLedger: oldest,
		}
		if len(page) > 0 {
			resp.Cursor = fmt.Sprintf("%d", page[len(page)-1].Sequence)
		} else if hasCursor {
			resp.Cursor = req.Pagination.Cursor
		}
		return resp
	}

	if b.Empty {
		return respond(nil), nil
	}
	if b.ReplayLast {
		return respond(s.lastLedgers), nil
	}

	var after uint32
	if hasCursor {
		if _, err := fmt.Sscanf(req.Pagination.Cursor, "%d", &after); err != nil {
			return protocol.GetLedgersResponse{}, fmt.Errorf("bad cursor: %w", err)
		}
	}

	var out []protocol.LedgerInfo
	for _, l := range s.ledgers {
		if hasCursor {
			if l.Sequence > after {
				out = append(out, l)
			}
			continue
		}
		if req.StartLedger == 0 || l.Sequence >= req.StartLedger {
			out = append(out, l)
		}
	}

	limit := s.pageLimit
	if req.Pagination != nil && req.Pagination.Limit > 0 && int(req.Pagination.Limit) < limit {
		limit = int(req.Pagination.Limit)
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return respond(out), nil
}

// GetTransaction implements rpc.Client.
func (s *Server) GetTransaction(ctx context.Context, req protocol.GetTransactionRequest) (protocol.GetTransactionResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return protocol.GetTransactionResponse{}, err
	}
	resp, ok := s.tx[req.Hash]
	if !ok {
		return protocol.GetTransactionResponse{
			TransactionDetails: protocol.TransactionDetails{
				Status: protocol.TransactionStatusNotFound,
			},
		}, nil
	}
	return resp, nil
}

// GetTransactions implements rpc.Client.
func (s *Server) GetTransactions(ctx context.Context, req protocol.GetTransactionsRequest) (protocol.GetTransactionsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return protocol.GetTransactionsResponse{}, err
	}
	if len(s.transactions) == 0 {
		return s.txs, nil
	}

	call := s.calls
	s.calls++
	b := s.script[call]
	if b.Err != nil {
		return protocol.GetTransactionsResponse{}, b.Err
	}

	oldest, latest := s.oldest, s.latest
	if b.OldestLedger != 0 {
		oldest = b.OldestLedger
	}
	if b.LatestLedger != 0 {
		latest = b.LatestLedger
	}

	hasCursor := req.Pagination != nil && req.Pagination.Cursor != ""
	// Mirror the real server's rule that a cursor and a ledger range are
	// mutually exclusive.
	if hasCursor && req.StartLedger != 0 {
		return protocol.GetTransactionsResponse{}, errors.New("ledger ranges and cursor cannot both be set")
	}

	respond := func(page []protocol.TransactionInfo) protocol.GetTransactionsResponse {
		s.lastTransactions = page
		resp := protocol.GetTransactionsResponse{
			Transactions: page,
			LatestLedger: latest,
			OldestLedger: oldest,
		}
		if len(page) > 0 {
			resp.Cursor = transactionCursor(page[len(page)-1])
		} else if hasCursor {
			resp.Cursor = req.Pagination.Cursor
		}
		return resp
	}

	if b.Empty {
		return respond(nil), nil
	}
	if b.ReplayLast {
		return respond(s.lastTransactions), nil
	}

	var after int64 = -1
	if hasCursor {
		parsed, err := strconv.ParseInt(req.Pagination.Cursor, 10, 64)
		if err != nil {
			return protocol.GetTransactionsResponse{}, fmt.Errorf("bad cursor: %w", err)
		}
		after = parsed
	}

	var out []protocol.TransactionInfo
	for _, tx := range s.transactions {
		if hasCursor {
			if transactionToid(tx) > after {
				out = append(out, tx)
			}
			continue
		}
		if req.StartLedger == 0 || tx.Ledger >= req.StartLedger {
			out = append(out, tx)
		}
	}

	limit := s.pageLimit
	if req.Pagination != nil && req.Pagination.Limit > 0 && int(req.Pagination.Limit) < limit {
		limit = int(req.Pagination.Limit)
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return respond(out), nil
}

func transactionCursor(tx protocol.TransactionInfo) string {
	return strconv.FormatInt(transactionToid(tx), 10)
}

// transactionToid returns the SEP-35 total-order position the real server
// encodes in its cursor. Getting this shape right is what keeps cursor
// parsing, gap detection and ordering tests honest.
func transactionToid(tx protocol.TransactionInfo) int64 {
	return toid.New(int32(tx.Ledger), tx.ApplicationOrder, 1).ToInt64() //nolint:gosec // test data
}

// GetLedgerEntries implements rpc.LedgerEntriesClient.
func (s *Server) GetLedgerEntries(ctx context.Context, req protocol.GetLedgerEntriesRequest) (protocol.GetLedgerEntriesResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return protocol.GetLedgerEntriesResponse{}, err
	}

	resp := protocol.GetLedgerEntriesResponse{LatestLedger: s.latest}
	for _, key := range req.Keys {
		entry, ok := s.entries[key]
		if !ok {
			continue
		}
		resp.Entries = append(resp.Entries, entry)
	}
	return resp, nil
}

// GetHealth implements rpc.HealthClient.
//
// Without an explicit response or script it derives a healthy answer from the
// fake's ledger data, so a test that only cares about a transport failure does
// not have to construct the full response.
func (s *Server) GetHealth(ctx context.Context) (protocol.GetHealthResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return protocol.GetHealthResponse{}, err
	}

	call := s.healthCalls
	s.healthCalls++

	if len(s.healthScript) > 0 {
		index := call
		if index >= len(s.healthScript) {
			index = len(s.healthScript) - 1
		}
		behaviour := s.healthScript[index]
		if behaviour.Err != nil {
			return protocol.GetHealthResponse{}, behaviour.Err
		}
		return behaviour.Response, nil
	}
	if s.healthErr != nil {
		return protocol.GetHealthResponse{}, s.healthErr
	}
	if s.healthSet {
		return s.health, nil
	}

	now := time.Now()
	var retention uint32
	if s.latest >= s.oldest {
		retention = s.latest - s.oldest
	}
	return protocol.GetHealthResponse{
		Status:                "healthy",
		LatestLedger:          s.latest,
		LatestLedgerCloseTime: now.Unix(),
		OldestLedger:          s.oldest,
		OldestLedgerCloseTime: now.Add(-time.Duration(retention) * 5 * time.Second).Unix(),
		LedgerRetentionWindow: retention,
	}, nil
}

// GetLatestLedger implements rpc.Client.
func (s *Server) GetLatestLedger(ctx context.Context) (protocol.GetLatestLedgerResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return protocol.GetLatestLedgerResponse{}, err
	}
	return protocol.GetLatestLedgerResponse{Sequence: s.latest}, nil
}

// SimulateTransaction implements rpc.Client.
func (s *Server) SimulateTransaction(ctx context.Context, req protocol.SimulateTransactionRequest) (protocol.SimulateTransactionResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return protocol.SimulateTransactionResponse{}, err
	}
	if len(s.simulations) == 0 {
		return protocol.SimulateTransactionResponse{}, nil
	}
	index := s.simulationCalls
	if index >= len(s.simulations) {
		index = len(s.simulations) - 1
	}
	s.simulationCalls++
	return s.simulations[index], nil
}

// MakeEvents builds a contiguous run of events, one per ledger starting at
// startLedger, for use as a fake's universe.
func MakeEvents(startLedger uint32, count int) []protocol.EventInfo {
	out := make([]protocol.EventInfo, 0, count)
	for i := range count {
		ledger := startLedger + uint32(i) //nolint:gosec // test data
		cur := protocol.Cursor{Ledger: ledger, Tx: 1, Op: 0, Event: 0}
		out = append(out, protocol.EventInfo{
			EventType:       protocol.EventTypeContract,
			Ledger:          int32(ledger), //nolint:gosec // test data
			LedgerClosedAt:  time.Unix(int64(1700000000+ledger), 0).UTC().Format(time.RFC3339),
			ContractID:      "CBGSBAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAKKY3",
			ID:              cur.String(),
			TxIndex:         1,
			TransactionHash: fmt.Sprintf("%064x", ledger),
			// A minimal but genuinely decodable ScVal: the u32 value 7.
			TopicXDR: []string{"AAAAAwAAAAc="},
			ValueXDR: "AAAAAwAAAAc=",
		})
	}
	return out
}

// MakeLedgers builds a contiguous run of ledgers.
func MakeLedgers(startLedger uint32, count int) []protocol.LedgerInfo {
	out := make([]protocol.LedgerInfo, 0, count)
	for i := range count {
		seq := startLedger + uint32(i) //nolint:gosec // test data
		out = append(out, protocol.LedgerInfo{
			Hash:            fmt.Sprintf("%064x", seq),
			Sequence:        seq,
			LedgerCloseTime: int64(1700000000 + seq),
		})
	}
	return out
}
