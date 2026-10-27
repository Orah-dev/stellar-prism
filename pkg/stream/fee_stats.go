package stream

import (
	"context"
	"fmt"
	"sort"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

// FeeStatsSource streams one fee-market summary per closed ledger.
type FeeStatsSource struct {
	client  rpc.Client
	ledgers *LedgerSource
	window  map[uint32][]int64
}

func NewFeeStatsSource(client rpc.Client, q LedgerQuery) (*FeeStatsSource, error) {
	if client == nil {
		return nil, fmt.Errorf("fee stats source: rpc client is required")
	}
	return &FeeStatsSource{client: client, ledgers: NewLedgerSource(client, q), window: make(map[uint32][]int64)}, nil
}
func (s *FeeStatsSource) Name() string { return "fee_stats" }
func (s *FeeStatsSource) Rewind(ledger uint32) {
	s.ledgers.Rewind(ledger)
	for seq := range s.window {
		if seq >= ledger {
			delete(s.window, seq)
		}
	}
}
func (s *FeeStatsSource) CursorLedger(cursor string) (uint32, bool) {
	return s.ledgers.CursorLedger(cursor)
}

func (s *FeeStatsSource) Fetch(ctx context.Context, cursor string) (Batch, error) {
	b, err := s.ledgers.Fetch(ctx, cursor)
	if err != nil {
		return Batch{}, err
	}
	out := Batch{Records: make([]Record, 0, len(b.Records)), NextCursor: b.NextCursor, Latest: b.Latest, Oldest: b.Oldest, Full: b.Full}
	for _, ledger := range b.Records {
		if ledger.Close == nil || ledger.Close.HeaderXDR == "" {
			continue
		}
		var header xdr.LedgerHeaderHistoryEntry
		if err := xdr.SafeUnmarshalBase64(ledger.Close.HeaderXDR, &header); err != nil {
			return Batch{}, fmt.Errorf("fee stats source: ledger %d header: %w", ledger.Sequence, err)
		}
		resp, err := s.client.GetTransactions(ctx, protocol.GetTransactionsRequest{StartLedger: ledger.Sequence, Pagination: &protocol.LedgerPaginationOptions{Limit: 1000}})
		if err != nil {
			return Batch{}, fmt.Errorf("fee stats source: ledger %d transactions: %w", ledger.Sequence, err)
		}
		var fees []int64
		var resourceFee, feeBid int64
		for _, tx := range resp.Transactions {
			if tx.Ledger != ledger.Sequence {
				continue
			}
			var result xdr.TransactionResult
			if tx.ResultXDR != "" && xdr.SafeUnmarshalBase64(tx.ResultXDR, &result) == nil {
				fees = append(fees, int64(result.FeeCharged))
			}
			resourceFee += sorobanResourceFee(tx.ResultMetaXDR)
			var env xdr.TransactionEnvelope
			if tx.EnvelopeXDR != "" && xdr.SafeUnmarshalBase64(tx.EnvelopeXDR, &env) == nil {
				if t, ok := envelopeTransaction(env); ok {
					if sd, ok := t.Ext.GetSorobanData(); ok {
						feeBid += int64(sd.ResourceFee)
					}
				}
			}
		}
		s.window[ledger.Sequence] = fees
		if len(s.window) > 50 {
			var seqs []uint32
			for seq := range s.window {
				seqs = append(seqs, seq)
			}
			sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
			for _, seq := range seqs[:len(seqs)-50] {
				delete(s.window, seq)
			}
		}
		stats := &FeeStats{BaseFee: uint32(header.Header.BaseFee), BaseReserve: uint32(header.Header.BaseReserve), FeePool: int64(header.Header.FeePool), CoinsInCirculation: int64(header.Header.TotalCoins), Transactions: uint32(len(fees)), SorobanResourceFee: resourceFee, SorobanFeeBid: feeBid}
		stats.AverageFee10 = averageFees(s.window, 10)
		stats.AverageFee50 = averageFees(s.window, 50)
		out.Records = append(out.Records, Record{ID: fmt.Sprintf("%010d", ledger.Sequence), Sequence: ledger.Sequence, Time: ledger.Time, Kind: KindFeeStats, FeeStats: stats})
	}
	return out, nil
}

func sorobanResourceFee(metaXDR string) int64 {
	if metaXDR == "" {
		return 0
	}
	var meta xdr.TransactionMeta
	if xdr.SafeUnmarshalBase64(metaXDR, &meta) != nil {
		return 0
	}
	var ext xdr.SorobanTransactionMetaExt
	if meta.V == 3 && meta.V3 != nil && meta.V3.SorobanMeta != nil {
		ext = meta.V3.SorobanMeta.Ext
	}
	if meta.V == 4 && meta.V4 != nil && meta.V4.SorobanMeta != nil {
		ext = meta.V4.SorobanMeta.Ext
	}
	if ext.V == 1 && ext.V1 != nil {
		return int64(ext.V1.TotalNonRefundableResourceFeeCharged + ext.V1.TotalRefundableResourceFeeCharged)
	}
	return 0
}

func averageFees(samples map[uint32][]int64, ledgers int) float64 {
	var sum int64
	var n int
	seqs := make([]uint32, 0, len(samples))
	for seq := range samples {
		seqs = append(seqs, seq)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] > seqs[j] })
	if len(seqs) > ledgers {
		seqs = seqs[:ledgers]
	}
	for _, seq := range seqs {
		for _, fee := range samples[seq] {
			sum += fee
			n++
		}
	}
	if n == 0 {
		return 0
	}
	return float64(sum) / float64(n)
}

func envelopeTransaction(e xdr.TransactionEnvelope) (xdr.Transaction, bool) {
	switch e.Type {
	case xdr.EnvelopeTypeEnvelopeTypeTx:
		if e.V1 != nil {
			return e.V1.Tx, true
		}
	case xdr.EnvelopeTypeEnvelopeTypeTxFeeBump:
		if e.FeeBump != nil && e.FeeBump.Tx.InnerTx.V1 != nil {
			return e.FeeBump.Tx.InnerTx.V1.Tx, true
		}
	}
	return xdr.Transaction{}, false
}
