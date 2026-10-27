package cli

import (
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func newTransactionsCmd(g *globalFlags, stdout, stderr io.Writer) *cobra.Command {
	var (
		detectConflicts bool
		sf              streamFlags
		memo            string
		statuses        []string
		trapped         bool
		feeBump         bool
		feeAccounts     []string
	)

	cmd := &cobra.Command{
		Use:   "transactions",
		Short: "Stream transactions, optionally filtered by status or envelope",
		Long: `Stream transactions as ledgers close.

Every transaction is classified as it arrives: the record carries a decoded
status, the account that actually paid the fee, and, for a fee-bump wrapper,
the wrapper and inner transaction details side by side. A failed transaction
also carries its decoded reason, naming the failing operation when the result
identifies one.

--status narrows the stream to successes, failures or both. --trapped narrows
it to Soroban invocations whose contract execution trapped or panicked, which
is a distinct outcome from a malformed invocation or an exceeded resource
limit. --fee-bump keeps only fee-bump wrappers, and --fee-account keeps only
transactions whose fee was paid by a given account, including when the payer
is a muxed address.

Filtering happens after each page is fetched, so the cursor still advances
across suppressed records: a filtered stream is a view of the transaction
stream, not a second stream that could skip history.

With --pg-dsn the position is committed to PostgreSQL as the stream advances,
so an interrupted run resumes where it stopped.

With --state-file the position reached is recorded after every page, and a
background pruner keeps that file from growing without bound. Add --resume to
start the next run where the last one stopped.`,
		Example: `  # Follow every transaction
  prism transactions --follow

  # Only failures, with the decoded reason
  prism transactions --status failed --follow

  # Soroban contract executions that trapped or panicked
  prism transactions --trapped --json | jq -r '.failure.operation'

  # Fee-bump wrappers, showing who paid
  prism transactions --fee-bump --json | jq -r '.feeAccount'

  # Transactions a particular account paid the fee for
  prism transactions --fee-account GBGSB... --follow`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := sf.validate(cmd); err != nil {
				return err
			}

			statusSet, err := stream.ParseTransactionStatuses(statuses)
			if err != nil {
				return err
			}
			if trapped && statusSet[stream.TxStatusSuccess] && !statusSet[stream.TxStatusFailed] {
				return fmt.Errorf(
					"--trapped cannot be combined with --status success: a trapped contract execution did not succeed")
			}

			filters, err := transactionFilters(trapped, feeBump, feeAccounts)
			if err != nil {
				return err
			}

			if memo != "" {
				filters = append(filters, func(tx *stream.Transaction) bool {
					return tx.Memo != nil && tx.Memo.Type == "text" && strings.HasPrefix(tx.Memo.Value, memo)
				})
			}

			client, closeClient, err := openClient(g, stderr)
			if err != nil {
				return err
			}
			defer closeClient()

			// The recorded cursor is keyed by every choice that changes which
			// records come out, so a resumed run never applies a position from
			// a differently filtered stream.
			sf.stateKey = stateKey("transactions",
				boolPart(detectConflicts, "detect-conflicts"),
				filterPart("status", statuses),
				boolPart(memo != "", "memo="+memo),
				filterPart("fee-account", feeAccounts),
				boolPart(trapped, "trapped"),
				boolPart(feeBump, "fee-bump"))

			newSource := func(startLedger uint32) (stream.Source, error) {
				if len(statuses) == 0 && len(filters) == 0 {
					return stream.NewTransactionSource(client, stream.TransactionQuery{
						StartLedger: startLedger,
						PageLimit:   sf.pageSize,
						Classify:    true,
					}), nil
				}
				return stream.NewTransactionFilterSource(client, stream.TransactionFilterQuery{
					StartLedger: startLedger,
					PageLimit:   sf.pageSize,
					Statuses:    statuses,
					Filters:     filters,
				})
			}
			sf.derivedKey = cursorKey(cmd.Name(), fromKeyPart(sf.from))
			if detectConflicts {
				analyzer := stream.NewFootprintAnalyzer()
				sf.process = analyzer.Analyze
				// Conflict analysis needs complete same-ledger transaction sets;
				// concurrent range workers would merge directly into the sink and
				// bypass the Tailer's ledger reorder/processing stage.
				sf.backfillWorkers = 1
			}
			return runStream(cmd, client, newSource, sf.from, &sf, g, stdout, stderr)
		},
	}

	cmd.Flags().StringVar(&memo, "memo", "", "include transactions whose sanitized text memo starts with this text")
	cmd.Flags().StringSliceVar(&statuses, "status", nil,
		"only emit transactions with this status: success or failed (repeatable; default: all)")
	cmd.Flags().BoolVar(&trapped, "trapped", false,
		"only emit Soroban transactions whose contract execution trapped or panicked")
	cmd.Flags().BoolVar(&feeBump, "fee-bump", false,
		"only emit transactions wrapped in a fee-bump envelope")
	cmd.Flags().StringSliceVar(&feeAccounts, "fee-account", nil,
		"only emit transactions whose fee was paid by this account; G or M address, repeatable")
	sf.register(cmd, stream.DefaultTransactionPageLimit)
	cmd.Flags().BoolVar(&detectConflicts, "detect-conflicts", false,
		"annotate Soroban transactions with same-ledger read/write footprint conflicts")
	registerFlagCompletion(cmd, "status", stream.TxStatusSuccess, stream.TxStatusFailed)
	registerFlagCompletion(cmd, "gap-strategy", string(stream.GapFail), string(stream.GapResume))

	return cmd
}

// transactionFilters builds the predicate chain implied by the envelope
// filters. Validation happens here, before a source is constructed, so a bad
// account address is reported as a flag error rather than on the first page.
func transactionFilters(trapped, feeBump bool, feeAccounts []string) ([]stream.TransactionFilter, error) {
	var filters []stream.TransactionFilter
	if trapped {
		filters = append(filters, stream.TrappedFilter())
	}
	if feeBump {
		filters = append(filters, stream.FeeBumpFilter())
	}
	if len(feeAccounts) > 0 {
		byAccount, err := stream.FeeAccountFilter(feeAccounts)
		if err != nil {
			return nil, err
		}
		filters = append(filters, byAccount)
	}
	return filters, nil
}

// boolPart contributes a state-key segment only when the flag is set, so the
// default stream keeps the same key it had before these filters existed.
func boolPart(set bool, name string) string {
	if !set {
		return ""
	}
	return name
}
