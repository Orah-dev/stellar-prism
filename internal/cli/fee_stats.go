package cli

import (
	"io"

	"github.com/spf13/cobra"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func newFeeStatsCmd(g *globalFlags, stdout, stderr io.Writer) *cobra.Command {
	var sf streamFlags
	cmd := &cobra.Command{Use: "fee-stats", Short: "Stream Stellar ledger fee metrics", Args: cobra.NoArgs,
		Long:    "Stream base fee, base reserve, fee pool, circulating coins, and transaction fee averages for each ledger close.",
		Example: "  prism fee-stats --follow\n  prism fee-stats --from 1234567 --json",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := sf.validate(cmd); err != nil {
				return err
			}
			client, closeClient, err := openClient(g, stderr)
			if err != nil {
				return err
			}
			defer closeClient()
			factory := func(start uint32) (stream.Source, error) {
				return stream.NewFeeStatsSource(client, stream.LedgerQuery{StartLedger: start, PageLimit: sf.pageSize})
			}
			return runStream(cmd, client, factory, sf.from, &sf, g, stdout, stderr)
		},
	}
	sf.register(cmd, stream.DefaultLedgerPageLimit)
	return cmd
}
