package cli

import (
	"io"

	"github.com/spf13/cobra"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func newTradesCmd(g *globalFlags, stdout, stderr io.Writer) *cobra.Command {
	var sf streamFlags
	var pair string
	cmd := &cobra.Command{Use: "trades", Short: "Stream Stellar path payments and Soroban swap events", Args: cobra.NoArgs,
		Long:    "Extract completed classic path-payment executions and Soroban contract events using the common swap topic layout (swap, trader, asset in, asset out; event data contains input and output amounts). Soroban amounts are emitted in raw token units because decimals are contract-specific. Classic offer-book fills outside path payments are not normalized.",
		Example: "  prism trades --pair XLM/USDC --follow\n  prism trades --from 1234567 --json",
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
				return stream.NewTradeSource(client, stream.TradeQuery{StartLedger: start, PageLimit: sf.pageSize, Pair: pair})
			}
			return runStream(cmd, client, factory, sf.from, &sf, g, stdout, stderr)
		},
	}
	cmd.Flags().StringVar(&pair, "pair", "", "asset pair filter in ASSET/ASSET form")
	sf.register(cmd, stream.DefaultTransactionPageLimit)
	return cmd
}
