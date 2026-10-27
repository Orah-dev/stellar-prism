package cli

import (
	"io"

	"github.com/spf13/cobra"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func newContractsCmd(g *globalFlags, stdout, stderr io.Writer) *cobra.Command {
	var sf streamFlags
	var watch []string
	var network string
	cmd := &cobra.Command{Use: "contracts", Aliases: []string{"contract-lifecycle"}, Short: "Stream watched contract upgrades and state maintenance", Args: cobra.NoArgs,
		Long:    "Watch contract storage changes, restoration and deployment activity, including TTL live-until ledgers and low-TTL warnings. TTL entries expose their ledger key hash, since RPC does not associate them with contract IDs in transaction metadata.",
		Example: "  prism contracts --watch CDLZF... --follow\n  prism contracts --follow --watch CDLZF... --json",
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
				return stream.NewContractLifecycleSource(client, stream.ContractLifecycleQuery{StartLedger: start, PageLimit: sf.pageSize, ContractIDs: watch, NetworkPassphrase: network})
			}
			return runStream(cmd, client, factory, sf.from, &sf, g, stdout, stderr)
		},
	}
	cmd.Flags().StringSliceVar(&watch, "watch", nil, "contract ID to watch; repeatable")
	cmd.Flags().StringVar(&network, "network-passphrase", stream.DefaultNetworkPassphrase, "network passphrase for contract ID derivation")
	sf.register(cmd, stream.DefaultTransactionPageLimit)
	return cmd
}
