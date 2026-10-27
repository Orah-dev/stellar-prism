package rpc

import (
	"context"
	"fmt"
	"strings"

	"github.com/stellar/go-stellar-sdk/network"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
)

// NetworkClient is the optional RPC capability used to identify a chain.
type NetworkClient interface {
	GetNetwork(context.Context) (protocol.GetNetworkResponse, error)
}

// ExpectedPassphrase returns the canonical passphrase for a named preset.
func ExpectedPassphrase(preset string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(preset)) {
	case "testnet":
		return network.TestNetworkPassphrase, nil
	case "mainnet":
		return network.PublicNetworkPassphrase, nil
	case "futurenet":
		return network.FutureNetworkPassphrase, nil
	case "standalone":
		return "Standalone Network ; February 2017", nil
	default:
		return "", fmt.Errorf("unknown network preset %q: want testnet, mainnet, futurenet, or standalone", preset)
	}
}

// ValidateNetwork verifies that the endpoint reports the passphrase selected
// by preset. It fails before a stream starts, preventing accidental writes to
// the wrong Stellar network.
func ValidateNetwork(ctx context.Context, client NetworkClient, preset string) error {
	expected, err := ExpectedPassphrase(preset)
	if err != nil {
		return err
	}
	actual, err := client.GetNetwork(ctx)
	if err != nil {
		return fmt.Errorf("get network: %w", err)
	}
	if actual.Passphrase != expected {
		return fmt.Errorf("network mismatch: --network %s expects %q, but RPC reports %q", strings.ToLower(preset), expected, actual.Passphrase)
	}
	return nil
}
