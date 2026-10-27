package rpc

import (
	"context"
	"strings"
	"testing"

	"github.com/stellar/go-stellar-sdk/network"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
)

type networkClient struct{ passphrase string }

func (c networkClient) GetNetwork(context.Context) (protocol.GetNetworkResponse, error) {
	return protocol.GetNetworkResponse{Passphrase: c.passphrase}, nil
}

func TestValidateNetwork(t *testing.T) {
	if err := ValidateNetwork(t.Context(), networkClient{network.TestNetworkPassphrase}, "testnet"); err != nil {
		t.Fatal(err)
	}
	err := ValidateNetwork(t.Context(), networkClient{network.PublicNetworkPassphrase}, "testnet")
	if err == nil || !strings.Contains(err.Error(), "network mismatch") {
		t.Fatalf("error = %v", err)
	}
}

func TestExpectedPassphraseRejectsUnknownPreset(t *testing.T) {
	if _, err := ExpectedPassphrase("other"); err == nil {
		t.Fatal("expected error")
	}
}
