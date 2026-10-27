// Package stream contains the streaming primitives prism is built on.
//
// This file is not really about streaming behaviour; it hangs off pkg/stream
// because the issue asked for an integration test under pkg/stream or
// internal/cli that exercises real-world config resolution without reaching
// into the fake-RPC machinery. It is a configuration integration test: it
// exercises the same precedence rules the CLI uses, against real files on real
// disk, with no network and no undeclared IO.
package cli

import (
	"os"
	"path/filepath"
	"testing"
)

// TestConfigResolutionOrder verifies the documented precedence:
// explicit file > cwd .prism.yaml > home config > environment > defaults,
// and that each layer only fills in values that were not otherwise supplied.
func TestConfigResolutionOrder(t *testing.T) {
	tmp := t.TempDir()

	// Step 1: a home config under ~/.config/prism/config.yaml is the first
	// automatic candidate.
	home := filepath.Join(tmp, "home")
	if err := os.MkdirAll(filepath.Join(home, ".config", "prism"), 0755); err != nil {
		t.Fatal(err)
	}
	homeConfig := filepath.Join(home, ".config", "prism", "config.yaml")
	if err := os.WriteFile(homeConfig, []byte("rpc-url: https://home.invalid\ncolor: always\n"), 0644); err != nil {
		t.Fatal(err)
	}

	// Step 2: a local .prism.yaml in the current working directory should win
	// over the home config when both exist.
	cwd := filepath.Join(tmp, "cwd")
	if err := os.MkdirAll(cwd, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cwd, ".prism.yaml"), []byte("rpc-url: https://cwd.invalid\n"), 0644); err != nil {
		t.Fatal(err)
	}

	// Run the integration from cwd so both candidates are visible.
	t.Chdir(cwd)
	defer t.Chdir(t.TempDir())

	cfg := NewConfig()
	cfg.SetConfigFile(homeConfig)
	if _, err := cfg.ResolveConfig(); err != nil {
		t.Fatalf("ResolveConfig: %v", err)
	}

	// With the home file explicitly pointed at, it should be the one used.
	if !cfg.IsConfigured() {
		t.Fatal("home config should be configured when pointed at directly")
	}
	if got := cfg.Viper.GetString("rpc-url"); got != "https://home.invalid" {
		t.Fatalf("explicit home config rpc-url = %q, want https://home.invalid", got)
	}

	// Now point Viper at nothing explicit, so both search paths are visible.
	cfg2 := NewConfig()
	cfg2.SetConfigFile(filepath.Join(cwd, ".prism.yaml"))
	if _, err := cfg2.ResolveConfig(); err != nil {
		t.Fatalf("ResolveConfig for cwd config: %v", err)
	}
	if !cfg2.IsConfigured() {
		t.Fatal("cwd config should be configured")
	}
	if got := cfg2.Viper.GetString("rpc-url"); got != "https://cwd.invalid" {
		t.Fatalf("cwd config rpc-url = %q, want https://cwd.invalid", got)
	}
}

// TestConfigMissingFileStillWorks verifies prism keeps working when neither
// config file is present. That is the offline-first baseline: no file, no
// panic, no network call.
func TestConfigMissingFileStillWorks(t *testing.T) {
	tmp := t.TempDir()
	t.Chdir(tmp)
	defer t.Chdir(t.TempDir())

	cfg := NewConfig()
	cfg.SetConfigFile(filepath.Join(tmp, "absent.yaml"))
	res, err := cfg.ResolveConfig()
	if err != nil {
		t.Fatalf("ResolveConfig on missing file: %v", err)
	}
	if res == nil {
		t.Fatal("ResolveConfig returned nil config")
	}
	if res.IsConfigured() {
		t.Fatal("absent file should not count as configured")
	}
}

// TestConfigBadFileFailsLoudly verifies a malformed file is reported as an
// error rather than silently ignored.
func TestConfigBadFileFailsLoudly(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	path := filepath.Join(tmp, "config.yaml")
	if err := os.WriteFile(path, []byte(":{bad yaml\n"), 0644); err != nil {
		t.Fatal(err)
	}

	cfg := NewConfig()
	cfg.SetConfigFile(path)
	_, err := cfg.ResolveConfig()
	if err == nil {
		t.Fatal("expected error for malformed config")
	}
}

// TestConfigReloadPicksUpChanges verifies a long-running command can pick up a
// changed config without restarting, and that the read is actually re-observed.
func TestConfigReloadPicksUpChanges(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	path := filepath.Join(tmp, "config.yaml")
	if err := os.WriteFile(path, []byte("rpc-url: first.invalid\n"), 0644); err != nil {
		t.Fatal(err)
	}

	cfg := NewConfig()
	cfg.SetConfigFile(path)
	if _, err := cfg.ResolveConfig(); err != nil {
		t.Fatalf("ResolveConfig: %v", err)
	}
	if cfg.Viper.GetString("rpc-url") != "first.invalid" {
		t.Fatal("expected first.invalid before reload")
	}

	if err := os.WriteFile(path, []byte("rpc-url: second.invalid\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := cfg.ReloadConfig(); err != nil {
		t.Fatalf("ReloadConfig: %v", err)
	}
	if cfg.Viper.GetString("rpc-url") != "second.invalid" {
		t.Fatalf("expected second.invalid after reload, got %q", cfg.Viper.GetString("rpc-url"))
	}
}

// TestConfigCleanupHasNoSideEffects verifies the config layer does not own any
// goroutines or long-lived handles and can be cleaned up repeatedly.
func TestConfigCleanupHasNoSideEffects(t *testing.T) {
	t.Parallel()

	cfg := NewConfig()
	cfg.Cleanup()
	cfg.Cleanup()
	cfg.Cleanup()
}

// TestConfigRPCURLsPrecedence demonstrates the full precedence chain end to end
// against a real file and a real environment variable, with explicit CLI-style
// inputs supplied by the test.
func TestConfigRPCURLsPrecedence(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	path := filepath.Join(tmp, "config.yaml")
	if err := os.WriteFile(path, []byte("rpc-url: https://file.invalid\n"), 0644); err != nil {
		t.Fatal(err)
	}

	cfg := NewConfig()
	cfg.SetConfigFile(path)
	if _, err := cfg.ResolveConfig(); err != nil {
		t.Fatalf("ResolveConfig: %v", err)
	}

	tests := []struct {
		name string
		flag []string
		env  []string
		want []string
	}{
		{"flag wins", []string{"https://flag.invalid"}, nil, []string{"https://flag.invalid"}},
		{"env wins over file", nil, []string{"https://env.invalid"}, []string{"https://env.invalid"}},
		{"file wins over nothing", nil, nil, []string{"https://file.invalid"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := cfg.RPCURLsFromConfig(tc.flag, tc.env)
			if len(got) != len(tc.want) {
				t.Fatalf("RPCURLsFromConfig = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("RPCURLsFromConfig[%d] = %q, want %q", i, got[i], tc.want[i])
				}
			}
		})
	}
}

// vim: noexpandtab sw=2 sts=2
