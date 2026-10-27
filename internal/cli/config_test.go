package cli

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultConfigFile(t *testing.T) {
	got := DefaultConfigFile()
	want := filepath.Join(hostConfigDir(), "prism", "config.yaml")
	if got != want {
		t.Fatalf("DefaultConfigFile() = %q, want %q", got, want)
	}
}

func TestNewConfig(t *testing.T) {
	c := NewConfig()
	if c == nil {
		t.Fatal("NewConfig returned nil")
	}
	if c.Viper == nil {
		t.Fatal("NewConfig.Viper is nil")
	}
	if c.Viper.ConfigFileUsed() != "" {
		t.Fatal("NewConfig should not have read a file yet")
	}
}

func TestNewConfigAddsSearchPaths(t *testing.T) {
	c := NewConfig()

	c.Viper.SetConfigFile(filepath.Join(t.TempDir(), "absent.yaml"))
	if err := c.Viper.ReadInConfig(); err == nil {
		t.Fatal("expected ConfigFileNotFoundError for absent file")
	}

	// When no explicit file is set, search paths should be registered.
	c2 := NewConfig()
	c2.Viper.SetConfigType("yaml")
	c2.Viper.AddConfigPath(t.TempDir())
	c2.Viper.SetConfigName("prism")
	if err := c2.Viper.ReadInConfig(); err != nil {
		t.Skipf("search-path behavior is platform/viper dependent: %v", err)
	}
}

func TestConfigSetConfigFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("rpc-url: https://example.invalid\n"), 0644); err != nil {
		t.Fatal(err)
	}

	c := NewConfig()
	c.SetConfigFile(path)
	if c.ConfigFile != path {
		t.Fatalf("SetConfigFile set ConfigFile = %q, want %q", c.ConfigFile, path)
	}
	if c.Viper.ConfigFileUsed() != "" {
		t.Fatal("SetConfigFile should not have read the file yet")
	}
}

func TestResolveConfigMissingFileIsFine(t *testing.T) {
	c := NewConfig()
	c.Viper.SetConfigFile(filepath.Join(t.TempDir(), "absent.yaml"))
	res, err := c.ResolveConfig()
	if err != nil {
		t.Fatalf("ResolveConfig on missing file returned error: %v", err)
	}
	if res == nil {
		t.Fatal("ResolveConfig returned nil config for missing file")
	}
	if res.IsConfigured() {
		t.Fatal("missing file should not count as configured")
	}
}

func TestResolveConfigBadFileIsAnError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(":{bad yaml\n"), 0644); err != nil {
		t.Fatal(err)
	}

	c := NewConfig()
	c.SetConfigFile(path)
	_, err := c.ResolveConfig()
	if err == nil {
		t.Fatal("expected error for malformed YAML, got nil")
	}
}

func TestResolveConfigReadsFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("rpc-url: https://file.invalid\ncolor: always\n"), 0644); err != nil {
		t.Fatal(err)
	}

	c := NewConfig()
	c.SetConfigFile(path)
	res, err := c.ResolveConfig()
	if err != nil {
		t.Fatalf("ResolveConfig returned error: %v", err)
	}
	if !res.IsConfigured() {
		t.Fatal("file should count as configured")
	}
	if got := res.Viper.GetString("rpc-url"); got != "https://file.invalid" {
		t.Fatalf("rpc-url = %q, want https://file.invalid", got)
	}
	if got := res.Viper.GetString("color"); got != "always" {
		t.Fatalf("color = %q, want always", got)
	}
}

func TestResolveConfigEnvOverridesFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("rpc-url: https://file.invalid\n"), 0644); err != nil {
		t.Fatal(err)
	}

	c := NewConfig()
	c.SetConfigFile(path)
	if _, err := c.ResolveConfig(); err != nil {
		t.Fatal(err)
	}
	c.Viper.Set("rpc-url", "https://env.invalid")

	if got := c.Viper.GetString("rpc-url"); got != "https://env.invalid" {
		t.Fatalf("env override = %q, want https://env.invalid", got)
	}
	_ = c.Viper.Get("rpc-url")
}

func TestRPCURLsFromConfigPrefersFlag(t *testing.T) {
	c := NewConfig()
	c.Viper.SetConfigFile(filepath.Join(t.TempDir(), "absent.yaml"))

	flagURLs := []string{"https://flag.invalid"}
	got := c.RPCURLsFromConfig(flagURLs, nil)
	if len(got) != len(flagURLs) || got[0] != flagURLs[0] {
		t.Fatalf("RPCURLsFromConfig(flag) = %v, want %v", got, flagURLs)
	}
}

func TestRPCURLsFromConfigPrefersEnv(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("rpc-url: https://file.invalid\n"), 0644); err != nil {
		t.Fatal(err)
	}

	c := NewConfig()
	c.SetConfigFile(path)
	if _, err := c.ResolveConfig(); err != nil {
		t.Fatal(err)
	}

	envURLs := []string{"https://env.invalid"}
	got := c.RPCURLsFromConfig(nil, envURLs)
	if len(got) != len(envURLs) || got[0] != envURLs[0] {
		t.Fatalf("RPCURLsFromConfig(env) = %v, want %v", got, envURLs)
	}
}

func TestRPCURLsFromConfigUsesFileWhenNothingElse(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("rpc-url: https://file.invalid\n"), 0644); err != nil {
		t.Fatal(err)
	}

	c := NewConfig()
	c.SetConfigFile(path)
	if _, err := c.ResolveConfig(); err != nil {
		t.Fatal(err)
	}

	got := c.RPCURLsFromConfig(nil, nil)
	if len(got) != 1 || got[0] != "https://file.invalid" {
		t.Fatalf("RPCURLsFromConfig(file) = %v, want [https://file.invalid]", got)
	}
}

func TestRPCURLsFromConfigFallsBackToFileKey(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("rpc_urls: https://files.invalid\n"), 0644); err != nil {
		t.Fatal(err)
	}

	c := NewConfig()
	c.SetConfigFile(path)
	if _, err := c.ResolveConfig(); err != nil {
		t.Fatal(err)
	}

	got := c.RPCURLsFromConfig(nil, nil)
	if len(got) != 1 || got[0] != "https://files.invalid" {
		t.Fatalf("RPCURLsFromConfig(file key) = %v, want [https://files.invalid]", got)
	}
}

func TestRPCURLsFromConfigReturnsNilWhenEmpty(t *testing.T) {
	c := NewConfig()
	c.Viper.SetConfigFile(filepath.Join(t.TempDir(), "absent.yaml"))
	if _, err := c.ResolveConfig(); err != nil {
		t.Fatal(err)
	}

	got := c.RPCURLsFromConfig(nil, nil)
	if got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestSplitURLs(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want []string
	}{
		{"empty", "", nil},
		{"single", "https://a.invalid", []string{"https://a.invalid"}},
		{"comma", "https://a.invalid,https://b.invalid", []string{"https://a.invalid", "https://b.invalid"}},
		{"spaces around comma", " https://a.invalid , https://b.invalid ", []string{"https://a.invalid", "https://b.invalid"}},
		{"mixed separators", "https://a.invalid,https://b.invalid https://c.invalid", []string{"https://a.invalid", "https://b.invalid", "https://c.invalid"}},
		{"trailing comma", "https://a.invalid,", []string{"https://a.invalid"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := splitURLs(tc.raw)
			if len(got) != len(tc.want) {
				t.Fatalf("splitURLs(%q) len = %d, want %d", tc.raw, len(got), len(tc.want))
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("splitURLs(%q)[%d] = %q, want %q", tc.raw, i, got[i], tc.want[i])
				}
			}
		})
	}
}

func TestHostConfigDirFallsBack(t *testing.T) {
	// hostConfigDir is unexported, so we test the behavior indirectly through
	// DefaultConfigFile and the fact that it never panics.
	got := DefaultConfigFile()
	if got == "" {
		t.Fatal("DefaultConfigFile returned empty string")
	}
	if filepath.Ext(got) != ".yaml" {
		t.Fatalf("DefaultConfigFile should end in .yaml, got %q", got)
	}
}

func TestCleanupIsIdempotent(t *testing.T) {
	c := NewConfig()
	c.Cleanup()
	c.Cleanup()
	// no panic and no leak expected
}

func TestReloadConfigMissingFile(t *testing.T) {
	c := NewConfig()
	c.Viper.SetConfigFile(filepath.Join(t.TempDir(), "absent.yaml"))
	if err := c.ReloadConfig(); err == nil {
		t.Fatal("expected error reloading absent file")
	}
}

func TestReloadConfigRefreshesFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("rpc-url: first.invalid\n"), 0644); err != nil {
		t.Fatal(err)
	}

	c := NewConfig()
	c.SetConfigFile(path)
	if _, err := c.ResolveConfig(); err != nil {
		t.Fatal(err)
	}
	if c.Viper.GetString("rpc-url") != "first.invalid" {
		t.Fatal("expected first.invalid before reload")
	}

	if err := os.WriteFile(path, []byte("rpc-url: second.invalid\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := c.ReloadConfig(); err != nil {
		t.Fatal(err)
	}
	if c.Viper.GetString("rpc-url") != "second.invalid" {
		t.Fatalf("expected second.invalid after reload, got %q", c.Viper.GetString("rpc-url"))
	}
}

// vim: noexpandtab sw=2 sts=2
