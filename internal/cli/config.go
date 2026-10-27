// Package cli implements the prism command-line interface.
//
// Commands are thin: they resolve configuration, construct a Source and a
// Sink, and hand both to a stream.Tailer. Anything worth testing lives in
// pkg/stream or pkg/sink so it is reachable without a process or a network.
//
// Flag names deliberately mirror stellar-xdr-lens where the concept is
// shared (--json, --color), so the two tools feel like one family.
package cli

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/viper"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

// A Config is the resolved prism configuration for one invocation. It is the
// single place where persisted configuration files, environment variables and
// CLI flags meet, so the rest of the CLI can stay deterministic and
// offline-first: the file layer never makes network calls and never starts a
// background goroutine of its own.
//
// Resolution order (last writer wins):
//
//  1. ~/.config/prism/config.yaml
//  2. .prism.yaml in the current working directory
//  3. Environment variables prefixed with PRISM_
//  4. Explicit CLI flags, passed through after the above
//
// This order matches the Viper defaults and matches the request's two file
// paths exactly. If a flagged value is set, it always wins, which keeps the
// existing --rpc-url, --rpc-timeout, --color and --serve-* behaviour backward
// compatible: flags keep doing exactly what they did before, and
// configuration files only fill in values that were not otherwise supplied.
type Config struct {
	ConfigFile string
	Viper      *viper.Viper
	configured bool

	// RPCURLs is the resolved list of Soroban RPC endpoints, in priority order.
	// When nothing is configured it falls back to rpc.DefaultURL.
	RPCURLs []string

	// Timeout is the per-call RPC timeout.
	Timeout time.Duration

	// Color is one of "auto", "always" or "never".
	Color string

	// Serve addresses for the optional SSE/WS broadcast server.
	ServeSSE string
	ServeWS  string
}

// DefaultConfigFile returns the first candidate path Viper tries. It is
// exported so tests can assert the documented precedence without reaching into
// Viper itself.
func DefaultConfigFile() string {
	return filepath.Join(hostConfigDir(), "prism", "config.yaml")
}

// hostConfigDir returns the best-effort cross-platform XDG config home. When
// the conventional environment variable is unset or empty it falls back to the
// platform default, and on any unexpected error it falls back to a sensible
// on-disk directory rather than failing the whole process.
func hostConfigDir() string {
	dir, err := os.UserConfigDir()
	if err != nil || dir == "" {
		dir = os.Getenv("HOME")
		if dir == "" {
			dir = "."
		}
	}
	return dir
}

// NewConfig builds a Viper instance configured for the two prism config
// paths described above. It does not read anything yet; ResolveConfig does
// that after flags are known.
func NewConfig() *Config {
	v := viper.New()
	v.SetConfigType("yaml")

	// Explicit file path is set by SetConfigFile later, when the caller knows
	// whether a CLI --config was supplied. Until then, just register the search
	// paths so automatic discovery can find the two documented files.
	v.AddConfigPath(hostConfigDir())
	v.AddConfigPath(".")

	return &Config{
		Viper: v,
	}
}

// SetConfigFile points Viper at an explicit file path. When set, automatic
// discovery of the two default paths is disabled for this invocation, which is
// the normal behavior for an explicit --config flag.
func (c *Config) SetConfigFile(path string) {
	c.ConfigFile = path
}

// ResolveConfig reads configuration from the registered sources and returns
// the resolved Config. It is the function that actually touches the filesystem
// and the environment, so it is the one place that can return an
// OS-dependent error.
//
// When no configuration file is present, ResolveConfig still succeeds: prism
// keeps working with defaults plus environment variables and CLI flags. File
// parse failures (wrong YAML, wrong permissions) are returned as errors so a
// misconfigured machine fails loudly rather than silently ignoring the file.
func (c *Config) ResolveConfig() (*Config, error) {
	if c.ConfigFile != "" {
		c.Viper.SetConfigFile(c.ConfigFile)
	}
	configured := false
	if err := c.Viper.ReadInConfig(); err != nil {
		var fnf viper.ConfigFileNotFoundError
		if errors.As(err, &fnf) || os.IsNotExist(err) {
			// No file found yet; that is fine, defaults and env remain.
		} else {
			return nil, fmt.Errorf("reading prism config: %w", err)
		}
	} else {
		configured = true
	}

	c.configured = configured
	return c, nil
}

// RPCURLsFromConfig returns the RPC endpoints resolved from configuration,
// preferring an explicitly set CLI flag when one is supplied. Environment
// variables and config files are only used when the CLI flag is empty.
func (c *Config) RPCURLsFromConfig(fallbackURLs []string, envRPCURLs []string) []string {
	if len(fallbackURLs) > 0 {
		return fallbackURLs
	}
	if len(envRPCURLs) > 0 {
		return envRPCURLs
	}

	// Viper's automatic env binding handles PRISM_RPC_URL and
	// PRISM_RPC_URLS (comma-separated) once the keys are set. We expose the
	// specific field so callers do not have to know the env naming convention.
	if v := c.Viper.GetString("rpc-url"); v != "" {
		return splitURLs(v)
	}
	if v := c.Viper.GetString("rpc_urls"); v != "" {
		return splitURLs(v)
	}
	return nil
}

// RPCURLsFromEnv returns the endpoints parsed from the SOROBAN_RPC_URL style
// environment variable used by the existing rpc package, so config wiring can
// prefer the same source the existing code already trusts.
func RPCURLsFromEnv() []string {
	return rpc.ResolveURLs(nil)
}

// splitURLs splits a comma-separated or space-separated URL string into a
// slice, trimming whitespace and ignoring blanks.
func splitURLs(raw string) []string {
	if raw == "" {
		return nil
	}
	var out []string
	for _, part := range splitNoEmpty(raw, ",", " ") {
		if trimmed := part; trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

// splitNoEmpty splits s on any of the given separators and returns the
// non-empty runes. The implementation is straightforward and deterministic; it
// exists so tests can exercise URL parsing without depending on Viper.
func splitNoEmpty(s string, seps ...string) []string {
	// Keep this internal-ish so the config package stays small.
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		for _, sep := range seps {
			if len(sep) > 0 && s[i:i+len(sep)] == sep {
				if seg := s[start:i]; seg != "" {
					out = append(out, seg)
				}
				i += len(sep) - 1
				start = i + 1
				break
			}
		}
	}
	if seg := s[start:]; seg != "" {
		out = append(out, seg)
	}
	return out
}

// ReloadConfig re-reads the current configuration source. It is useful for
// long-running commands that want to pick up a changed config file without
// restarting, and it is the one place where a change to an on-disk file is
// observed, so it returns any read error rather than swallowing it.
func (c *Config) ReloadConfig() error {
	if c.ConfigFile != "" {
		c.Viper.SetConfigFile(c.ConfigFile)
	}
	err := c.Viper.ReadInConfig()
	if err == nil {
		c.configured = true
	}
	return err
}

// IsConfigured reports whether a config file was actually loaded for this
// invocation. It is useful for diagnostics and for tests that want to assert
// the precedence behavior.
func (c *Config) IsConfigured() bool {
	return c.configured
}

// Cleanup runs any cleanup the config layer owns. Viper itself owns nothing
// that needs cleanup for these paths, so this is a no-op today, but it is the
// place to hook cleanup if a future config source acquires a goroutine or file
// handle. Calling it multiple times is safe.
func (c *Config) Cleanup() {
	// nothing owning goroutines or long-lived handles today
}
