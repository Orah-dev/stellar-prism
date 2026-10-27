package stream_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// passphraseSource returns a key source backed by a passphrase.
func passphraseSource(passphrase string) stream.KeySource {
	return &stream.PassphraseKeySource{Passphrase: passphrase, Iterations: 1000}
}

// TestEncryptedStateStoreRoundTrip covers the ordinary path: a checkpoint is
// written and read back.
func TestEncryptedStateStoreRoundTrip(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "state.enc")
	store, err := stream.NewEncryptedStateStore(path, passphraseSource("correct horse"))
	if err != nil {
		t.Fatalf("NewEncryptedStateStore() error = %v", err)
	}

	ctx := context.Background()
	meta := stream.CursorMetadata{
		Key:        "events|contract=C1",
		Cursor:     "0000000123",
		Ledger:     123,
		RecordedAt: time.Unix(1700000000, 0).UTC(),
	}
	if err := store.Append(ctx, meta); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	latest, found, err := store.Latest(ctx, meta.Key)
	if err != nil {
		t.Fatalf("Latest() error = %v", err)
	}
	if !found {
		t.Fatal("Latest() found no checkpoint")
	}
	if latest.Cursor != meta.Cursor {
		t.Errorf("Latest().Cursor = %q, want %q", latest.Cursor, meta.Cursor)
	}
	if latest.Ledger != meta.Ledger {
		t.Errorf("Latest().Ledger = %d, want %d", latest.Ledger, meta.Ledger)
	}
}

// TestEncryptedStateStoreKeepsThePlaintextOffDisk is the point of the
// whole exercise: the cursor must not be readable in the file.
func TestEncryptedStateStoreKeepsThePlaintextOffDisk(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "state.enc")
	store, err := stream.NewEncryptedStateStore(path, passphraseSource("correct horse"))
	if err != nil {
		t.Fatalf("NewEncryptedStateStore() error = %v", err)
	}

	cursor := "SECRET-CURSOR-0000000123"
	if err := store.Append(context.Background(), stream.CursorMetadata{
		Key:    "events|contract=C1",
		Cursor: cursor,
		Ledger: 123,
	}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if strings.Contains(string(raw), cursor) {
		t.Errorf("the state file contains the cursor in the clear:\n%s", raw)
	}
	if strings.Contains(string(raw), "correct horse") {
		t.Errorf("the state file contains the passphrase:\n%s", raw)
	}
	// The file must still say how the key was derived, so it can be read
	// again by whoever holds the same secret.
	if !strings.Contains(string(raw), "pbkdf2-hmac-sha256") {
		t.Errorf("the state file does not record how the key was derived:\n%s", raw)
	}
}

// TestEncryptedStateStoreRejectsTheWrongKey checks that a file written with
// one passphrase cannot be read with another.
func TestEncryptedStateStoreRejectsTheWrongKey(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "state.enc")
	store, err := stream.NewEncryptedStateStore(path, passphraseSource("correct horse"))
	if err != nil {
		t.Fatalf("NewEncryptedStateStore() error = %v", err)
	}
	if err := store.Append(context.Background(), stream.CursorMetadata{
		Key:    "events",
		Cursor: "0000000123",
	}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	wrong, err := stream.NewEncryptedStateStore(path, passphraseSource("wrong horse"))
	if err != nil {
		t.Fatalf("NewEncryptedStateStore() error = %v", err)
	}
	_, _, err = wrong.Latest(context.Background(), "events")
	if err == nil {
		t.Fatal("Latest() error = nil with the wrong passphrase")
	}
	if !errors.Is(err, stream.ErrStateAuthentication) {
		t.Errorf("Latest() error = %v, want it to report an authentication failure", err)
	}
}

// TestEncryptedStateStoreDetectsTampering covers a file that was edited on
// disk: the authentication tag has to reject it.
func TestEncryptedStateStoreDetectsTampering(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "state.enc")
	store, err := stream.NewEncryptedStateStore(path, passphraseSource("correct horse"))
	if err != nil {
		t.Fatalf("NewEncryptedStateStore() error = %v", err)
	}
	if err := store.Append(context.Background(), stream.CursorMetadata{
		Key:    "events",
		Cursor: "0000000123",
	}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	tamperEntry(t, path)

	reopened, err := stream.NewEncryptedStateStore(path, passphraseSource("correct horse"))
	if err != nil {
		t.Fatalf("NewEncryptedStateStore() error = %v", err)
	}
	_, _, err = reopened.Latest(context.Background(), "events")
	if err == nil {
		t.Fatal("Latest() error = nil for a tampered file")
	}
	if !errors.Is(err, stream.ErrStateAuthentication) {
		t.Errorf("Latest() error = %v, want it to report an authentication failure", err)
	}
	if !strings.Contains(err.Error(), "tampered") {
		t.Errorf("error = %q, want it to say the file was tampered with", err.Error())
	}
}

// tamperEntry flips a byte of the ciphertext of the first entry of an
// encrypted state file.
func tamperEntry(t *testing.T, path string) {
	t.Helper()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	if len(lines) < 2 {
		t.Fatalf("the state file has %d lines, want a header and an entry", len(lines))
	}

	var envelope struct {
		Nonce      string `json:"nonce"`
		Ciphertext string `json:"ciphertext"`
	}
	if err := json.Unmarshal([]byte(lines[1]), &envelope); err != nil {
		t.Fatalf("decoding the entry: %v", err)
	}
	ciphertext, err := base64.StdEncoding.DecodeString(envelope.Ciphertext)
	if err != nil {
		t.Fatalf("decoding the ciphertext: %v", err)
	}
	if len(ciphertext) == 0 {
		t.Fatal("the entry has no ciphertext")
	}
	ciphertext[0] ^= 0xff
	envelope.Ciphertext = base64.StdEncoding.EncodeToString(ciphertext)

	edited, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("encoding the entry: %v", err)
	}
	lines[1] = string(edited)
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
}

// TestEncryptedStateStoreUsesAFreshNoncePerEntry covers the requirement that
// a nonce never repeats under one key.
func TestEncryptedStateStoreUsesAFreshNoncePerEntry(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "state.enc")
	store, err := stream.NewEncryptedStateStore(path, passphraseSource("correct horse"))
	if err != nil {
		t.Fatalf("NewEncryptedStateStore() error = %v", err)
	}

	ctx := context.Background()
	for i := range 20 {
		if err := store.Append(ctx, stream.CursorMetadata{
			Key:    "events",
			Cursor: strings.Repeat("0", 10) + string(rune('a'+i)),
		}); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	seen := map[string]bool{}
	for _, line := range lines[1:] {
		var envelope struct {
			Nonce string `json:"nonce"`
		}
		if err := json.Unmarshal([]byte(line), &envelope); err != nil {
			t.Fatalf("decoding the entry: %v", err)
		}
		if envelope.Nonce == "" {
			t.Fatal("an entry has no nonce")
		}
		if seen[envelope.Nonce] {
			t.Fatalf("the nonce %q was used twice", envelope.Nonce)
		}
		seen[envelope.Nonce] = true
	}
	if len(seen) != 20 {
		t.Errorf("got %d nonces, want 20", len(seen))
	}
}

// TestEncryptedStateStorePrunes checks that the pruner keeps the newest
// entries and that the file is still readable afterwards.
func TestEncryptedStateStorePrunes(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "state.enc")
	store, err := stream.NewEncryptedStateStore(path, passphraseSource("correct horse"))
	if err != nil {
		t.Fatalf("NewEncryptedStateStore() error = %v", err)
	}

	ctx := context.Background()
	for i := range 10 {
		if err := store.Append(ctx, stream.CursorMetadata{
			Key:    "events",
			Cursor: strings.Repeat("0", 9) + string(rune('a'+i)),
		}); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}

	stats, err := store.Prune(ctx, stream.PrunePolicy{Keep: 3})
	if err != nil {
		t.Fatalf("Prune() error = %v", err)
	}
	if stats.Kept != 3 {
		t.Errorf("Prune() kept %d entries, want 3", stats.Kept)
	}
	if stats.Removed != 7 {
		t.Errorf("Prune() removed %d entries, want 7", stats.Removed)
	}

	entries, err := store.Entries(ctx, "events")
	if err != nil {
		t.Fatalf("Entries() error = %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("Entries() returned %d entries, want 3", len(entries))
	}
	// The newest entry must survive the rewrite.
	if want := strings.Repeat("0", 9) + "j"; entries[len(entries)-1].Cursor != want {
		t.Errorf("the newest cursor is %q, want %q", entries[len(entries)-1].Cursor, want)
	}
}

// TestEncryptedStateStoreIsReopenedByAnotherInstance covers the handover: a
// second process reads the file the first one wrote.
func TestEncryptedStateStoreIsReopenedByAnotherInstance(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "state.enc")
	first, err := stream.NewEncryptedStateStore(path, passphraseSource("correct horse"))
	if err != nil {
		t.Fatalf("NewEncryptedStateStore() error = %v", err)
	}
	if err := first.Append(context.Background(), stream.CursorMetadata{
		Key:    "events",
		Cursor: "0000000123",
		Ledger: 123,
	}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	second, err := stream.NewEncryptedStateStore(path, passphraseSource("correct horse"))
	if err != nil {
		t.Fatalf("NewEncryptedStateStore() error = %v", err)
	}
	meta, found, err := second.Latest(context.Background(), "events")
	if err != nil {
		t.Fatalf("Latest() error = %v", err)
	}
	if !found {
		t.Fatal("Latest() found no checkpoint in a file another instance wrote")
	}
	if meta.Cursor != "0000000123" {
		t.Errorf("Latest().Cursor = %q, want %q", meta.Cursor, "0000000123")
	}
}

// TestEncryptedStateStoreMissingFileIsNotAnError covers a first run: there is
// no file yet, and that is not a failure.
func TestEncryptedStateStoreMissingFileIsNotAnError(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "state.enc")
	store, err := stream.NewEncryptedStateStore(path, passphraseSource("correct horse"))
	if err != nil {
		t.Fatalf("NewEncryptedStateStore() error = %v", err)
	}
	_, found, err := store.Latest(context.Background(), "events")
	if err != nil {
		t.Fatalf("Latest() error = %v", err)
	}
	if found {
		t.Error("Latest() found a checkpoint in a file that does not exist")
	}
}

// TestEncryptedStateStoreNeedsAPathAndAKey checks the construction guards.
func TestEncryptedStateStoreNeedsAPathAndAKey(t *testing.T) {
	t.Parallel()

	if _, err := stream.NewEncryptedStateStore("", passphraseSource("x")); err == nil {
		t.Error("NewEncryptedStateStore() error = nil without a path")
	}
	if _, err := stream.NewEncryptedStateStore(filepath.Join(t.TempDir(), "s.enc"), nil); err == nil {
		t.Error("NewEncryptedStateStore() error = nil without a key source")
	}
}

// fakeKMS is a key management service that keeps its keys in memory.
//
// It stands in for AWS KMS and Google Cloud KMS in the tests, so the key
// handling can be exercised without a network or a cloud account.
type fakeKMS struct {
	keys map[string][]byte
}

func newFakeKMS() *fakeKMS {
	return &fakeKMS{keys: map[string][]byte{}}
}

func (k *fakeKMS) Name() string { return "fake-kms" }

func (k *fakeKMS) GenerateDataKey(ctx context.Context) ([]byte, []byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	plaintext := make([]byte, 32)
	for i := range plaintext {
		plaintext[i] = byte(i)
	}
	wrapped := []byte("wrapped:" + string(plaintext))
	k.keys[string(wrapped)] = plaintext
	return plaintext, wrapped, nil
}

func (k *fakeKMS) DecryptDataKey(ctx context.Context, wrapped []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key, found := k.keys[string(wrapped)]
	if !found {
		return nil, errors.New("fake kms: no such key")
	}
	return key, nil
}

// TestEncryptedStateStoreWithAKMSKey covers the key management service path:
// the service holds the key, the file holds only the wrapped form.
func TestEncryptedStateStoreWithAKMSKey(t *testing.T) {
	t.Parallel()

	kms := newFakeKMS()
	path := filepath.Join(t.TempDir(), "state.enc")
	store, err := stream.NewEncryptedStateStore(path, &stream.KMSKeySource{Client: kms})
	if err != nil {
		t.Fatalf("NewEncryptedStateStore() error = %v", err)
	}

	ctx := context.Background()
	if err := store.Append(ctx, stream.CursorMetadata{
		Key:    "events",
		Cursor: "0000000123",
	}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if !strings.Contains(string(raw), "fake-kms") {
		t.Errorf("the state file does not record the key service:\n%s", raw)
	}

	// A second instance with the same service reads the file back.
	second, err := stream.NewEncryptedStateStore(path, &stream.KMSKeySource{Client: kms})
	if err != nil {
		t.Fatalf("NewEncryptedStateStore() error = %v", err)
	}
	meta, found, err := second.Latest(ctx, "events")
	if err != nil {
		t.Fatalf("Latest() error = %v", err)
	}
	if !found {
		t.Fatal("Latest() found no checkpoint")
	}
	if meta.Cursor != "0000000123" {
		t.Errorf("Latest().Cursor = %q, want %q", meta.Cursor, "0000000123")
	}
}

// TestEncryptedStateStoreRejectsAnotherKeyService checks that a file written
// by one service is not read by another.
func TestEncryptedStateStoreRejectsAnotherKeyService(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "state.enc")
	store, err := stream.NewEncryptedStateStore(path, &stream.KMSKeySource{Client: newFakeKMS()})
	if err != nil {
		t.Fatalf("NewEncryptedStateStore() error = %v", err)
	}
	if err := store.Append(context.Background(), stream.CursorMetadata{
		Key:    "events",
		Cursor: "0000000123",
	}); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	other := &otherKMS{}
	reopened, err := stream.NewEncryptedStateStore(path, &stream.KMSKeySource{Client: other})
	if err != nil {
		t.Fatalf("NewEncryptedStateStore() error = %v", err)
	}
	if _, _, err := reopened.Latest(context.Background(), "events"); err == nil {
		t.Fatal("Latest() error = nil for a file written by another key service")
	}
}

// otherKMS is a key management service with a different name.
type otherKMS struct {
	fakeKMS
}

func (k *otherKMS) Name() string { return "other-kms" }

// TestEncryptedStateStoreRejectsAnUnreadableFile checks that a file that is
// not a prism state file is reported rather than silently ignored.
func TestEncryptedStateStoreRejectsAnUnreadableFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "state.enc")
	if err := os.WriteFile(path, []byte("this is not a state file\n"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	store, err := stream.NewEncryptedStateStore(path, passphraseSource("correct horse"))
	if err != nil {
		t.Fatalf("NewEncryptedStateStore() error = %v", err)
	}
	if _, _, err := store.Latest(context.Background(), "events"); err == nil {
		t.Fatal("Latest() error = nil for a file that is not a state file")
	}
}
