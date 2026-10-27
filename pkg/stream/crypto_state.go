package stream

import (
	"bufio"
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// DefaultPBKDF2Iterations is the work factor used to turn a passphrase into
// an AES-256 key.
//
// A hundred thousand iterations of PBKDF2-HMAC-SHA256 costs a fraction of a
// second, which is paid once when the state file is opened, and makes a
// stolen passphrase far more expensive to attack offline.
const DefaultPBKDF2Iterations = 100000

// stateFileVersion is the version of the encrypted state file format.
const stateFileVersion = 1

// stateAAD is the additional data every sealed entry is bound to.
//
// Binding the entries to a fixed label stops a ciphertext from one file
// being moved into another and decrypted successfully.
var stateAAD = []byte("prism.state.v1")

// ErrStateAuthentication reports that a state file could not be
// authenticated.
//
// It means the file was tampered with, truncated, or written with a
// different key: in every case the ciphertext is not what this key sealed.
var ErrStateAuthentication = errors.New("state file: authentication failed")

// KeyDescriptor records how the key of a state file was derived.
//
// It is stored in the clear at the top of the file. It describes how to
// reproduce the key, not the key itself, so a file can be read again by
// whoever holds the same secret.
type KeyDescriptor struct {
	// KDF names how the key was derived: "pbkdf2-hmac-sha256" for a
	// passphrase, or the name of the key management service.
	KDF string `json:"kdf"`
	// Iterations is the PBKDF2 work factor. It is zero for a KMS key.
	Iterations int `json:"iterations,omitempty"`
	// Salt is the PBKDF2 salt. It is empty for a KMS key.
	Salt []byte `json:"salt,omitempty"`
	// Wrapped is the KMS-wrapped data key. It is empty for a passphrase.
	Wrapped []byte `json:"wrapped,omitempty"`
}

// KeySource resolves the key material used to encrypt a state file.
//
// Implementations are provided for an environment variable, a key file and
// cloud key management services.
type KeySource interface {
	// Resolve returns the key for a file that does not exist yet, and the
	// descriptor that records how to reproduce it.
	Resolve(ctx context.Context) (key []byte, desc KeyDescriptor, err error)
	// Restore returns the key for an existing file, given the descriptor
	// read from it.
	Restore(ctx context.Context, desc KeyDescriptor) ([]byte, error)
}

// PassphraseKeySource derives an AES-256 key from a passphrase.
//
// The passphrase is never stored: only the salt, the work factor and the
// derivation name go into the file, so the file says how to reproduce the
// key without giving the key away.
type PassphraseKeySource struct {
	// Passphrase is the secret the key is derived from.
	Passphrase string
	// Salt is the PBKDF2 salt. When empty, a random one is generated for
	// each new file.
	Salt []byte
	// Iterations is the PBKDF2 work factor. Zero uses
	// DefaultPBKDF2Iterations.
	Iterations int
	// Rand is the source of randomness for the salt. Nil uses
	// crypto/rand.Reader.
	Rand io.Reader
}

// Resolve implements KeySource.
func (s *PassphraseKeySource) Resolve(ctx context.Context) ([]byte, KeyDescriptor, error) {
	if err := ctx.Err(); err != nil {
		return nil, KeyDescriptor{}, err
	}
	if s.Passphrase == "" {
		return nil, KeyDescriptor{}, errors.New("state key: a passphrase is required")
	}
	iterations := s.Iterations
	if iterations <= 0 {
		iterations = DefaultPBKDF2Iterations
	}
	salt := s.Salt
	if len(salt) == 0 {
		salt = make([]byte, 16)
		random := s.Rand
		if random == nil {
			random = rand.Reader
		}
		if _, err := io.ReadFull(random, salt); err != nil {
			return nil, KeyDescriptor{}, fmt.Errorf("state key: generating the salt: %w", err)
		}
	}
	key := pbkdf2SHA256([]byte(s.Passphrase), salt, iterations, 32)
	desc := KeyDescriptor{
		KDF:        "pbkdf2-hmac-sha256",
		Iterations: iterations,
		Salt:       salt,
	}
	return key, desc, nil
}

// Restore implements KeySource.
func (s *PassphraseKeySource) Restore(ctx context.Context, desc KeyDescriptor) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if desc.KDF != "pbkdf2-hmac-sha256" {
		return nil, fmt.Errorf("state key: the file was written with %q, not a passphrase", desc.KDF)
	}
	if len(desc.Salt) == 0 {
		return nil, errors.New("state key: the file has no salt")
	}
	if desc.Iterations <= 0 {
		return nil, errors.New("state key: the file has no work factor")
	}
	if desc.Iterations > maxPBKDF2Iterations {
		// A tampered header must not be able to make prism spend minutes
		// deriving a key.
		return nil, fmt.Errorf("state key: the file asks for %d iterations, more than the %d allowed",
			desc.Iterations, maxPBKDF2Iterations)
	}
	return pbkdf2SHA256([]byte(s.Passphrase), desc.Salt, desc.Iterations, 32), nil
}

// maxPBKDF2Iterations bounds the work factor a state file may ask for.
const maxPBKDF2Iterations = 10_000_000

// KMSKeySource derives the key of a state file from a key management
// service.
//
// The service generates a data key once, when the file is created, and the
// wrapped form of that key is stored in the file. Reading the file again
// unwraps it, so the plaintext key is never written to disk.
type KMSKeySource struct {
	// Client talks to the key management service.
	Client KMSClient
}

// Resolve implements KeySource.
func (s *KMSKeySource) Resolve(ctx context.Context) ([]byte, KeyDescriptor, error) {
	if err := ctx.Err(); err != nil {
		return nil, KeyDescriptor{}, err
	}
	if s.Client == nil {
		return nil, KeyDescriptor{}, errors.New("state key: a key management client is required")
	}
	plaintext, wrapped, err := s.Client.GenerateDataKey(ctx)
	if err != nil {
		return nil, KeyDescriptor{}, err
	}
	if len(plaintext) != 32 {
		return nil, KeyDescriptor{}, fmt.Errorf("state key: the key management service returned a %d byte key, want 32",
			len(plaintext))
	}
	desc := KeyDescriptor{KDF: s.Client.Name(), Wrapped: wrapped}
	return plaintext, desc, nil
}

// Restore implements KeySource.
func (s *KMSKeySource) Restore(ctx context.Context, desc KeyDescriptor) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.Client == nil {
		return nil, errors.New("state key: a key management client is required")
	}
	if desc.KDF != s.Client.Name() {
		return nil, fmt.Errorf("state key: the file was written with %q, not %q", desc.KDF, s.Client.Name())
	}
	if len(desc.Wrapped) == 0 {
		return nil, errors.New("state key: the file has no wrapped key")
	}
	key, err := s.Client.DecryptDataKey(ctx, desc.Wrapped)
	if err != nil {
		return nil, err
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("state key: the key management service returned a %d byte key, want 32", len(key))
	}
	return key, nil
}

// pbkdf2SHA256 implements PBKDF2 (RFC 8018) with HMAC-SHA256.
//
// It is written out rather than pulled in, because the state file needs one
// key derivation and prism has no business carrying a cryptography library
// for it.
func pbkdf2SHA256(password, salt []byte, iterations, keyLen int) []byte {
	prf := hmac.New(sha256.New, password)
	hashLen := prf.Size()
	blocks := (keyLen + hashLen - 1) / hashLen

	out := make([]byte, 0, blocks*hashLen)
	block := make([]byte, 4)
	u := make([]byte, 0, hashLen)
	for i := 1; i <= blocks; i++ {
		prf.Reset()
		prf.Write(salt)
		block[0] = byte(i >> 24)
		block[1] = byte(i >> 16)
		block[2] = byte(i >> 8)
		block[3] = byte(i)
		prf.Write(block)
		u = prf.Sum(u[:0])

		t := make([]byte, hashLen)
		copy(t, u)
		for n := 1; n < iterations; n++ {
			prf.Reset()
			prf.Write(u)
			u = prf.Sum(u[:0])
			for j := range t {
				t[j] ^= u[j]
			}
		}
		out = append(out, t...)
	}
	return out[:keyLen]
}

// stateHeader is the first line of an encrypted state file.
//
// It is not secret: it says how the key was derived, so the file can be
// read again. It is not trusted either: the work factor is bounded when it
// is read, and the entries themselves are authenticated.
type stateHeader struct {
	Version int           `json:"version"`
	Alg     string        `json:"alg"`
	Key     KeyDescriptor `json:"key"`
}

// stateEnvelope is one encrypted entry of an encrypted state file.
type stateEnvelope struct {
	Version    int    `json:"version"`
	Nonce      string `json:"nonce"`
	Ciphertext string `json:"ciphertext"`
}

// EncryptedStateStore keeps cursor metadata in an encrypted file.
//
// It is the same journal as the plaintext one — one checkpoint per line,
// rewritten in place by the pruner — with every entry sealed under
// AES-256-GCM. The key never touches the disk: it comes from a passphrase,
// a key file or a key management service, and only the parameters needed
// to reproduce it are written to the file.
//
// Every entry gets its own nonce, drawn from the system randomness source.
// GCM's security rests on a nonce never repeating under one key, and a
// random 96-bit nonce makes a repeat vanishingly unlikely even for a file
// that is written many times.
//
// A file that was tampered with, truncated or written with a different key
// fails to authenticate, and is reported as such rather than being read as
// if it were intact.
type EncryptedStateStore struct {
	path   string
	source KeySource

	mu          sync.Mutex
	key         []byte
	desc        KeyDescriptor
	initialised bool
}

// NewEncryptedStateStore returns a store backed by the file at path and
// encrypted with the key the source resolves.
//
// The file itself is created by the first append. The parent directory must
// already exist, for the same reason the plaintext store insists on it: a
// typo in the path should be reported now, not hours into a run.
func NewEncryptedStateStore(path string, source KeySource) (*EncryptedStateStore, error) {
	if path == "" {
		return nil, errors.New("encrypted state: a file path is required")
	}
	if source == nil {
		return nil, errors.New("encrypted state: a key source is required")
	}
	dir := filepath.Dir(path)
	info, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("encrypted state directory %s: %w", dir, err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("encrypted state directory %s is not a directory", dir)
	}
	return &EncryptedStateStore{path: path, source: source}, nil
}

// Path returns the file the store writes to.
func (s *EncryptedStateStore) Path() string { return s.path }

// Append records one checkpoint.
//
// The entry is appended as a single line, so a crash can lose at most the
// line being written and never corrupts the entries before it.
func (s *EncryptedStateStore) Append(ctx context.Context, meta CursorMetadata) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if meta.RecordedAt.IsZero() {
		meta.RecordedAt = time.Now().UTC()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ensureKey(ctx); err != nil {
		return err
	}
	line, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("encoding cursor metadata: %w", err)
	}
	sealed, err := s.seal(line)
	if err != nil {
		return err
	}
	sealed = append(sealed, '\n')

	f, err := os.OpenFile(s.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("opening encrypted state file %s: %w", s.path, err)
	}
	if _, err := f.Write(sealed); err != nil {
		closeErr := f.Close()
		return errors.Join(
			fmt.Errorf("writing encrypted state file %s: %w", s.path, err),
			closeErr)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("closing encrypted state file %s: %w", s.path, err)
	}
	return nil
}

// Latest returns the most recent checkpoint recorded for key.
//
// A missing file is not an error: it simply holds no checkpoints.
func (s *EncryptedStateStore) Latest(ctx context.Context, key string) (CursorMetadata, bool, error) {
	if err := ctx.Err(); err != nil {
		return CursorMetadata{}, false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ensureKey(ctx); err != nil {
		return CursorMetadata{}, false, err
	}
	entries, err := s.readEntries()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return CursorMetadata{}, false, nil
		}
		return CursorMetadata{}, false, err
	}

	var (
		latest CursorMetadata
		found  bool
	)
	for _, entry := range entries {
		if entry.meta.Key != key {
			continue
		}
		latest = entry.meta
		found = true
	}
	return latest, found, nil
}

// Entries returns every checkpoint recorded for key, oldest first.
//
// It reads and decrypts the whole journal, so it is meant for inspection
// and for tests rather than for the streaming path.
func (s *EncryptedStateStore) Entries(ctx context.Context, key string) ([]CursorMetadata, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ensureKey(ctx); err != nil {
		return nil, err
	}
	entries, err := s.readEntries()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}

	var out []CursorMetadata
	for _, entry := range entries {
		if entry.meta.Key == key {
			out = append(out, entry.meta)
		}
	}
	return out, nil
}

// Prune rewrites the file, keeping the entries the policy calls for.
//
// The rewrite is atomic: the new content is written to a temporary file in
// the same directory and renamed over the old one, so a reader either sees
// the whole old file or the whole new one.
func (s *EncryptedStateStore) Prune(ctx context.Context, policy PrunePolicy) (PruneStats, error) {
	if err := ctx.Err(); err != nil {
		return PruneStats{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ensureKey(ctx); err != nil {
		return PruneStats{}, err
	}
	entries, err := s.readEntries()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return PruneStats{}, nil
		}
		return PruneStats{}, err
	}

	// The counts are taken first, so the rewrite can keep the newest
	// entries of every key without holding the whole journal in memory.
	counts := make(map[string]int)
	for _, entry := range entries {
		if expired(entry.meta, policy.Cutoff) {
			continue
		}
		counts[entry.meta.Key]++
	}

	stats := PruneStats{}
	seen := make(map[string]int)
	var kept [][]byte
	for _, entry := range entries {
		if expired(entry.meta, policy.Cutoff) {
			stats.Removed++
			continue
		}
		index := seen[entry.meta.Key]
		seen[entry.meta.Key] = index + 1
		if policy.Keep > 0 && index < counts[entry.meta.Key]-policy.Keep {
			stats.Removed++
			continue
		}
		kept = append(kept, entry.line)
		stats.Kept++
	}

	if err := s.rewrite(kept); err != nil {
		return PruneStats{}, err
	}
	return stats, nil
}

// stateEntry is one decrypted checkpoint and the line it came from.
type stateEntry struct {
	meta CursorMetadata
	line []byte
}

// rewrite replaces the file with the header and the given entries.
//
// The entries are already sealed, so the rewrite does not have to decrypt
// and re-encrypt them: what was written is what is kept.
func (s *EncryptedStateStore) rewrite(entries [][]byte) error {
	mode := os.FileMode(0o600)
	if info, err := os.Stat(s.path); err == nil {
		mode = info.Mode().Perm()
	}

	tmp, err := os.CreateTemp(filepath.Dir(s.path), filepath.Base(s.path)+".prune-*")
	if err != nil {
		return fmt.Errorf("creating temporary encrypted state file: %w", err)
	}
	tmpName := tmp.Name()
	defer func() {
		// A no-op once the rename succeeded; the point is to clean up
		// after a failure.
		_ = os.Remove(tmpName)
	}()

	header, err := s.headerLine()
	if err != nil {
		closeErr := tmp.Close()
		return errors.Join(err, closeErr)
	}
	if _, err := tmp.Write(header); err != nil {
		closeErr := tmp.Close()
		return errors.Join(fmt.Errorf("writing temporary encrypted state file: %w", err), closeErr)
	}
	for _, entry := range entries {
		if _, err := tmp.Write(entry); err != nil {
			closeErr := tmp.Close()
			return errors.Join(fmt.Errorf("writing temporary encrypted state file: %w", err), closeErr)
		}
		// The entries are stored one per line, and the line they came
		// from had its newline trimmed away.
		if _, err := tmp.Write([]byte{'\n'}); err != nil {
			closeErr := tmp.Close()
			return errors.Join(fmt.Errorf("writing temporary encrypted state file: %w", err), closeErr)
		}
	}
	if err := tmp.Sync(); err != nil {
		closeErr := tmp.Close()
		return errors.Join(fmt.Errorf("syncing temporary encrypted state file: %w", err), closeErr)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing temporary encrypted state file: %w", err)
	}
	if err := os.Chmod(tmpName, mode); err != nil {
		return fmt.Errorf("setting permissions on temporary encrypted state file: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		return fmt.Errorf("replacing encrypted state file %s: %w", s.path, err)
	}
	return nil
}

// headerLine returns the header of the file as it is written.
func (s *EncryptedStateStore) headerLine() ([]byte, error) {
	header := stateHeader{
		Version: stateFileVersion,
		Alg:     "aes-256-gcm",
		Key:     s.desc,
	}
	b, err := json.Marshal(header)
	if err != nil {
		return nil, fmt.Errorf("encoding encrypted state header: %w", err)
	}
	return append(b, '\n'), nil
}

// ensureKey resolves the key, reading the header of an existing file or
// creating one for a new file.
//
// The caller holds the lock.
func (s *EncryptedStateStore) ensureKey(ctx context.Context) error {
	if s.initialised {
		return nil
	}

	desc, found, err := s.readHeader()
	if err != nil {
		return err
	}
	if found {
		key, err := s.source.Restore(ctx, desc)
		if err != nil {
			return err
		}
		if len(key) != 32 {
			return fmt.Errorf("encrypted state: the key is %d bytes, want 32", len(key))
		}
		s.key = key
		s.desc = desc
		s.initialised = true
		return nil
	}

	key, desc, err := s.source.Resolve(ctx)
	if err != nil {
		return err
	}
	if len(key) != 32 {
		return fmt.Errorf("encrypted state: the key is %d bytes, want 32", len(key))
	}
	s.key = key
	s.desc = desc
	s.initialised = true
	return s.writeHeader()
}

// readHeader reads the header of the file.
//
// The second result is false when the file does not exist yet or is empty.
func (s *EncryptedStateStore) readHeader() (KeyDescriptor, bool, error) {
	f, err := os.Open(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return KeyDescriptor{}, false, nil
		}
		return KeyDescriptor{}, false, fmt.Errorf("opening encrypted state file %s: %w", s.path, err)
	}
	defer func() {
		_ = f.Close()
	}()

	reader := newLineReader(f)
	line, err := reader.next()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, errNoLine) {
			return KeyDescriptor{}, false, nil
		}
		return KeyDescriptor{}, false, fmt.Errorf("reading encrypted state file %s: %w", s.path, err)
	}
	if len(line) == 0 {
		return KeyDescriptor{}, false, nil
	}

	var header stateHeader
	if err := json.Unmarshal(line, &header); err != nil {
		return KeyDescriptor{}, false, fmt.Errorf(
			"encrypted state file %s is not a prism state file: %w", s.path, err)
	}
	if header.Version != stateFileVersion {
		return KeyDescriptor{}, false, fmt.Errorf(
			"encrypted state file %s has version %d, want %d", s.path, header.Version, stateFileVersion)
	}
	if header.Alg != "aes-256-gcm" {
		return KeyDescriptor{}, false, fmt.Errorf(
			"encrypted state file %s uses %q, want aes-256-gcm", s.path, header.Alg)
	}
	return header.Key, true, nil
}

// writeHeader writes the header of a new file.
//
// The file is created exclusively, so two processes opening the same new
// state file cannot both write a header: the loser reads the winner's.
func (s *EncryptedStateStore) writeHeader() error {
	header, err := s.headerLine()
	if err != nil {
		return err
	}

	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			// Somebody else created the file first. Its header is the one
			// that counts.
			desc, found, readErr := s.readHeader()
			if readErr != nil {
				return readErr
			}
			if !found {
				return fmt.Errorf("encrypted state file %s was created without a header", s.path)
			}
			key, restoreErr := s.source.Restore(context.Background(), desc)
			if restoreErr != nil {
				return restoreErr
			}
			if len(key) != 32 {
				return fmt.Errorf("encrypted state: the key is %d bytes, want 32", len(key))
			}
			s.key = key
			s.desc = desc
			return nil
		}
		return fmt.Errorf("creating encrypted state file %s: %w", s.path, err)
	}
	if _, err := f.Write(header); err != nil {
		closeErr := f.Close()
		return errors.Join(fmt.Errorf("writing encrypted state file %s: %w", s.path, err), closeErr)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("closing encrypted state file %s: %w", s.path, err)
	}
	return nil
}

// readEntries reads and decrypts every entry of the file.
//
// The header is skipped: it describes the file rather than being one of
// its checkpoints.
func (s *EncryptedStateStore) readEntries() ([]stateEntry, error) {
	f, err := os.Open(s.path)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = f.Close()
	}()

	reader := newLineReader(f)
	// The header.
	if _, err := reader.next(); err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, errNoLine) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading encrypted state file %s: %w", s.path, err)
	}

	var entries []stateEntry
	for {
		line, err := reader.next()
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, errNoLine) {
			return entries, nil
		}
		if err != nil {
			return nil, fmt.Errorf("reading encrypted state file %s: %w", s.path, err)
		}
		plain, err := s.open(line)
		if err != nil {
			return nil, err
		}
		var meta CursorMetadata
		if err := json.Unmarshal(plain, &meta); err != nil {
			return nil, fmt.Errorf("decoding cursor metadata in %s: %w", s.path, err)
		}
		entries = append(entries, stateEntry{meta: meta, line: line})
	}
}

// seal encrypts one entry under the store's key.
func (s *EncryptedStateStore) seal(plaintext []byte) ([]byte, error) {
	gcm, err := s.gcm()
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("generating a nonce: %w", err)
	}
	sealed := gcm.Seal(nil, nonce, plaintext, stateAAD)
	envelope := stateEnvelope{
		Version:    stateFileVersion,
		Nonce:      base64.StdEncoding.EncodeToString(nonce),
		Ciphertext: base64.StdEncoding.EncodeToString(sealed),
	}
	b, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("encoding encrypted state entry: %w", err)
	}
	return b, nil
}

// open decrypts one entry under the store's key.
//
// A failure to authenticate is reported as ErrStateAuthentication: the file
// was tampered with, truncated, or written with a different key, and in
// every case its contents cannot be trusted.
func (s *EncryptedStateStore) open(line []byte) ([]byte, error) {
	var envelope stateEnvelope
	if err := json.Unmarshal(line, &envelope); err != nil {
		return nil, fmt.Errorf("encrypted state file %s has an unreadable entry: %w", s.path, err)
	}
	nonce, err := base64.StdEncoding.DecodeString(envelope.Nonce)
	if err != nil {
		return nil, fmt.Errorf("encrypted state file %s has an unreadable nonce: %w", s.path, err)
	}
	ciphertext, err := base64.StdEncoding.DecodeString(envelope.Ciphertext)
	if err != nil {
		return nil, fmt.Errorf("encrypted state file %s has unreadable ciphertext: %w", s.path, err)
	}

	gcm, err := s.gcm()
	if err != nil {
		return nil, err
	}
	plaintext, err := gcm.Open(nil, nonce, ciphertext, stateAAD)
	if err != nil {
		return nil, fmt.Errorf("%w: %s was tampered with, truncated, or written with a different key: %w",
			ErrStateAuthentication, s.path, err)
	}
	return plaintext, nil
}

// gcm returns the AEAD the store encrypts with.
func (s *EncryptedStateStore) gcm() (cipher.AEAD, error) {
	block, err := aes.NewCipher(s.key)
	if err != nil {
		return nil, fmt.Errorf("encrypted state: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("encrypted state: %w", err)
	}
	return gcm, nil
}

// errNoLine reports that a reader has no more lines.
var errNoLine = errors.New("no more lines")

// lineReader reads a file one line at a time.
//
// The lines of a state file are sealed entries, which are longer than the
// JSON they carry, so the scanner is given room for them.
type lineReader struct {
	scanner *bufio.Scanner
}

// newLineReader returns a reader over r.
func newLineReader(r io.Reader) *lineReader {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, bufio.MaxScanTokenSize), maxMetadataLine)
	return &lineReader{scanner: scanner}
}

// next returns the next non-empty line, trimmed of its surrounding space.
//
// It returns errNoLine once the file is exhausted.
func (r *lineReader) next() ([]byte, error) {
	for r.scanner.Scan() {
		line := bytes.TrimSpace(r.scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		return append([]byte(nil), line...), nil
	}
	if err := r.scanner.Err(); err != nil {
		return nil, err
	}
	return nil, errNoLine
}
