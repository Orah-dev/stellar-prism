package sink

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// MarshalRecord renders a record as the one-line JSON object NDJSON emits,
// without the trailing newline.
//
// Sinks that deliver records over another protocol (webhook bodies, archived
// object blocks, syslog messages) use this so every destination shares one
// documented schema. A decoded payload is embedded under the same {type,
// value} envelope lens produces; with decode false the base64 XDR is kept
// under the *Xdr fields.
func MarshalRecord(r stream.Record, decode bool) (json.RawMessage, error) {
	var buf bytes.Buffer
	enc := NewNDJSON(&buf, decode)
	if err := enc.encode(r); err != nil {
		return nil, err
	}
	if err := enc.Flush(); err != nil {
		return nil, fmt.Errorf("encoding record %s: %w", r.ID, err)
	}
	return json.RawMessage(bytes.TrimSuffix(buf.Bytes(), []byte("\n"))), nil
}
