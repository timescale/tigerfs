package synth

import (
	"bytes"
	"encoding/base64"
	"unicode/utf8"
)

// IsBinary returns true if data contains null bytes or is not valid UTF-8.
// Used to decide whether to base64-encode content for TEXT column storage.
func IsBinary(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	if bytes.ContainsRune(data, 0) {
		return true
	}
	return !utf8.Valid(data)
}

// EncodeBody base64-encodes binary data for storage in a TEXT column.
func EncodeBody(data []byte) string {
	return base64.StdEncoding.EncodeToString(data)
}

// DecodeBody decodes a base64-encoded string back to raw bytes.
func DecodeBody(encoded string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(encoded)
}
