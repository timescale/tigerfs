package format

import (
	"encoding/binary"
	"fmt"
	"math/big"
	"time"

	"github.com/google/uuid"
)

// DisplayNameLayout is the time format for the timestamp portion of UUIDv7
// display names. Uses millisecond precision, UTC, filesystem-safe (no colons).
const DisplayNameLayout = "2006-01-02T150405.000Z"

// displayNameTSLen is the length of the timestamp portion in a display name.
var displayNameTSLen = len(DisplayNameLayout) // 22

// UUIDv7ToDisplayName converts a UUIDv7 to a human-readable display name:
// "<timestamp>-<base36 entropy>", e.g. "2026-04-07T143000.123Z-zzz0063hd8e5r42".
//
// The format is fully reversible via DisplayNameToUUIDv7. It encodes all 122
// meaningful bits of the UUID (48 timestamp + 12 rand_a + 62 rand_b). The 4
// version bits and 2 variant bits are fixed constants reconstructed on decode.
//
// Base36 (0-9a-z) is used for the entropy portion to be case-insensitive safe
// on macOS APFS.
func UUIDv7ToDisplayName(id [16]byte) string {
	ts := ExtractUUIDv7Time(id)
	entropy := packEntropy(id)
	var n big.Int
	n.SetBytes(entropy)
	return fmt.Sprintf("%s-%s", ts.UTC().Format(DisplayNameLayout), n.Text(36))
}

// DisplayNameToUUIDv7 parses a display name back to a UUIDv7. This is the
// inverse of UUIDv7ToDisplayName -- fully reversible with no lookup needed.
func DisplayNameToUUIDv7(name string) (uuid.UUID, error) {
	var id uuid.UUID

	// Find the separator between timestamp and entropy.
	// The timestamp is fixed-length (22 chars: "2006-01-02T150405.000Z").
	// The entropy follows after a "-".
	if len(name) < displayNameTSLen+2 { // timestamp + dash + at least 1 entropy char
		return id, fmt.Errorf("invalid UUIDv7 display name %q: too short", name)
	}
	tsStr := name[:displayNameTSLen]
	if name[displayNameTSLen] != '-' {
		return id, fmt.Errorf("invalid UUIDv7 display name %q: expected '-' at position %d", name, displayNameTSLen)
	}
	entropyStr := name[displayNameTSLen+1:]

	// Parse timestamp -> reconstruct bits 0-47
	ts, err := time.Parse(DisplayNameLayout, tsStr)
	if err != nil {
		return id, fmt.Errorf("invalid UUIDv7 display name %q: %w", name, err)
	}
	msec := ts.UnixMilli()

	// Reconstruct first 6 bytes from millisecond timestamp
	binary.BigEndian.PutUint16(id[0:2], uint16(msec>>32))
	binary.BigEndian.PutUint32(id[2:6], uint32(msec))

	// Parse base36 entropy -> reconstruct bits 52-63 and 66-127
	var n big.Int
	_, ok := n.SetString(entropyStr, 36)
	if !ok {
		return id, fmt.Errorf("invalid UUIDv7 display name %q: invalid base36 entropy", name)
	}
	entropy := n.Bytes()
	unpackEntropy(id[:], entropy)

	// Set version bits (48-51 = 0111) and variant bits (64-65 = 10)
	id[6] = (id[6] & 0x0F) | 0x70 // version 7
	id[8] = (id[8] & 0x3F) | 0x80 // variant 10

	return id, nil
}

// IsUUIDv7 checks whether a UUID is version 7 by inspecting the version bits
// at positions 48-51 (the high nibble of byte 6).
func IsUUIDv7(id [16]byte) bool {
	return (id[6] >> 4) == 7
}

// IsDisplayName checks whether a string looks like a UUIDv7 display name
// (timestamp-base36 format). Used for path resolution to distinguish display
// names from hex UUIDs.
func IsDisplayName(s string) bool {
	if len(s) < displayNameTSLen+2 {
		return false
	}
	if s[displayNameTSLen] != '-' {
		return false
	}
	_, err := time.Parse(DisplayNameLayout, s[:displayNameTSLen])
	return err == nil
}

// ExtractUUIDv7Time extracts the millisecond timestamp from a UUIDv7.
// UUIDv7 stores a Unix timestamp in milliseconds in the first 48 bits.
func ExtractUUIDv7Time(id [16]byte) time.Time {
	b := id[:]
	msec := int64(binary.BigEndian.Uint16(b[0:2]))<<32 |
		int64(binary.BigEndian.Uint32(b[2:6]))
	return time.UnixMilli(msec)
}

// packEntropy extracts the 74 entropy bits (rand_a + rand_b) from a UUIDv7
// and packs them into a byte slice suitable for big.Int encoding.
func packEntropy(id [16]byte) []byte {
	// Extract rand_a (12 bits): low nibble of byte 6 + all of byte 7
	randA := uint16(id[6]&0x0F)<<8 | uint16(id[7])

	// Extract rand_b (62 bits): low 6 bits of byte 8 + bytes 9-15
	var randB uint64
	randB = uint64(id[8]&0x3F) << 56
	randB |= uint64(id[9]) << 48
	randB |= uint64(id[10]) << 40
	randB |= uint64(id[11]) << 32
	randB |= uint64(id[12]) << 24
	randB |= uint64(id[13]) << 16
	randB |= uint64(id[14]) << 8
	randB |= uint64(id[15])

	// Pack 74 bits into 10 bytes (80 bits, 6 leading zero bits)
	buf := make([]byte, 10)
	buf[0] = byte(randA >> 4)
	buf[1] = byte(randA<<4) | byte(randB>>58)
	buf[2] = byte(randB >> 50)
	buf[3] = byte(randB >> 42)
	buf[4] = byte(randB >> 34)
	buf[5] = byte(randB >> 26)
	buf[6] = byte(randB >> 18)
	buf[7] = byte(randB >> 10)
	buf[8] = byte(randB >> 2)
	buf[9] = byte(randB << 6)

	return buf
}

// unpackEntropy restores the 74 entropy bits from a big.Int byte encoding
// back into the UUID byte array (bytes 6-15), preserving version and variant
// bits which are set by the caller.
func unpackEntropy(id []byte, entropy []byte) {
	// Pad entropy to 10 bytes (big.Int.Bytes() strips leading zeros)
	padded := make([]byte, 10)
	copy(padded[10-len(entropy):], entropy)

	// Extract rand_a (12 bits) from bits 6-17 of the 80-bit field
	randA := uint16(padded[0])<<4 | uint16(padded[1]>>4)

	// Extract rand_b (62 bits) from bits 18-79
	var randB uint64
	randB = uint64(padded[1]&0x0F) << 58
	randB |= uint64(padded[2]) << 50
	randB |= uint64(padded[3]) << 42
	randB |= uint64(padded[4]) << 34
	randB |= uint64(padded[5]) << 26
	randB |= uint64(padded[6]) << 18
	randB |= uint64(padded[7]) << 10
	randB |= uint64(padded[8]) << 2
	randB |= uint64(padded[9]) >> 6

	// Write rand_a into bytes 6-7 (preserving version bits in high nibble of byte 6)
	id[6] = (id[6] & 0xF0) | byte(randA>>8)
	id[7] = byte(randA)

	// Write rand_b into bytes 8-15 (preserving variant bits in high 2 bits of byte 8)
	id[8] = (id[8] & 0xC0) | byte(randB>>56)
	id[9] = byte(randB >> 48)
	id[10] = byte(randB >> 40)
	id[11] = byte(randB >> 32)
	id[12] = byte(randB >> 24)
	id[13] = byte(randB >> 16)
	id[14] = byte(randB >> 8)
	id[15] = byte(randB)
}
