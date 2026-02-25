package synth

import (
	"bytes"
	"encoding/base64"
	"testing"
)

func TestIsBinary(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want bool
	}{
		{"empty", []byte{}, false},
		{"plain text", []byte("hello world"), false},
		{"utf8 with emoji", []byte("hello 🌍"), false},
		{"null byte", []byte("hello\x00world"), true},
		{"binary data", []byte{0xFF, 0xD8, 0xFF, 0xE0}, true}, // JPEG header
		{"invalid utf8", []byte{0x80, 0x81, 0x82}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsBinary(tt.data); got != tt.want {
				t.Errorf("IsBinary() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEncodeBody(t *testing.T) {
	binary := []byte{0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10}
	encoded := EncodeBody(binary)
	expected := base64.StdEncoding.EncodeToString(binary)
	if encoded != expected {
		t.Errorf("EncodeBody() = %q, want %q", encoded, expected)
	}
}

func TestDecodeBody(t *testing.T) {
	original := []byte{0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10}
	encoded := base64.StdEncoding.EncodeToString(original)
	decoded, err := DecodeBody(encoded)
	if err != nil {
		t.Fatalf("DecodeBody() error: %v", err)
	}
	if !bytes.Equal(decoded, original) {
		t.Errorf("DecodeBody() = %v, want %v", decoded, original)
	}
}

func TestDecodeBody_Invalid(t *testing.T) {
	_, err := DecodeBody("not valid base64!!!")
	if err == nil {
		t.Fatal("expected error for invalid base64")
	}
}
