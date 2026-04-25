package store

import (
	"testing"
)

func TestHashURL_Deterministic(t *testing.T) {
	a := HashURL("https://example.com/page")
	b := HashURL("https://example.com/page")
	if a != b {
		t.Error("same URL should produce the same hash")
	}
}

func TestHashURL_UniquePerURL(t *testing.T) {
	a := HashURL("https://example.com/page1")
	b := HashURL("https://example.com/page2")
	if a == b {
		t.Error("different URLs should produce different hashes")
	}
}

func TestHashURL_Length(t *testing.T) {
	h := HashURL("https://example.com")
	if len(h) != 64 { // SHA-256 as hex = 64 characters
		t.Errorf("hash length: got %d, want 64", len(h))
	}
}

func TestHashURL_Empty(t *testing.T) {
	h := HashURL("")
	if len(h) != 64 {
		t.Errorf("hash of empty string: length %d, want 64", len(h))
	}
}

func TestContentHash_Deterministic(t *testing.T) {
	a := ContentHash([]byte("hello world"))
	b := ContentHash([]byte("hello world"))
	if a != b {
		t.Error("same content should produce the same hash")
	}
}

func TestContentHash_UniquePerContent(t *testing.T) {
	a := ContentHash([]byte("content A"))
	b := ContentHash([]byte("content B"))
	if a == b {
		t.Error("different content should produce different hashes")
	}
}

func TestContentHash_Length(t *testing.T) {
	h := ContentHash([]byte("test"))
	if len(h) != 64 {
		t.Errorf("hash length: got %d, want 64", len(h))
	}
}

func TestHostOf(t *testing.T) {
	tests := []struct {
		url  string
		want string
	}{
		{"https://example.com/path", "example.com"},
		{"http://www.google.com/search?q=test", "www.google.com"},
		{"https://example.com:8080/path", "example.com"},
		{"https://sub.domain.example.com/", "sub.domain.example.com"},
		{"not-a-url", ""},
	}
	for _, tt := range tests {
		got := HostOf(tt.url)
		if got != tt.want {
			t.Errorf("HostOf(%q) = %q, want %q", tt.url, got, tt.want)
		}
	}
}
