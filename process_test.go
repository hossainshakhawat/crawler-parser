package main

import (
	"bytes"
	"compress/gzip"
	"testing"
)

func TestGzipDecompress_RoundTrip(t *testing.T) {
	input := []byte("hello, compressed parser world!")
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(input); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	got, err := gzipDecompress(buf.Bytes())
	if err != nil {
		t.Fatalf("gzipDecompress: %v", err)
	}
	if !bytes.Equal(got, input) {
		t.Errorf("got %q, want %q", got, input)
	}
}

func TestGzipDecompress_InvalidData(t *testing.T) {
	_, err := gzipDecompress([]byte("this is not gzip data"))
	if err == nil {
		t.Error("expected error for invalid gzip data")
	}
}

func TestGzipDecompress_Empty(t *testing.T) {
	_, err := gzipDecompress([]byte{})
	if err == nil {
		t.Error("expected error for empty input")
	}
}
