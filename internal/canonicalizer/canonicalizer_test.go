package canonicalizer

import (
	"testing"
)

func TestNormalize_AbsoluteURL(t *testing.T) {
	got, err := Normalize("https://example.com/path", "https://base.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "https://example.com/path" {
		t.Errorf("got %q, want %q", got, "https://example.com/path")
	}
}

func TestNormalize_RelativePath(t *testing.T) {
	got, err := Normalize("/about", "https://example.com/home")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "https://example.com/about" {
		t.Errorf("got %q, want %q", got, "https://example.com/about")
	}
}

func TestNormalize_RemovesFragment(t *testing.T) {
	got, err := Normalize("https://example.com/page#section", "https://example.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "https://example.com/page" {
		t.Errorf("got %q, want %q", got, "https://example.com/page")
	}
}

func TestNormalize_RemovesTrailingSlash(t *testing.T) {
	got, err := Normalize("https://example.com/path/", "https://example.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "https://example.com/path" {
		t.Errorf("got %q, want %q", got, "https://example.com/path")
	}
}

func TestNormalize_RemovesTrackingParams(t *testing.T) {
	got, err := Normalize("https://example.com/page?utm_source=foo&id=1", "https://example.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "https://example.com/page?id=1" {
		t.Errorf("got %q, want %q", got, "https://example.com/page?id=1")
	}
}

func TestNormalize_RemovesAllTrackingParams(t *testing.T) {
	got, err := Normalize("https://example.com/page?utm_source=a&utm_medium=b&fbclid=c", "https://example.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "https://example.com/page" {
		t.Errorf("got %q, want %q", got, "https://example.com/page")
	}
}

func TestNormalize_NonHTTPScheme(t *testing.T) {
	got, err := Normalize("ftp://example.com/file", "https://base.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "" {
		t.Errorf("expected empty string for non-http scheme, got %q", got)
	}
}

func TestNormalize_RemovesDefaultHTTPSPort(t *testing.T) {
	got, err := Normalize("https://example.com:443/path", "https://example.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "https://example.com/path" {
		t.Errorf("got %q, want %q", got, "https://example.com/path")
	}
}

func TestNormalize_RemovesDefaultHTTPPort(t *testing.T) {
	got, err := Normalize("http://example.com:80/path", "http://example.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "http://example.com/path" {
		t.Errorf("got %q, want %q", got, "http://example.com/path")
	}
}

func TestNormalize_SortsQueryParams(t *testing.T) {
	got, err := Normalize("https://example.com/page?z=3&a=1", "https://example.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "https://example.com/page?a=1&z=3" {
		t.Errorf("got %q, want %q", got, "https://example.com/page?a=1&z=3")
	}
}

func TestNormalize_LowercaseHost(t *testing.T) {
	got, err := Normalize("https://EXAMPLE.COM/path", "https://base.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "https://example.com/path" {
		t.Errorf("got %q, want %q", got, "https://example.com/path")
	}
}

func TestNormalize_RootPath(t *testing.T) {
	got, err := Normalize("https://example.com/", "https://base.com")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Root path "/" should not be trimmed
	if got != "https://example.com/" {
		t.Errorf("got %q, want %q", got, "https://example.com/")
	}
}
