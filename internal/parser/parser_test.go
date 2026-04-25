package parser

import (
	"testing"
)

func TestParse_Title(t *testing.T) {
	body := []byte(`<html><head><title>Test Page</title></head><body></body></html>`)
	result := Parse(body)
	if result.Title != "Test Page" {
		t.Errorf("title: got %q, want %q", result.Title, "Test Page")
	}
}

func TestParse_TitleWhitespace(t *testing.T) {
	body := []byte(`<html><head><title>  Padded Title  </title></head></html>`)
	result := Parse(body)
	if result.Title != "Padded Title" {
		t.Errorf("title: got %q, want %q", result.Title, "Padded Title")
	}
}

func TestParse_Links(t *testing.T) {
	body := []byte(`<html><body>
		<a href="https://example.com">link1</a>
		<a href="/relative/path">link2</a>
	</body></html>`)
	result := Parse(body)
	if len(result.Links) != 2 {
		t.Errorf("links: got %d, want 2", len(result.Links))
	}
}

func TestParse_SkipsAnchors(t *testing.T) {
	body := []byte(`<html><body>
		<a href="https://example.com">link</a>
		<a href="#section">anchor</a>
	</body></html>`)
	result := Parse(body)
	if len(result.Links) != 1 {
		t.Errorf("links: got %d, want 1 (anchor should be skipped)", len(result.Links))
	}
}

func TestParse_SkipsEmptyHref(t *testing.T) {
	body := []byte(`<html><body>
		<a href="">empty</a>
		<a href="https://example.com">valid</a>
	</body></html>`)
	result := Parse(body)
	if len(result.Links) != 1 {
		t.Errorf("links: got %d, want 1 (empty href should be skipped)", len(result.Links))
	}
}

func TestParse_Empty(t *testing.T) {
	result := Parse([]byte{})
	if result.Title != "" {
		t.Errorf("expected empty title, got %q", result.Title)
	}
	if len(result.Links) != 0 {
		t.Errorf("expected no links, got %d", len(result.Links))
	}
}

func TestParse_NoTitle(t *testing.T) {
	body := []byte(`<html><body><a href="http://a.com">x</a></body></html>`)
	result := Parse(body)
	if result.Title != "" {
		t.Errorf("expected empty title, got %q", result.Title)
	}
	if len(result.Links) != 1 {
		t.Errorf("expected 1 link, got %d", len(result.Links))
	}
}

func TestParse_MultipleLinks(t *testing.T) {
	body := []byte(`<html><body>
		<a href="http://a.com">a</a>
		<a href="http://b.com">b</a>
		<a href="http://c.com">c</a>
	</body></html>`)
	result := Parse(body)
	if len(result.Links) != 3 {
		t.Errorf("links: got %d, want 3", len(result.Links))
	}
}
