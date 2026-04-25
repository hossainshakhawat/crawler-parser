package store

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/url"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type Status string

const (
	StatusDiscovered Status = "DISCOVERED"
	StatusParsed     Status = "PARSED"
	StatusFailed     Status = "FAILED"
)

type URLMeta struct {
	URLHash      string
	CanonicalURL string
	Host         string
	Status       Status
	HTTPStatus   int
	LastCrawledAt time.Time
	ContentHash  string
	Title        string
}

type MetadataStore struct {
	db *sql.DB
}

func NewMetadataStore(dsn string) (*MetadataStore, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("metadata store: open: %w", err)
	}
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("metadata store: ping: %w", err)
	}
	return &MetadataStore{db: db}, nil
}

func (s *MetadataStore) Close() error { return s.db.Close() }

func (s *MetadataStore) Upsert(m *URLMeta) error {
	const q = `
INSERT INTO url_metadata
  (url_hash, canonical_url, host, status, http_status, last_crawled_at, content_hash, title)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
  status          = VALUES(status),
  http_status     = VALUES(http_status),
  last_crawled_at = VALUES(last_crawled_at),
  content_hash    = VALUES(content_hash),
  title           = VALUES(title)`

	var lastCrawled interface{}
	if !m.LastCrawledAt.IsZero() {
		lastCrawled = m.LastCrawledAt.UTC()
	}
	_, err := s.db.Exec(q,
		m.URLHash, m.CanonicalURL, m.Host,
		string(m.Status), m.HTTPStatus, lastCrawled,
		m.ContentHash, m.Title,
	)
	return err
}

func HashURL(u string) string {
	h := sha256.Sum256([]byte(u))
	return hex.EncodeToString(h[:])
}

func ContentHash(body []byte) string {
	h := sha256.Sum256(body)
	return hex.EncodeToString(h[:])
}

func HostOf(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return u.Hostname()
}
