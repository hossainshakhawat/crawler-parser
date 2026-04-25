package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"log"
	"time"

	"github.com/hossainshakhawat/crawler-parser/events"
	"github.com/hossainshakhawat/crawler-parser/internal/canonicalizer"
	"github.com/hossainshakhawat/crawler-parser/internal/parser"
	"github.com/hossainshakhawat/crawler-parser/internal/store"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
)

func processPage(
	ctx context.Context,
	page events.CrawledPage,
	kafkaClient *kgo.Client,
	redisClient *redis.Client,
	metaStore *store.MetadataStore,
	maxDepth int,
	topicDiscovered string,
) {
	// ① Decompress body
	body, err := gzipDecompress(page.Body)
	if err != nil {
		// Fall back to treating body as raw HTML
		body = page.Body
	}

	// ② Parse HTML
	parsed := parser.Parse(body)

	// ③ Store metadata in MySQL
	urlMeta := &store.URLMeta{
		URLHash:       store.HashURL(page.FinalURL),
		CanonicalURL:  page.FinalURL,
		Host:          store.HostOf(page.FinalURL),
		Status:        store.StatusParsed,
		HTTPStatus:    page.HTTPStatus,
		LastCrawledAt: page.CrawledAt,
		ContentHash:   store.ContentHash(body),
		Title:         parsed.Title,
	}
	if err := metaStore.Upsert(urlMeta); err != nil {
		log.Printf("[store err] %s — %v", page.FinalURL, err)
	}

	log.Printf("[parsed] depth=%-2d  %-55s  %q  links=%d",
		page.Depth, page.FinalURL, parsed.Title, len(parsed.Links))

	// ④ If we're at max depth, don't publish more links
	if page.Depth >= maxDepth {
		return
	}

	// ⑤ Canonicalize, dedup, and republish each link to discovered-urls
	for _, link := range parsed.Links {
		norm, err := canonicalizer.Normalize(link, page.FinalURL)
		if err != nil || norm == "" {
			continue
		}

		// Redis SADD: returns 1 if new, 0 if already in the set
		added, err := redisClient.SAdd(ctx, redisSeenKey, norm).Result()
		if err != nil || added == 0 {
			continue // already seen
		}

		event := events.DiscoveredURL{
			URL:        norm,
			Depth:      page.Depth + 1,
			SourceURL:  page.FinalURL,
			EnqueuedAt: time.Now().UTC(),
		}
		payload, err := json.Marshal(event)
		if err != nil {
			continue
		}
		kafkaRecord := &kgo.Record{
			Topic: topicDiscovered,
			Key:   []byte(norm),
			Value: payload,
		}
		if err := kafkaClient.ProduceSync(ctx, kafkaRecord).FirstErr(); err != nil {
			if ctx.Err() == nil {
				log.Printf("[kafka] publish %s: %v", norm, err)
			}
		}
	}
}

func gzipDecompress(data []byte) ([]byte, error) {
	gzipReader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer gzipReader.Close()
	return io.ReadAll(gzipReader)
}
