// Command crawler-parser consumes crawled pages from the "crawled-urls" Kafka
// topic, parses HTML to extract links and titles, stores metadata to MySQL,
// and republishes newly discovered links to "discovered-urls" for the worker
// to fetch next.
//
// Usage:
//
//	crawler-parser [flags]
//
// Flags:
//
//	-kafka      Kafka broker address (default localhost:9092)
//	-redis      Redis address for dedup (default localhost:6379)
//	-dsn        MySQL DSN (default root:@tcp(127.0.0.1:3306)/webcrawler?parseTime=true)
//	-max-depth  Maximum crawl depth; links at this depth are not republished (default 3)
//	-workers    Parallel parse goroutines (default 8)
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"io"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/shakhawathossain/crawler-parser/events"
	"github.com/shakhawathossain/crawler-parser/internal/canonicalizer"
	"github.com/shakhawathossain/crawler-parser/internal/parser"
	"github.com/shakhawathossain/crawler-parser/internal/store"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	consumerGroup = "crawler-parsers"
	redisSeenKey  = "webcrawler:seen_urls"
)

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	kafkaBroker := flag.String("kafka", "localhost:9092", "Kafka broker address")
	redisAddr   := flag.String("redis", "localhost:6379", "Redis address for URL dedup")
	dsn         := flag.String("dsn", "root:@tcp(127.0.0.1:3306)/webcrawler?parseTime=true", "MySQL DSN")
	maxDepth    := flag.Int("max-depth", 3, "Maximum crawl depth")
	numWorkers  := flag.Int("workers", 8, "Parallel parse goroutines")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sigs; log.Println("shutting down"); cancel() }()

	// ── Redis for dedup ───────────────────────────────────────────────────────
	rdb := redis.NewClient(&redis.Options{Addr: *redisAddr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("redis ping: %v", err)
	}
	defer rdb.Close()

	// ── MySQL metadata store ──────────────────────────────────────────────────
	meta, err := store.NewMetadataStore(*dsn)
	if err != nil {
		log.Fatalf("metadata store: %v", err)
	}
	defer meta.Close()

	// ── Kafka consumer + producer ─────────────────────────────────────────────
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(*kafkaBroker),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(events.TopicCrawled),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
	)
	if err != nil {
		log.Fatalf("kafka client: %v", err)
	}
	defer cl.Close()

	sem := make(chan struct{}, *numWorkers)

	log.Printf("crawler-parser started: group=%s max-depth=%d workers=%d",
		consumerGroup, *maxDepth, *numWorkers)

	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			break
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				log.Printf("fetch error: %v", e.Err)
			}
			continue
		}

		var wg sync.WaitGroup
		fetches.EachRecord(func(r *kgo.Record) {
			var page events.CrawledPage
			if err := json.Unmarshal(r.Value, &page); err != nil {
				log.Printf("unmarshal: %v", err)
				return
			}
			sem <- struct{}{}
			wg.Add(1)
			go func(page events.CrawledPage) {
				defer func() { <-sem; wg.Done() }()
				processPage(ctx, page, cl, rdb, meta, *maxDepth)
			}(page)
		})
		wg.Wait()

		if err := cl.CommitUncommittedOffsets(ctx); err != nil {
			log.Printf("commit offsets: %v", err)
		}
	}
}

func processPage(
	ctx context.Context,
	page events.CrawledPage,
	cl *kgo.Client,
	rdb *redis.Client,
	meta *store.MetadataStore,
	maxDepth int,
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
	urlHash := store.HashURL(page.FinalURL)
	m := &store.URLMeta{
		URLHash:       urlHash,
		CanonicalURL:  page.FinalURL,
		Host:          store.HostOf(page.FinalURL),
		Status:        store.StatusParsed,
		HTTPStatus:    page.HTTPStatus,
		LastCrawledAt: page.CrawledAt,
		ContentHash:   store.ContentHash(body),
		Title:         parsed.Title,
	}
	if err := meta.Upsert(m); err != nil {
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
		added, err := rdb.SAdd(ctx, redisSeenKey, norm).Result()
		if err != nil || added == 0 {
			continue // already seen
		}

		ev := events.DiscoveredURL{
			URL:        norm,
			Depth:      page.Depth + 1,
			SourceURL:  page.FinalURL,
			EnqueuedAt: time.Now().UTC(),
		}
		val, err := json.Marshal(ev)
		if err != nil {
			continue
		}
		rec := &kgo.Record{
			Topic: events.TopicDiscovered,
			Key:   []byte(norm),
			Value: val,
		}
		if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
			if ctx.Err() == nil {
				log.Printf("[kafka] publish %s: %v", norm, err)
			}
		}
	}
}

func gzipDecompress(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}
