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
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/hossainshakhawat/crawler-parser/events"
	"github.com/hossainshakhawat/crawler-parser/internal/kafkaconn"
	"github.com/hossainshakhawat/crawler-parser/internal/redisconn"
	"github.com/hossainshakhawat/crawler-parser/internal/store"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	consumerGroup = "crawler-parsers"
	redisSeenKey  = "webcrawler:parsed_urls"
)

type config struct {
	kafkaBroker string
	redisAddr   string
	dsn         string
	maxDepth    int
	numWorkers  int
}

func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.kafkaBroker, "kafka", "localhost:9092", "Kafka broker address")
	flag.StringVar(&cfg.redisAddr, "redis", "localhost:6379", "Redis address for URL dedup")
	flag.StringVar(&cfg.dsn, "dsn", "root:@tcp(127.0.0.1:3306)/webcrawler?parseTime=true", "MySQL DSN")
	flag.IntVar(&cfg.maxDepth, "max-depth", 3, "Maximum crawl depth")
	flag.IntVar(&cfg.numWorkers, "workers", 8, "Parallel parse goroutines")
	flag.Parse()
	return cfg
}

func listenForShutdown(cancel context.CancelFunc) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sigs; log.Println("shutting down"); cancel() }()
}

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	cfg := parseFlags()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	listenForShutdown(cancel)

	redisClient, err := redisconn.New(ctx, cfg.redisAddr)
	if err != nil {
		log.Fatalf("redis: %v", err)
	}
	defer redisClient.Close()

	metaStore, err := store.NewMetadataStore(cfg.dsn)
	if err != nil {
		log.Fatalf("metadata store: %v", err)
	}
	defer metaStore.Close()

	kafkaClient, err := kafkaconn.New(cfg.kafkaBroker, consumerGroup)
	if err != nil {
		log.Fatalf("kafka: %v", err)
	}
	defer kafkaClient.Close()

	log.Printf("crawler-parser started: group=%s max-depth=%d workers=%d",
		consumerGroup, cfg.maxDepth, cfg.numWorkers)

	run(ctx, kafkaClient, redisClient, metaStore, cfg.maxDepth, cfg.numWorkers)
}

func run(
	ctx context.Context,
	kafkaClient *kgo.Client,
	redisClient *redis.Client,
	metaStore *store.MetadataStore,
	maxDepth int,
	numWorkers int,
) {
	semaphore := make(chan struct{}, numWorkers)

	for {
		fetches := kafkaClient.PollFetches(ctx)
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
		fetches.EachRecord(func(record *kgo.Record) {
			var page events.CrawledPage
			if err := json.Unmarshal(record.Value, &page); err != nil {
				log.Printf("unmarshal: %v", err)
				return
			}
			semaphore <- struct{}{}
			wg.Add(1)
			go func(page events.CrawledPage) {
				defer func() { <-semaphore; wg.Done() }()
				processPage(ctx, page, kafkaClient, redisClient, metaStore, maxDepth)
			}(page)
		})
		wg.Wait()

		if err := kafkaClient.CommitUncommittedOffsets(ctx); err != nil {
			log.Printf("commit offsets: %v", err)
		}
	}
}
