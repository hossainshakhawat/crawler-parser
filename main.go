// Command crawler-parser consumes crawled pages from the "crawled-urls" Kafka
// topic, parses HTML to extract links and titles, stores metadata to MySQL,
// and republishes newly discovered links to "discovered-urls" for the worker
// to fetch next.
//
// Configuration is loaded in this priority order (highest wins):
//
//  1. CLI flags
//  2. Environment variables  (prefix PARSER_, e.g. PARSER_KAFKA_BROKER)
//  3. config.yml             (must be in the working directory)
//  4. Built-in defaults
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
	"github.com/spf13/viper"
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

func loadConfig() config {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")

	viper.SetDefault("kafka_broker", "localhost:9092")
	viper.SetDefault("redis_addr", "localhost:6379")
	viper.SetDefault("dsn", "root:@tcp(127.0.0.1:3306)/webcrawler?parseTime=true")
	viper.SetDefault("max_depth", 3)
	viper.SetDefault("workers", 8)

	viper.SetEnvPrefix("PARSER")
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			log.Fatalf("config: %v", err)
		}
	}

	// Flags override env vars and config.yml; defaults come from Viper
	// so env vars and config.yml flow through when flags are not set.
	kafka := flag.String("kafka", viper.GetString("kafka_broker"), "Kafka broker address")
	redisAddr := flag.String("redis", viper.GetString("redis_addr"), "Redis address for URL dedup")
	dsn := flag.String("dsn", viper.GetString("dsn"), "MySQL DSN")
	maxDepth := flag.Int("max-depth", viper.GetInt("max_depth"), "Maximum crawl depth")
	workers := flag.Int("workers", viper.GetInt("workers"), "Parallel parse goroutines")
	flag.Parse()

	return config{
		kafkaBroker: *kafka,
		redisAddr:   *redisAddr,
		dsn:         *dsn,
		maxDepth:    *maxDepth,
		numWorkers:  *workers,
	}
}

func listenForShutdown(cancel context.CancelFunc) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sigs; log.Println("shutting down"); cancel() }()
}

func main() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	cfg := loadConfig()

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
