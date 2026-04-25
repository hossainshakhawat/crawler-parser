// Command crawler-parser consumes crawled pages from the "crawled-urls" Kafka
// topic, parses HTML to extract links and titles, stores metadata to MySQL,
// and republishes newly discovered links to "discovered-urls" for the worker
// to fetch next.
//
// Configuration is loaded in this priority order (highest wins):
//
//  1. Environment variables  (prefix PARSER_, e.g. PARSER_KAFKA_BROKER)
//  2. config.yml             (must be in the working directory)
//  3. Built-in defaults
package main

import (
	"context"
	"encoding/json"
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
	kafkaBroker     string
	redisAddr       string
	dsn             string
	maxDepth        int
	numWorkers      int
	topicCrawled    string
	topicDiscovered string
}

func loadConfig() config {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")

	viper.SetEnvPrefix("PARSER")
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			log.Fatalf("config: %v", err)
		}
	}

	return config{
		kafkaBroker:     viper.GetString("kafka_broker"),
		redisAddr:       viper.GetString("redis_addr"),
		dsn:             viper.GetString("dsn"),
		maxDepth:        viper.GetInt("max_depth"),
		numWorkers:      viper.GetInt("workers"),
		topicCrawled:    viper.GetString("topic_crawled"),
		topicDiscovered: viper.GetString("topic_discovered"),
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

	kafkaClient, err := kafkaconn.New(cfg.kafkaBroker, consumerGroup, cfg.topicCrawled)
	if err != nil {
		log.Fatalf("kafka: %v", err)
	}
	defer kafkaClient.Close()

	log.Printf("crawler-parser started: group=%s max-depth=%d workers=%d",
		consumerGroup, cfg.maxDepth, cfg.numWorkers)

	run(ctx, kafkaClient, redisClient, metaStore, cfg.maxDepth, cfg.numWorkers, cfg.topicDiscovered)
}

func run(
	ctx context.Context,
	kafkaClient *kgo.Client,
	redisClient *redis.Client,
	metaStore *store.MetadataStore,
	maxDepth int,
	numWorkers int,
	topicDiscovered string,
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
				processPage(ctx, page, kafkaClient, redisClient, metaStore, maxDepth, topicDiscovered)
			}(page)
		})
		wg.Wait()

		if err := kafkaClient.CommitUncommittedOffsets(ctx); err != nil {
			log.Printf("commit offsets: %v", err)
		}
	}
}
