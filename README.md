# crawler-parser

Consumes crawled HTML pages from the `crawled-urls` Kafka topic, parses each page to extract the title and outbound links, stores page metadata in MySQL, deduplicates newly found links via Redis, and republishes them to the `discovered-urls` topic to be fetched by `crawler-worker`.

## Architecture overview

```
[crawled-urls]  ──►  crawler-parser  ──►  MySQL (url_metadata)
                           │
                           ├──►  Redis (dedup: webcrawler:parsed_urls)
                           │
                           └──►  [discovered-urls]  (new links, depth + 1)
```

Multiple instances can run in parallel under the same Kafka consumer group (`crawler-parsers`). Kafka distributes `crawled-urls` partitions across all active instances automatically.

## Prerequisites

| Dependency | Minimum version | Notes |
|------------|-----------------|-------|
| Go         | 1.25            |       |
| Kafka      | 3.x             | Topics `crawled-urls` and `discovered-urls` must exist |
| Redis      | 6.x             | Used for link deduplication (`SADD` on `webcrawler:parsed_urls`) |
| MySQL      | 8.x             | Database `webcrawler` must exist with the schema below |

### MySQL schema

```sql
CREATE DATABASE IF NOT EXISTS webcrawler;

USE webcrawler;

CREATE TABLE IF NOT EXISTS url_metadata (
  url_hash       CHAR(64)     NOT NULL,
  canonical_url  TEXT         NOT NULL,
  host           VARCHAR(255) NOT NULL,
  status         ENUM('DISCOVERED', 'PARSED', 'FAILED') NOT NULL,
  http_status    SMALLINT     NOT NULL,
  last_crawled_at DATETIME,
  content_hash   CHAR(64)     NOT NULL,
  title          TEXT,
  PRIMARY KEY (url_hash)
);
```

## Configuration

Configuration is read from `config.yml` in the working directory. All keys can be overridden by environment variables with the `PARSER_` prefix (e.g. `PARSER_KAFKA_BROKER`).

| Key                | Default                                                | Env var                    | Description                                                        |
|--------------------|--------------------------------------------------------|----------------------------|--------------------------------------------------------------------|  
| `kafka_broker`     | `localhost:9092`                                       | `PARSER_KAFKA_BROKER`      | Kafka broker address                                               |
| `redis_addr`       | `localhost:6379`                                       | `PARSER_REDIS_ADDR`        | Redis address for link deduplication                               |
| `dsn`              | `root:@tcp(127.0.0.1:3306)/webcrawler?parseTime=true`  | `PARSER_DSN`               | MySQL Data Source Name                                             |
| `max_depth`        | `3`                                                    | `PARSER_MAX_DEPTH`         | Maximum crawl depth. Links found at this depth are not republished |
| `workers`          | `8`                                                    | `PARSER_WORKERS`           | Number of parallel parse goroutines                                |
| `topic_crawled`    | `crawled-urls`                                         | `PARSER_TOPIC_CRAWLED`     | Kafka topic to consume crawled pages from                          |
| `topic_discovered` | `discovered-urls`                                      | `PARSER_TOPIC_DISCOVERED`  | Kafka topic to publish newly discovered links to                   |

## Running

```bash
# Build
go build -o crawler-parser ./...

# Run with defaults (reads config.yml in the working directory)
./crawler-parser

# Override individual values with environment variables
PARSER_KAFKA_BROKER=broker:9092 \
PARSER_REDIS_ADDR=redis:6379 \
PARSER_DSN="user:pass@tcp(mysql:3306)/webcrawler?parseTime=true" \
PARSER_MAX_DEPTH=5 \
PARSER_WORKERS=16 \
./crawler-parser

# Use custom Kafka topics
PARSER_TOPIC_CRAWLED=my-pages \
PARSER_TOPIC_DISCOVERED=my-urls \
./crawler-parser

# Run multiple instances for higher throughput (same consumer group)
./crawler-parser &
./crawler-parser &
```

Shut down gracefully with `SIGINT` or `SIGTERM`. The parser commits Kafka offsets before exiting.

## What it does

For every `CrawledPage` event consumed from `crawled-urls`:

1. **Decompress** — Gunzips the `body` field. Falls back to treating it as raw HTML if decompression fails.
2. **Parse HTML** — Extracts the page `<title>` and all `<a href>` links using `golang.org/x/net/html`.
3. **Store metadata** — Upserts a row into MySQL `url_metadata` with the canonical URL, host, HTTP status, title, and a SHA-256 content hash.
4. **Depth check** — If `page.Depth >= max-depth`, stops here (no further link republishing).
5. **Canonicalize links** — Each extracted link is resolved against the page's final URL and normalized (scheme lowercased, trailing slashes normalized, common tracking query parameters like `utm_*`, `fbclid`, `gclid` removed).
6. **Deduplicate** — Attempts `SADD webcrawler:parsed_urls <canonical_url>` in Redis. Skips links already in the set.
7. **Republish** — Produces a `DiscoveredURL` event to `discovered-urls` for each new link:
   ```json
   {
     "url": "https://www.example.com/page",
     "depth": 1,
     "source_url": "https://www.example.com/",
     "enqueued_at": "2026-04-25T10:00:05Z"
   }
   ```
8. **Offset commit** — Commits Kafka consumer offsets after each poll batch.

## Internal packages

| Package                    | Responsibility                                                    |
|----------------------------|-------------------------------------------------------------------|
| `internal/parser`          | HTML parser — extracts `<title>` and `<a href>` links             |
| `internal/canonicalizer`   | URL normalizer — resolves relative URLs, strips tracking params   |
| `internal/store`           | MySQL `url_metadata` upsert, SHA-256 helpers                      |
| `internal/redisconn`       | Redis client factory with connectivity check                      |
| `internal/kafkaconn`       | Kafka consumer-producer client factory                            |

## Kafka topics

Topic names are configurable via `config.yml` or environment variables (see Configuration above).

| Config key         | Default           | Direction | Message type    |
|--------------------|-------------------|-----------|-----------------|
| `topic_crawled`    | `crawled-urls`    | Consume   | `CrawledPage`   |
| `topic_discovered` | `discovered-urls` | Produce   | `DiscoveredURL` |
