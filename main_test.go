package main

import (
	"os"
	"testing"

	"github.com/spf13/viper"
)

func TestLoadConfig_EnvOverride(t *testing.T) {
	viper.Reset()
	t.Setenv("PARSER_KAFKA_BROKER", "test-kafka:9092")
	t.Setenv("PARSER_REDIS_ADDR", "test-redis:6379")
	t.Setenv("PARSER_WORKERS", "4")
	t.Setenv("PARSER_MAX_DEPTH", "5")
	defer viper.Reset()

	cfg := loadConfig()

	if cfg.kafkaBroker != "test-kafka:9092" {
		t.Errorf("kafkaBroker: got %q, want %q", cfg.kafkaBroker, "test-kafka:9092")
	}
	if cfg.redisAddr != "test-redis:6379" {
		t.Errorf("redisAddr: got %q, want %q", cfg.redisAddr, "test-redis:6379")
	}
	if cfg.numWorkers != 4 {
		t.Errorf("numWorkers: got %d, want 4", cfg.numWorkers)
	}
	if cfg.maxDepth != 5 {
		t.Errorf("maxDepth: got %d, want 5", cfg.maxDepth)
	}
}

func TestLoadConfig_ConfigFile(t *testing.T) {
	viper.Reset()
	defer viper.Reset()
	for _, key := range []string{
		"PARSER_KAFKA_BROKER", "PARSER_REDIS_ADDR", "PARSER_DSN",
		"PARSER_WORKERS", "PARSER_MAX_DEPTH",
	} {
		os.Unsetenv(key)
	}

	cfg := loadConfig()

	if cfg.kafkaBroker == "" {
		t.Error("kafkaBroker should not be empty when read from config.yml")
	}
	if cfg.numWorkers <= 0 {
		t.Errorf("numWorkers should be > 0, got %d", cfg.numWorkers)
	}
	if cfg.maxDepth <= 0 {
		t.Errorf("maxDepth should be > 0, got %d", cfg.maxDepth)
	}
}
