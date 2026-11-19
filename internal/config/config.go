package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

type Config struct {
	Env                string
	Port               string
	PostgresDSN        string
	KafkaBrokers       []string
	KafkaTopic         string
	KafkaConsumerGroup string
}

func Load() (Config, error) {
	if err := godotenv.Load(); err != nil && !errors.Is(err, os.ErrNotExist) {
		return Config{}, fmt.Errorf("load .env: %w", err)
	}

	cfg := Config{
		Env:                envOrDefault("APP_ENV", "development"),
		Port:               envOrDefault("PORT", "8080"),
		PostgresDSN:        strings.TrimSpace(os.Getenv("POSTGRES_DSN")),
		KafkaTopic:         strings.TrimSpace(os.Getenv("KAFKA_TOPIC")),
		KafkaConsumerGroup: envOrDefault("KAFKA_CONSUMER_GROUP", "pathfinder-ms-buses"),
	}

	if cfg.PostgresDSN == "" {
		return Config{}, errors.New("POSTGRES_DSN is required")
	}
	if cfg.KafkaTopic == "" {
		return Config{}, errors.New("KAFKA_TOPIC is required")
	}

	port, err := strconv.Atoi(cfg.Port)
	if err != nil || port < 1 || port > 65535 {
		return Config{}, fmt.Errorf("PORT must be a number between 1 and 65535: %q", cfg.Port)
	}

	for _, broker := range strings.Split(os.Getenv("KAFKA_BROKERS"), ",") {
		if broker = strings.TrimSpace(broker); broker != "" {
			cfg.KafkaBrokers = append(cfg.KafkaBrokers, broker)
		}
	}
	if len(cfg.KafkaBrokers) == 0 {
		return Config{}, errors.New("KAFKA_BROKERS must contain at least one broker")
	}

	return cfg, nil
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
