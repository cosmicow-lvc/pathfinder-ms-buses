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
	Env                   string
	Port                  string
	PostgresDSN           string
	KafkaBrokers          []string
	KafkaTopic            string
	KafkaCommandsTopic    string
	KafkaBFFTopics        []string
	KafkaValidationTopic  string
	KafkaValidationTopics []string
	KafkaReplyTopics      []string
	KafkaConsumerGroup    string
	KafkaRoutesGroup      string
}

func Load() (Config, error) {
	if err := godotenv.Load(); err != nil && !errors.Is(err, os.ErrNotExist) {
		return Config{}, fmt.Errorf("load .env: %w", err)
	}

	cfg := Config{
		Env:                  envOrDefault("APP_ENV", "development"),
		Port:                 envOrDefault("PORT", "8080"),
		PostgresDSN:          strings.TrimSpace(os.Getenv("POSTGRES_DSN")),
		KafkaTopic:           envOrDefault("KAFKA_TOPIC", "bus.status.changed"),
		KafkaCommandsTopic:   envOrDefault("KAFKA_BFF_COMMANDS_TOPIC", "buses.commands.v1"),
		KafkaValidationTopic: envOrDefault("KAFKA_BUS_VALIDATION_TOPIC", "buses.validation.requests.v1"),
		KafkaConsumerGroup:   envOrDefault("KAFKA_CONSUMER_GROUP", "pathfinder-buses-bff-v1"),
		KafkaRoutesGroup:     envOrDefault("KAFKA_ROUTES_CONSUMER_GROUP", "pathfinder-buses-routes-v1"),
	}
	cfg.KafkaBFFTopics = []string{
		cfg.KafkaCommandsTopic,
		"buses.driver.list",
		"buses.driver.create",
		"buses.bus.list",
		"buses.bus.get",
		"buses.bus.create",
		"buses.bus.change-status",
	}

	if cfg.PostgresDSN == "" {
		return Config{}, errors.New("POSTGRES_DSN is required")
	}
	cfg.KafkaValidationTopics = []string{cfg.KafkaValidationTopic, "bus.validate"}
	if cfg.KafkaTopic == "" {
		return Config{}, errors.New("KAFKA_TOPIC is required")
	}

	replyTopics := envOrDefault("KAFKA_REPLY_TOPICS", "bff.replies.v1,routes.replies.v1,bus.validate.reply")
	for _, topic := range strings.Split(replyTopics, ",") {
		if topic = strings.TrimSpace(topic); topic != "" {
			cfg.KafkaReplyTopics = append(cfg.KafkaReplyTopics, topic)
		}
	}
	if len(cfg.KafkaReplyTopics) == 0 {
		return Config{}, errors.New("KAFKA_REPLY_TOPICS must contain at least one topic")
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
