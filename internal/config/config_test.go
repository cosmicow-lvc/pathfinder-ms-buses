package config

import "testing"

func setValidEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv("APP_ENV", "test")
	t.Setenv("PORT", "8081")
	t.Setenv("POSTGRES_DSN", "postgres://localhost/test")
	t.Setenv("KAFKA_BROKERS", "localhost:9092, broker:9092")
	t.Setenv("KAFKA_TOPIC", "buses.events.v1")
	t.Setenv("KAFKA_BFF_COMMANDS_TOPIC", "buses.commands.v1")
	t.Setenv("KAFKA_BUS_VALIDATION_TOPIC", "buses.validation.requests.v1")
	t.Setenv("KAFKA_REPLY_TOPICS", "bff.replies.v1, routes.replies.v1")
	t.Setenv("KAFKA_CONSUMER_GROUP", "test-group")
	t.Setenv("KAFKA_ROUTES_CONSUMER_GROUP", "routes-test-group")
}

func TestLoadParsesConfiguration(t *testing.T) {
	setValidEnvironment(t)

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}

	if cfg.Port != "8081" || cfg.Env != "test" {
		t.Fatalf("unexpected server configuration: env=%q port=%q", cfg.Env, cfg.Port)
	}
	if len(cfg.KafkaBrokers) != 2 || cfg.KafkaBrokers[1] != "broker:9092" {
		t.Fatalf("unexpected Kafka brokers: %#v", cfg.KafkaBrokers)
	}
	if cfg.KafkaTopic != "buses.events.v1" || cfg.KafkaConsumerGroup != "test-group" {
		t.Fatalf("unexpected Kafka configuration: topic=%q group=%q", cfg.KafkaTopic, cfg.KafkaConsumerGroup)
	}
	if cfg.KafkaCommandsTopic != "buses.commands.v1" {
		t.Fatalf("unexpected Kafka commands topic: %q", cfg.KafkaCommandsTopic)
	}
	if cfg.KafkaValidationTopic != "buses.validation.requests.v1" || cfg.KafkaRoutesGroup != "routes-test-group" {
		t.Fatalf("unexpected routes Kafka configuration: topic=%q group=%q", cfg.KafkaValidationTopic, cfg.KafkaRoutesGroup)
	}
	if len(cfg.KafkaReplyTopics) != 2 || cfg.KafkaReplyTopics[1] != "routes.replies.v1" {
		t.Fatalf("unexpected Kafka reply topics: %#v", cfg.KafkaReplyTopics)
	}
}

func TestLoadRejectsMissingPostgresDSN(t *testing.T) {
	setValidEnvironment(t)
	t.Setenv("POSTGRES_DSN", "")

	if _, err := Load(); err == nil {
		t.Fatal("expected an error when POSTGRES_DSN is missing")
	}
}

func TestLoadRejectsInvalidPort(t *testing.T) {
	setValidEnvironment(t)
	t.Setenv("PORT", "70000")

	if _, err := Load(); err == nil {
		t.Fatal("expected an error when PORT is out of range")
	}
}
