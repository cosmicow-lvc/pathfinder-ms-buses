package config

import "testing"

func setValidEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv("APP_ENV", "test")
	t.Setenv("PORT", "8081")
	t.Setenv("POSTGRES_DSN", "postgres://localhost/test")
	t.Setenv("KAFKA_BROKERS", "localhost:9092, broker:9092")
	t.Setenv("KAFKA_TOPIC", "bus.events")
	t.Setenv("KAFKA_CONSUMER_GROUP", "test-group")
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
	if cfg.KafkaTopic != "bus.events" || cfg.KafkaConsumerGroup != "test-group" {
		t.Fatalf("unexpected Kafka configuration: topic=%q group=%q", cfg.KafkaTopic, cfg.KafkaConsumerGroup)
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
