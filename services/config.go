package services

import (
	"log"

	"github.com/caarlos0/env/v6"
	"github.com/joho/godotenv"
)

func init() {
	if err := godotenv.Load(); err != nil {
		log.Printf("No .env file found or error loading .env file: %v", err)
	}
}

type KafkaConfig struct {
	BootstrapServers string `env:"KAFKA_BOOTSTRAP_SERVERS" envDefault:"localhost:9092"`
	Topic            string `env:"KAFKA_TOPIC"`
	Group            string `env:"KAFKA_CONSUMER_GROUP"`
}

type SchemaRegistryConfig struct {
	SchemaRegistryUrl      string `env:"SCHEMA_REGISTRY_URL" envDefault:"http://localhost:8081"`
	SchemaRegistryUsername string `env:"SCHEMA_REGISTRY_USERNAME" envDefault:""`
	SchemaRegistryPassword string `env:"SCHEMA_REGISTRY_PASSWORD" envDefault:""`
	AutoRegisterSchemas    bool   `env:"SCHEMA_REGISTRY_AUTO_REGISTER_SCHEMAS" envDefault:"false"`
	UseLatestVersion       bool   `env:"SCHEMA_REGISTRY_USE_LATEST_VERSION" envDefault:"true"`
	Schema                 string `env:"SCHEMA_REGISTRY_SCHEMA"`
}

// LoadKafkaConfig loads KafkaConfig using github.com/caarlos0/env/v6.
func LoadKafkaConfig() KafkaConfig {
	var cfg KafkaConfig
	if err := env.Parse(&cfg); err != nil {
		log.Printf("Failed to parse KafkaConfig from env: %v", err)
	}
	return cfg
}

// LoadSchemaRegistryConfig loads SchemaRegistryConfig using github.com/caarlos0/env/v6.
func LoadSchemaRegistryConfig() SchemaRegistryConfig {
	var cfg SchemaRegistryConfig
	if err := env.Parse(&cfg); err != nil {
		log.Printf("Failed to parse SchemaRegistryConfig from env: %v", err)
	}
	return cfg
}
