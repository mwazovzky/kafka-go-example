package config

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/spf13/viper"
)

func init() {
	// Configure viper to read environment variables
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	viper.SetDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
	viper.SetDefault("SCHEMA_REGISTRY_URL", "http://localhost:8081")
	viper.SetDefault("SCHEMA_REGISTRY_AUTO_REGISTER_SCHEMAS", false)
	viper.SetDefault("SCHEMA_REGISTRY_USE_LATEST_VERSION", true)
	viper.SetDefault("MYSQL_HOST", "localhost")
	viper.SetDefault("MYSQL_PORT", 3306)
	viper.SetDefault("MYSQL_USER", "user")
	viper.SetDefault("MYSQL_PASSWORD", "password")
	viper.SetDefault("MYSQL_DATABASE", "example")

	if _, err := os.Stat(".env"); err == nil {
		viper.SetConfigFile(".env")
		if err := viper.ReadInConfig(); err != nil {
			log.Printf("Failed to load .env file: %v", err)
		}
	}
}

type KafkaConfig struct {
	BootstrapServers string `mapstructure:"KAFKA_BOOTSTRAP_SERVERS"`
	Group            string `mapstructure:"KAFKA_CONSUMER_GROUP"`
}

type SchemaRegistryConfig struct {
	SchemaRegistryUrl      string `mapstructure:"SCHEMA_REGISTRY_URL"`
	SchemaRegistryUsername string `mapstructure:"SCHEMA_REGISTRY_USERNAME"`
	SchemaRegistryPassword string `mapstructure:"SCHEMA_REGISTRY_PASSWORD"`
	AutoRegisterSchemas    bool   `mapstructure:"SCHEMA_REGISTRY_AUTO_REGISTER_SCHEMAS"`
	UseLatestVersion       bool   `mapstructure:"SCHEMA_REGISTRY_USE_LATEST_VERSION"`
}

type DatabaseConfig struct {
	Host         string `mapstructure:"MYSQL_HOST"`
	Port         int    `mapstructure:"MYSQL_PORT"`
	RootPassword string `mapstructure:"MYSQL_ROOT_PASSWORD"`
	Database     string `mapstructure:"MYSQL_DATABASE"`
	User         string `mapstructure:"MYSQL_USER"`
	Password     string `mapstructure:"MYSQL_PASSWORD"`
}

type TaskConfig struct {
	Name      string        `yaml:"name" mapstructure:"name"`             // Explicitly map "name"
	QueryFile string        `yaml:"query_file" mapstructure:"query_file"` // Explicitly map "query"
	Topic     string        `yaml:"topic" mapstructure:"topic"`           // Explicitly map "topic"
	Schema    string        `yaml:"schema" mapstructure:"schema"`         // Explicitly map "schema"
	Interval  time.Duration `yaml:"interval" mapstructure:"interval"`     // Explicitly map "interval"
}

// LoadKafkaConfig loads KafkaConfig using viper.
func LoadKafkaConfig() KafkaConfig {
	var cfg KafkaConfig
	if err := viper.Unmarshal(&cfg); err != nil {
		log.Printf("Failed to parse KafkaConfig: %v", err)
	}
	return cfg
}

// LoadSchemaRegistryConfig loads SchemaRegistryConfig using viper.
func LoadSchemaRegistryConfig() SchemaRegistryConfig {
	var cfg SchemaRegistryConfig
	if err := viper.Unmarshal(&cfg); err != nil {
		log.Printf("Failed to parse SchemaRegistryConfig: %v", err)
	}
	return cfg
}

// LoadDatabaseConfig loads DatabaseConfig using viper.
func LoadDatabaseConfig() DatabaseConfig {
	var cfg DatabaseConfig
	if err := viper.Unmarshal(&cfg); err != nil {
		log.Printf("Failed to parse DatabaseConfig: %v", err)
	}
	return cfg
}

// LoadTaskConfigs loads task configurations from YAML files in the specified directory using viper.
func LoadTaskConfigs(configDir string) ([]TaskConfig, error) {
	var configs []TaskConfig

	v := viper.New()
	v.SetConfigType("yaml")

	// Iterate over YAML files in the directory
	files, err := listYAMLFiles(configDir)
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		v.SetConfigFile(file)

		// Read the YAML file
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("failed to read config file %s: %w", file, err)
		}

		// Unmarshal the configuration into TaskConfig
		var cfg TaskConfig
		if err := v.Unmarshal(&cfg); err != nil {
			return nil, fmt.Errorf("failed to unmarshal config file %s: %w", file, err)
		}

		configs = append(configs, cfg)
	}

	return configs, nil
}

// LoadSingleTaskConfig loads a single task configuration from a YAML file.
func LoadSingleTaskConfig(filePath string) (TaskConfig, error) {
	v := viper.New()
	v.SetConfigFile(filePath)
	v.SetConfigType("yaml")

	// Read the YAML file
	if err := v.ReadInConfig(); err != nil {
		return TaskConfig{}, fmt.Errorf("failed to read config file %s: %w", filePath, err)
	}

	// Unmarshal the configuration into TaskConfig
	var cfg TaskConfig
	if err := v.Unmarshal(&cfg); err != nil {
		return TaskConfig{}, fmt.Errorf("failed to unmarshal config file %s: %w", filePath, err)
	}

	return cfg, nil
}

// listYAMLFiles lists all YAML files in the specified directory.
func listYAMLFiles(dir string) ([]string, error) {
	var files []string

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".yaml") {
			files = append(files, fmt.Sprintf("%s/%s", dir, entry.Name()))
		}
	}

	return files, nil
}
