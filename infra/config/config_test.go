package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

// resetViperForTest resets viper to a clean state and properly initializes it for testing
func resetViperForTest() {
	// Create a brand new viper instance
	viper.Reset()

	// Set default values (same as in init function)
	viper.SetDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
	viper.SetDefault("KAFKA_CONSUMER_GROUP", "") // Add this default
	viper.SetDefault("SCHEMA_REGISTRY_URL", "http://localhost:8081")
	viper.SetDefault("SCHEMA_REGISTRY_USERNAME", "") // Add this default
	viper.SetDefault("SCHEMA_REGISTRY_PASSWORD", "") // Add this default
	viper.SetDefault("SCHEMA_REGISTRY_AUTO_REGISTER_SCHEMAS", false)
	viper.SetDefault("SCHEMA_REGISTRY_USE_LATEST_VERSION", true)
	viper.SetDefault("MYSQL_HOST", "localhost")
	viper.SetDefault("MYSQL_PORT", 3306)
	viper.SetDefault("MYSQL_ROOT_PASSWORD", "") // Add this default
	viper.SetDefault("MYSQL_DATABASE", "example")
	viper.SetDefault("MYSQL_USER", "user")
	viper.SetDefault("MYSQL_PASSWORD", "password")

	// Configure viper to read environment variables
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
}

func TestLoadKafkaConfig(t *testing.T) {
	// Arrange
	os.Setenv("KAFKA_BOOTSTRAP_SERVERS", "test-server:9092")
	os.Setenv("KAFKA_CONSUMER_GROUP", "test-group")
	resetViperForTest()

	// Act
	cfg := LoadKafkaConfig()

	// Assert
	assert.Equal(t, "test-server:9092", cfg.BootstrapServers)
	assert.Equal(t, "test-group", cfg.Group)
}

// TestLoadSchemaRegistryConfig tests loading schema registry config from environment
func TestLoadSchemaRegistryConfig(t *testing.T) {
	// Arrange
	os.Setenv("SCHEMA_REGISTRY_URL", "http://test-schema:8081")
	os.Setenv("SCHEMA_REGISTRY_USERNAME", "testuser")
	os.Setenv("SCHEMA_REGISTRY_PASSWORD", "testpass")
	os.Setenv("SCHEMA_REGISTRY_AUTO_REGISTER_SCHEMAS", "true")
	os.Setenv("SCHEMA_REGISTRY_USE_LATEST_VERSION", "false")
	resetViperForTest()

	// Act
	cfg := LoadSchemaRegistryConfig()

	// Assert
	assert.Equal(t, "http://test-schema:8081", cfg.SchemaRegistryUrl)
	assert.Equal(t, "testuser", cfg.SchemaRegistryUsername)
	assert.Equal(t, "testpass", cfg.SchemaRegistryPassword)
	assert.True(t, cfg.AutoRegisterSchemas)
	assert.False(t, cfg.UseLatestVersion)
}

// TestLoadDatabaseConfig tests loading database configuration from environment
func TestLoadDatabaseConfig(t *testing.T) {
	// Arrange
	os.Setenv("MYSQL_HOST", "test-db")
	os.Setenv("MYSQL_PORT", "3307")
	os.Setenv("MYSQL_ROOT_PASSWORD", "root-pass")
	os.Setenv("MYSQL_DATABASE", "test-db")
	os.Setenv("MYSQL_USER", "test-user")
	os.Setenv("MYSQL_PASSWORD", "test-pass")
	resetViperForTest()

	// Act
	cfg := LoadDatabaseConfig()

	// Assert
	assert.Equal(t, "test-db", cfg.Host)
	assert.Equal(t, 3307, cfg.Port)
	assert.Equal(t, "root-pass", cfg.RootPassword)
	assert.Equal(t, "test-db", cfg.Database)
	assert.Equal(t, "test-user", cfg.User)
	assert.Equal(t, "test-pass", cfg.Password)
}

func TestLoadTaskConfigs(t *testing.T) {
	// Arrange
	// Create temporary directory with test YAML files
	tempDir, err := os.MkdirTemp("", "config-test-*")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create first test config file
	task1Content := []byte(`name: "task1"
query_file: "queries/task1.sql"
topic: "task1-topic"
schema: "task1-schema"
interval: "30s"`)

	// Create second test config file
	task2Content := []byte(`name: "task2"
query_file: "queries/task2.sql"
topic: "task2-topic"
schema: "task2-schema"
interval: "1m"`)

	err = os.WriteFile(filepath.Join(tempDir, "task1.yaml"), task1Content, 0644)
	assert.NoError(t, err)

	err = os.WriteFile(filepath.Join(tempDir, "task2.yaml"), task2Content, 0644)
	assert.NoError(t, err)

	// Also create a non-YAML file that should be ignored
	err = os.WriteFile(filepath.Join(tempDir, "ignored.txt"), []byte("ignored"), 0644)
	assert.NoError(t, err)

	// Act
	configs, err := LoadTaskConfigs(tempDir)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, configs, 2)

	// Maps for easier assertion regardless of order
	configMap := make(map[string]TaskConfig)
	for _, cfg := range configs {
		configMap[cfg.Name] = cfg
	}

	// Assert task1
	task1, ok := configMap["task1"]
	assert.True(t, ok)
	assert.Equal(t, "queries/task1.sql", task1.QueryFile)
	assert.Equal(t, "task1-topic", task1.Topic)
	assert.Equal(t, "task1-schema", task1.Schema)
	assert.Equal(t, 30*time.Second, task1.Interval)

	// Assert task2
	task2, ok := configMap["task2"]
	assert.True(t, ok)
	assert.Equal(t, "queries/task2.sql", task2.QueryFile)
	assert.Equal(t, "task2-topic", task2.Topic)
	assert.Equal(t, "task2-schema", task2.Schema)
	assert.Equal(t, time.Minute, task2.Interval)
}

func TestLoadTaskConfigs_DirectoryNotFound(t *testing.T) {
	// Act
	configs, err := LoadTaskConfigs("non-existent-directory")

	// Assert
	assert.Error(t, err)
	assert.Nil(t, configs)
}

func TestLoadSingleTaskConfig(t *testing.T) {
	// Arrange
	// Create temporary file with test YAML content
	tmpFile, err := os.CreateTemp("", "test-task-*.yaml")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	yamlContent := []byte(`name: "single-task"
query_file: "queries/single.sql"
topic: "single-topic"
schema: "single-schema"
interval: "5m"`)

	err = os.WriteFile(tmpFile.Name(), yamlContent, 0644)
	assert.NoError(t, err)

	// Act
	cfg, err := LoadSingleTaskConfig(tmpFile.Name())

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, "single-task", cfg.Name)
	assert.Equal(t, "queries/single.sql", cfg.QueryFile)
	assert.Equal(t, "single-topic", cfg.Topic)
	assert.Equal(t, "single-schema", cfg.Schema)
	assert.Equal(t, 5*time.Minute, cfg.Interval)
}

func TestLoadSingleTaskConfig_FileNotFound(t *testing.T) {
	// Act
	cfg, err := LoadSingleTaskConfig("non-existent-file.yaml")

	// Assert
	assert.Error(t, err)
	assert.Equal(t, TaskConfig{}, cfg)
}

func TestLoadSingleTaskConfig_InvalidYAML(t *testing.T) {
	// Arrange
	// Create temporary file with invalid YAML content
	tmpFile, err := os.CreateTemp("", "invalid-yaml-*.yaml")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	// Malformed YAML (missing colon after name)
	yamlContent := []byte(`name "invalid"
query_file: "queries/invalid.sql"`)

	err = os.WriteFile(tmpFile.Name(), yamlContent, 0644)
	assert.NoError(t, err)

	// Act
	cfg, err := LoadSingleTaskConfig(tmpFile.Name())

	// Assert
	assert.Error(t, err)
	assert.Equal(t, TaskConfig{}, cfg)
}

func TestListYAMLFiles(t *testing.T) {
	// Arrange
	// Create temporary directory with YAML and non-YAML files
	tempDir, err := os.MkdirTemp("", "yaml-test-*")
	assert.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create files
	yamlFiles := []string{"file1.yaml", "file2.yaml"}
	nonYamlFiles := []string{"file.txt", "file.json"}

	for _, f := range yamlFiles {
		err := os.WriteFile(filepath.Join(tempDir, f), []byte{}, 0644)
		assert.NoError(t, err)
	}

	for _, f := range nonYamlFiles {
		err := os.WriteFile(filepath.Join(tempDir, f), []byte{}, 0644)
		assert.NoError(t, err)
	}

	// Create a subdirectory that should be ignored
	err = os.Mkdir(filepath.Join(tempDir, "subdir"), 0755)
	assert.NoError(t, err)
	err = os.WriteFile(filepath.Join(tempDir, "subdir", "nested.yaml"), []byte{}, 0644)
	assert.NoError(t, err)

	// Act
	files, err := listYAMLFiles(tempDir)

	// Assert
	assert.NoError(t, err)
	assert.Len(t, files, 2)

	// Check file paths - they should include the directory
	for _, f := range files {
		assert.Contains(t, f, tempDir)
		assert.True(t, filepath.Base(f) == "file1.yaml" || filepath.Base(f) == "file2.yaml")
	}
}

func TestListYAMLFiles_DirectoryNotFound(t *testing.T) {
	// Act
	files, err := listYAMLFiles("non-existent-directory")

	// Assert
	assert.Error(t, err)
	assert.Nil(t, files)
}

// TestLoadConfigWithDefaults tests that default values are used when no environment variables are set
func TestLoadConfigWithDefaults(t *testing.T) {
	// Arrange
	os.Unsetenv("KAFKA_BOOTSTRAP_SERVERS")
	os.Unsetenv("KAFKA_CONSUMER_GROUP")
	os.Unsetenv("SCHEMA_REGISTRY_URL")
	os.Unsetenv("SCHEMA_REGISTRY_AUTO_REGISTER_SCHEMAS")
	os.Unsetenv("SCHEMA_REGISTRY_USE_LATEST_VERSION")
	os.Unsetenv("MYSQL_HOST")
	os.Unsetenv("MYSQL_PORT")
	os.Unsetenv("MYSQL_DATABASE")
	os.Unsetenv("MYSQL_USER")
	os.Unsetenv("MYSQL_PASSWORD")
	resetViperForTest()

	// Act
	kafkaConfig := LoadKafkaConfig()
	schemaConfig := LoadSchemaRegistryConfig()
	dbConfig := LoadDatabaseConfig()

	// Assert - check default values
	assert.Equal(t, "localhost:9092", kafkaConfig.BootstrapServers)
	assert.Equal(t, "", kafkaConfig.Group) // Default is empty string

	assert.Equal(t, "http://localhost:8081", schemaConfig.SchemaRegistryUrl)
	assert.False(t, schemaConfig.AutoRegisterSchemas)
	assert.True(t, schemaConfig.UseLatestVersion)

	assert.Equal(t, "localhost", dbConfig.Host)
	assert.Equal(t, 3306, dbConfig.Port)
	assert.Equal(t, "example", dbConfig.Database)
	assert.Equal(t, "user", dbConfig.User)
	assert.Equal(t, "password", dbConfig.Password)
}

// TestLoadConfigFromEnvFile tests loading configuration from a .env file
func TestLoadConfigFromEnvFile(t *testing.T) {
	// Arrange
	// Create a temporary .env file
	tmpDir, err := os.MkdirTemp("", "env-test-*")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Save current working directory and change to temp dir
	currentDir, err := os.Getwd()
	assert.NoError(t, err)
	err = os.Chdir(tmpDir)
	assert.NoError(t, err)
	defer os.Chdir(currentDir)

	// Create .env file with test values
	envContent := []byte(`
KAFKA_BOOTSTRAP_SERVERS=envfile:9092
KAFKA_CONSUMER_GROUP=env-consumer-group
SCHEMA_REGISTRY_URL=http://envfile-schema:8081
SCHEMA_REGISTRY_USERNAME=env-user
SCHEMA_REGISTRY_PASSWORD=env-pass
MYSQL_HOST=envfile-db
MYSQL_PORT=3308
MYSQL_DATABASE=env-db
`)
	err = os.WriteFile(".env", envContent, 0644)
	assert.NoError(t, err)

	// Clear any environment variables that could interfere
	os.Unsetenv("KAFKA_BOOTSTRAP_SERVERS")
	os.Unsetenv("KAFKA_CONSUMER_GROUP")
	os.Unsetenv("SCHEMA_REGISTRY_URL")
	os.Unsetenv("SCHEMA_REGISTRY_USERNAME")
	os.Unsetenv("SCHEMA_REGISTRY_PASSWORD")
	os.Unsetenv("MYSQL_HOST")
	os.Unsetenv("MYSQL_PORT")
	os.Unsetenv("MYSQL_DATABASE")

	// Reset viper to pick up the .env file
	viper.Reset()
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.SetConfigFile(".env")
	err = viper.ReadInConfig()
	assert.NoError(t, err)

	// Act
	kafkaConfig := LoadKafkaConfig()
	schemaConfig := LoadSchemaRegistryConfig()
	dbConfig := LoadDatabaseConfig()

	// Assert - values from .env file should be used
	assert.Equal(t, "envfile:9092", kafkaConfig.BootstrapServers)
	assert.Equal(t, "env-consumer-group", kafkaConfig.Group)

	assert.Equal(t, "http://envfile-schema:8081", schemaConfig.SchemaRegistryUrl)
	assert.Equal(t, "env-user", schemaConfig.SchemaRegistryUsername)
	assert.Equal(t, "env-pass", schemaConfig.SchemaRegistryPassword)

	assert.Equal(t, "envfile-db", dbConfig.Host)
	assert.Equal(t, 3308, dbConfig.Port)
	assert.Equal(t, "env-db", dbConfig.Database)
}

func TestEnvVarPrecedenceOverEnvFile(t *testing.T) {
	// Arrange
	// Create a temporary .env file
	tmpDir, err := os.MkdirTemp("", "env-precedence-*")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Save current working directory and change to temp dir
	currentDir, err := os.Getwd()
	assert.NoError(t, err)
	err = os.Chdir(tmpDir)
	assert.NoError(t, err)
	defer os.Chdir(currentDir)

	// Create .env file with values
	envContent := []byte(`
KAFKA_BOOTSTRAP_SERVERS=envfile:9092
MYSQL_HOST=envfile-db
`)
	err = os.WriteFile(".env", envContent, 0644)
	assert.NoError(t, err)

	// Set environment variables that should override .env file
	os.Setenv("KAFKA_BOOTSTRAP_SERVERS", "env-var:9092")
	os.Setenv("MYSQL_HOST", "env-var-db")
	defer func() {
		os.Unsetenv("KAFKA_BOOTSTRAP_SERVERS")
		os.Unsetenv("MYSQL_HOST")
	}()

	// Reset viper to pick up both .env file and environment variables
	viper.Reset()
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.SetConfigFile(".env")
	err = viper.ReadInConfig()
	assert.NoError(t, err)

	// Act
	kafkaConfig := LoadKafkaConfig()
	dbConfig := LoadDatabaseConfig()

	// Assert - environment variables should take precedence
	assert.Equal(t, "env-var:9092", kafkaConfig.BootstrapServers)
	assert.Equal(t, "env-var-db", dbConfig.Host)
}
