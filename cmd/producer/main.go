package main

import (
	"log"

	"kafka-go-example/services"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	// Load configurations
	dbCfg := services.LoadDatabaseConfig()
	kafkaCfg := services.LoadKafkaConfig()
	schemaCfg := services.LoadSchemaRegistryConfig()
	taskCfg, err := services.LoadSingleTaskConfig("config/users.yaml")
	if err != nil {
		log.Fatalf("Failed to load task configuration: %v", err)
	}

	// Initialize database
	db, err := services.NewDatabase(dbCfg)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Initialize serializer
	serializer, err := services.NewAvroSerializer(schemaCfg)
	if err != nil {
		log.Fatalf("Failed to create serializer: %v", err)
	}

	// Initialize producer
	producer, err := services.NewKafkaProducer(kafkaCfg)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer producer.Close()

	// Create the task
	task, err := services.CreateTask(db, taskCfg, serializer, producer)
	if err != nil {
		log.Fatalf("Failed to create task: %v", err)
	}

	// Execute the task once
	task.Execute()
}
