package main

import (
	"log"

	"kafka-go-example/infra/avro"
	"kafka-go-example/infra/config"
	"kafka-go-example/infra/database"
	"kafka-go-example/infra/kafka"
	"kafka-go-example/tasks"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	// Load configurations
	dbCfg := config.LoadDatabaseConfig()
	kafkaCfg := config.LoadKafkaConfig()
	schemaCfg := config.LoadSchemaRegistryConfig()
	taskCfg, err := config.LoadSingleTaskConfig("config/users.yaml")
	if err != nil {
		log.Fatalf("Failed to load task configuration: %v", err)
	}

	// Initialize database
	db, err := database.NewDatabase(dbCfg)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Initialize serializer
	avroSer, err := avro.NewAvroSerializer(schemaCfg)
	if err != nil {
		log.Fatalf("Failed to create avro serializer: %v", err)
	}
	serializer, err := avro.NewSerializer(avroSer)
	if err != nil {
		log.Fatalf("Failed to create serializer: %v", err)
	}

	// Initialize producer
	kafkaProducer, err := kafka.NewKafkaProducer(kafkaCfg)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	producer := kafka.NewProducer(kafkaProducer)
	defer producer.Close()

	// Create the task
	t, err := tasks.CreateTask(db, taskCfg, serializer, producer)
	if err != nil {
		log.Fatalf("Failed to create task: %v", err)
	}

	// Execute the task once
	t.Execute()
}
