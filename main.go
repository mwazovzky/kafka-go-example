package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

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
	taskConfigs, err := config.LoadTaskConfigs("config")
	if err != nil {
		log.Fatalf("Failed to load task configurations: %v", err)
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

	// Create a context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())

	// Initialize and run tasks
	for _, cfg := range taskConfigs {
		kafkaProducer, err := kafka.NewKafkaProducer(kafkaCfg)
		if err != nil {
			log.Printf("Failed to create Kafka producer for task %s: %v", cfg.Name, err)
		}
		producer := kafka.NewProducer(kafkaProducer)
		defer producer.Close()

		if err != nil {
			continue
		}
		defer kafkaProducer.Close()

		t, err := tasks.CreateTask(db, cfg, serializer, producer)
		if err != nil {
			log.Printf("Failed to initialize task: %v", err)
			continue
		}

		runner := tasks.NewTaskRunner(t, cfg.Interval)
		go runner.Run(ctx)
	}

	// Graceful shutdown
	// Wait for termination signal
	// Cancel the context to stop tasks
	// @fixme: add a timeout to the context
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	<-signalChan
	log.Println("Shutting down gracefully...")
	cancel()
}
