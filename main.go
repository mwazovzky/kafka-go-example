package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"kafka-go-example/services"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	// Load configurations
	dbCfg := services.LoadDatabaseConfig()
	kafkaCfg := services.LoadKafkaConfig()
	schemaCfg := services.LoadSchemaRegistryConfig()

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

	// Load task configurations
	taskConfigs, err := services.LoadTaskConfigs("config")
	if err != nil {
		log.Fatalf("Failed to load task configurations: %v", err)
	}

	// Create a context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())

	// Initialize and run tasks
	for _, cfg := range taskConfigs {
		producer, err := services.NewKafkaProducer(kafkaCfg)
		if err != nil {
			log.Printf("Failed to create Kafka producer for task %s: %v", cfg.Name, err)
			continue
		}
		defer producer.Close()

		task, err := services.CreateTask(db, cfg, serializer, producer)
		if err != nil {
			log.Printf("Failed to initialize task: %v", err)
			continue
		}

		runner := services.NewTaskRunner(task, cfg.Interval)
		go runner.Run(ctx)
	}

	// Graceful shutdown
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	<-signalChan // Wait for termination signal
	log.Println("Shutting down gracefully...")
	cancel() // Cancel the context to stop tasks
}
