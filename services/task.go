package services

import (
	"fmt"
	"log"
	"os"
	"time"

	"kafka-go-example/models"
	"kafka-go-example/repositories"

	"github.com/jmoiron/sqlx"
)

// TaskInterface defines the behavior of a task.
type TaskInterface interface {
	Execute()
}

type Task[T any] struct {
	Config     TaskConfig
	Repository *repositories.Repository[T]
	Serializer *AvroSerializer
	Producer   *ProducerService
}

func NewTask[T any](db *sqlx.DB, config TaskConfig, serializer *AvroSerializer, producer *ProducerService) (*Task[T], error) {
	query, err := loadQueryFromFile(config.QueryFile)
	if err != nil {
		return nil, err
	}

	repo := repositories.NewRepository[T](db, query)

	return &Task[T]{
		Config:     config,
		Repository: repo,
		Serializer: serializer,
		Producer:   producer,
	}, nil
}

func (t *Task[T]) Execute() {
	const TimeDelay = 24 * time.Hour
	data, errs := t.Repository.Stream(time.Now().Add(-TimeDelay))

	for item := range data {
		payload, err := t.Serializer.Serialize(t.Config.Schema, &item)
		if err != nil {
			log.Printf("Failed to serialize data: %v", err)
			continue
		}

		err = t.Producer.ProduceMessage(t.Config.Topic, payload, nil)
		if err != nil {
			log.Printf("Failed to produce message: %v", err)
			continue
		}

		log.Printf("Message produced for topic: %s", t.Config.Topic)
	}

	if err, ok := <-errs; ok && err != nil {
		log.Printf("Error streaming data: %v", err)
	}

	log.Printf("Task <%s> completed", t.Config.Name)
}

// CreateTask dynamically creates a Task based on the model name.
func CreateTask(db *sqlx.DB, cfg TaskConfig, serializer *AvroSerializer, producer *ProducerService) (TaskInterface, error) {
	switch cfg.Name {
	case "user":
		return NewTask[models.User](db, cfg, serializer, producer)
	// Add cases for other models here
	default:
		return nil, fmt.Errorf("unsupported task name: %s", cfg.Name)
	}
}

func loadQueryFromFile(filePath string) (string, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to open query file %s: %w", filePath, err)
	}
	return string(content), nil
}
