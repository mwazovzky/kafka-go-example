package tasks

import (
	"fmt"
	"log"
	"os"
	"time"

	"kafka-go-example/infra/config"
	"kafka-go-example/repositories"

	"github.com/jmoiron/sqlx"
)

type RepositoryInterface[T any] interface {
	Stream(since time.Time) (<-chan T, <-chan error)
}

type SyncRepositoryInterface interface {
	Get(task string) (time.Time, error)
	Set(task string, syncedAt time.Time) error
}

type SerializerInterface interface {
	Serialize(schema string, data interface{}) ([]byte, error)
}

type ProducerInterface interface {
	ProduceMessage(topic string, payload []byte, key []byte) error
	Close()
}

type Task[T any] struct {
	Config     config.TaskConfig
	Repository RepositoryInterface[T]
	SyncRepo   SyncRepositoryInterface
	Serializer SerializerInterface
	Producer   ProducerInterface
}

func NewTask[T any](db *sqlx.DB, config config.TaskConfig, serializer SerializerInterface, producer ProducerInterface) (*Task[T], error) {
	query, err := loadQueryFromFile(config.QueryFile)
	if err != nil {
		return nil, err
	}

	syncRepo := repositories.NewSyncRepository(db)
	repo := repositories.NewRepository[T](db, query)

	return &Task[T]{
		Config:     config,
		Repository: repo,
		SyncRepo:   syncRepo,
		Serializer: serializer,
		Producer:   producer,
	}, nil
}

func (t *Task[T]) Execute() {
	const TimeDelay = 24 * time.Hour
	syncedAt, err := t.SyncRepo.Get(t.Config.Name)
	if err != nil {
		log.Printf("Failed to get last sync time: %v", err)
		return
	}

	data, errs := t.Repository.Stream(syncedAt)
	syncedAt = time.Now()

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

	err = t.SyncRepo.Set(t.Config.Name, syncedAt)
	if err != nil {
		log.Printf("Failed to set sync time: %v", err)
		return
	}

	log.Printf("Task <%s> completed", t.Config.Name)
}

func loadQueryFromFile(filePath string) (string, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to open query file %s: %w", filePath, err)
	}
	return string(content), nil
}
