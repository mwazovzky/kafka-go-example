package tasks

import (
	"fmt"

	"kafka-go-example/infra/config"
	"kafka-go-example/models"

	"github.com/jmoiron/sqlx"
)

// CreateTask dynamically creates a Task based on the model name.
func CreateTask(db *sqlx.DB, cfg config.TaskConfig, serializer SerializerInterface, producer ProducerInterface) (TaskInterface, error) {
	switch cfg.Name {
	case "user":
		return NewTask[models.User](db, cfg, serializer, producer)
	// Add cases for other models here
	default:
		return nil, fmt.Errorf("unsupported task name: %s", cfg.Name)
	}
}
