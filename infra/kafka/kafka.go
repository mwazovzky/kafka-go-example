package kafka

import (
	"kafka-go-example/infra/config"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func NewKafkaProducer(cfg config.KafkaConfig) (*kafka.Producer, error) {
	return kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": cfg.BootstrapServers,
	})
}
