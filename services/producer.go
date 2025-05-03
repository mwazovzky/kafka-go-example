package services

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Producer abstracts the Kafka producer for testing purposes.
type Producer interface {
	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
	Close()
}

// ProducerService encapsulates Kafka producer logic.
type ProducerService struct {
	Producer     Producer
	DeliveryChan chan kafka.Event
}

// NewProducerService creates a new ProducerService with a reusable delivery channel.
func NewProducerService(p Producer) *ProducerService {
	return &ProducerService{
		Producer:     p,
		DeliveryChan: make(chan kafka.Event, 1), // Buffered channel for better performance.
	}
}

// ProduceMessage encapsulates publishing a message and handling delivery events.
func (ps *ProducerService) ProduceMessage(topic string, payload []byte, key []byte) error {
	err := ps.Producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
		Value:          payload,
	}, ps.DeliveryChan)
	if err != nil {
		return err
	}

	e := <-ps.DeliveryChan
	m := e.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		return m.TopicPartition.Error
	}
	return nil
}

// Close cleans up the delivery channel.
func (ps *ProducerService) Close() {
	close(ps.DeliveryChan)
	ps.Producer.Close()
}

// NewKafkaProducer creates a new Kafka producer.
func NewKafkaProducer(cfg KafkaConfig) (*ProducerService, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": cfg.BootstrapServers})
	if err != nil {
		return nil, err
	}
	return NewProducerService(producer), nil
}
