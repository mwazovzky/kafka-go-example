package kafka

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// KafkaProducerInterface abstracts the Kafka producer for testing purposes.
type KafkaProducerInterface interface {
	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
	Close()
}

type Producer struct {
	producer     KafkaProducerInterface // Using the interface instead of concrete type
	deliveryChan chan kafka.Event
}

func NewProducer(kp KafkaProducerInterface) *Producer {
	return &Producer{
		producer:     kp,
		deliveryChan: make(chan kafka.Event, 1),
	}
}

// ProduceMessage encapsulates publishing a message and handling delivery events.
func (p *Producer) ProduceMessage(topic string, payload []byte, key []byte) error {
	err := p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
		Value:          payload,
	}, p.deliveryChan)
	if err != nil {
		return err
	}

	e := <-p.deliveryChan
	m := e.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		return m.TopicPartition.Error
	}
	return nil
}

// Close cleans up the delivery channel.
func (p *Producer) Close() {
	close(p.deliveryChan)
	p.producer.Close()
}
