package services

import (
	"errors"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockProducer struct {
	mock.Mock
}

func (m *MockProducer) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	args := m.Called(msg, deliveryChan)
	if deliveryChan != nil {
		deliveryChan <- &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     msg.TopicPartition.Topic,
				Partition: kafka.PartitionAny,
				Error:     args.Error(1),
			},
		}
	}
	return args.Error(0)
}

func (m *MockProducer) Close() {
	m.Called()
}

func TestProduceMessage_Success(t *testing.T) {
	// Arrange
	mockProducer := new(MockProducer)
	topic := "test-topic"
	ps := &ProducerService{
		Producer:     mockProducer,
		Topic:        topic,
		DeliveryChan: make(chan kafka.Event, 1),
	}

	payload := []byte("test-payload")
	key := []byte("test-key")

	mockProducer.On("Produce", mock.Anything, ps.DeliveryChan).Return(nil, nil)

	// Act
	err := ps.ProduceMessage(payload, key)

	// Assert
	assert.NoError(t, err)
	mockProducer.AssertExpectations(t)
}

func TestProduceMessage_ProduceError(t *testing.T) {
	// Arrange
	mockProducer := new(MockProducer)
	topic := "test-topic"
	ps := &ProducerService{
		Producer:     mockProducer,
		Topic:        topic,
		DeliveryChan: make(chan kafka.Event, 1),
	}

	payload := []byte("test-payload")
	key := []byte("test-key")

	mockProducer.On("Produce", mock.Anything, ps.DeliveryChan).Return(errors.New("produce error"), nil)

	// Act
	err := ps.ProduceMessage(payload, key)

	// Assert
	assert.Error(t, err)
	assert.Equal(t, "produce error", err.Error())
	mockProducer.AssertExpectations(t)
}

func TestProduceMessage_DeliveryError(t *testing.T) {
	// Arrange
	mockProducer := new(MockProducer)
	topic := "test-topic"
	ps := &ProducerService{
		Producer:     mockProducer,
		Topic:        topic,
		DeliveryChan: make(chan kafka.Event, 1),
	}

	payload := []byte("test-payload")
	key := []byte("test-key")

	mockProducer.On("Produce", mock.Anything, ps.DeliveryChan).Return(nil, errors.New("delivery error"))

	// Act
	err := ps.ProduceMessage(payload, key)

	// Assert
	assert.Error(t, err)
	assert.Equal(t, "delivery error", err.Error())
	mockProducer.AssertExpectations(t)
}

func TestClose(t *testing.T) {
	// Arrange
	mockProducer := new(MockProducer)
	mockProducer.On("Close").Return()

	ps := &ProducerService{
		Producer:     mockProducer,
		Topic:        "test-topic",
		DeliveryChan: make(chan kafka.Event, 1),
	}

	// Act
	ps.Close()

	// Assert
	assert.Panics(t, func() { close(ps.DeliveryChan) }) // Ensure the channel is closed.
	mockProducer.AssertExpectations(t)
}
