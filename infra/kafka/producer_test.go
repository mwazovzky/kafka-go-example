package kafka

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
	producer := &Producer{
		producer:     mockProducer,
		deliveryChan: make(chan kafka.Event, 1),
	}

	payload := []byte("test-payload")
	key := []byte("test-key")

	mockProducer.On("Produce", mock.Anything, producer.deliveryChan).Return(nil, nil)

	// Act
	err := producer.ProduceMessage(topic, payload, key)

	// Assert
	assert.NoError(t, err)
	mockProducer.AssertExpectations(t)
}

func TestProduceMessage_ProduceError(t *testing.T) {
	// Arrange
	mockProducer := new(MockProducer)
	topic := "test-topic"
	producer := &Producer{
		producer:     mockProducer,
		deliveryChan: make(chan kafka.Event, 1),
	}

	payload := []byte("test-payload")
	key := []byte("test-key")

	mockProducer.On("Produce", mock.Anything, producer.deliveryChan).Return(errors.New("produce error"), nil)

	// Act
	err := producer.ProduceMessage(topic, payload, key)

	// Assert
	assert.Error(t, err)
	assert.Equal(t, "produce error", err.Error())
	mockProducer.AssertExpectations(t)
}

func TestProduceMessage_DeliveryError(t *testing.T) {
	// Arrange
	mockProducer := new(MockProducer)
	topic := "test-topic"
	producer := &Producer{
		producer:     mockProducer,
		deliveryChan: make(chan kafka.Event, 1),
	}

	payload := []byte("test-payload")
	key := []byte("test-key")

	mockProducer.On("Produce", mock.Anything, producer.deliveryChan).Return(nil, errors.New("delivery error"))

	// Act
	err := producer.ProduceMessage(topic, payload, key)

	// Assert
	assert.Error(t, err)
	assert.Equal(t, "delivery error", err.Error())
	mockProducer.AssertExpectations(t)
}

func TestClose(t *testing.T) {
	// Arrange
	mockProducer := new(MockProducer)
	mockProducer.On("Close").Return()

	producer := &Producer{
		producer:     mockProducer,
		deliveryChan: make(chan kafka.Event, 1),
	}

	// Act
	producer.Close()

	// Assert
	assert.Panics(t, func() { close(producer.deliveryChan) }) // Ensure the channel is closed.
	mockProducer.AssertExpectations(t)
}
