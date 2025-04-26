package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
)

type UserStatusUpdated struct {
	UserID int64  `json:"user_id"`
	Status string `json:"status"`
}

func main() {
	bootstrapServers := "localhost:9092"

	srUrl := "http://localhost:8081"
	srUsername := ""
	srPassword := ""
	topic := "user-status-updated"

	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": bootstrapServers})

	if err != nil {
		fmt.Printf("failed to create producer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("created producer %v\n", producer)

	client, err := schemaregistry.NewClient(schemaregistry.NewConfigWithAuthentication(srUrl, srUsername, srPassword))

	if err != nil {
		fmt.Printf("failed to create schemaregistry client: %s\n", err)
		os.Exit(1)
	}

	ser, err := avro.NewGenericSerializer(client, serde.ValueSerde, avro.NewSerializerConfig())

	if err != nil {
		fmt.Printf("failed to create serializer: %s\n", err)
		os.Exit(1)
	}

	deliveryChan := make(chan kafka.Event)

	value := UserStatusUpdated{
		UserID: 333,
		Status: "blocked",
	}

	payload, err := ser.Serialize(topic, &value)

	if err != nil {
		fmt.Printf("failed to serialize payload: %s\n", err)
		os.Exit(1)
	}

	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(strconv.FormatInt(value.UserID, 10)),
		Value:          payload,
		Headers:        []kafka.Header{{Key: "test-header-key", Value: []byte("test-header-value")}},
	}, deliveryChan)

	if err != nil {
		fmt.Printf("failed to produce message: %v\n", err)
		os.Exit(1)
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		fmt.Printf("message delivery failed: %v\n", m.TopicPartition.Error)
	} else {
		fmt.Printf(
			"message delivered to topic %s [%d] at offset %v\n",
			*m.TopicPartition.Topic,
			m.TopicPartition.Partition,
			m.TopicPartition.Offset,
		)
	}

	close(deliveryChan)
}
