package main

import (
	"fmt"
	"kafka-go-example/services"
	"os"
	"strconv"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Country struct {
	Code string `avro:"code"`
	Name string `avro:"name"`
}

type UserStatusUpdated struct {
	UserID  int64   `avro:"user_id"`
	Status  string  `avro:"status"`
	Name    string  `avro:"user_name"`
	Country Country `avro:"country"`
}

func main() {
	// read config
	kafkaCfg := services.LoadKafkaConfig()
	schemaregistryCfg := services.LoadSchemaRegistryConfig()
	topic := kafkaCfg.Topic

	// create producer
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaCfg.BootstrapServers})
	if err != nil {
		fmt.Printf("failed to create producer: %s\n", err)
		os.Exit(1)
	}
	defer producer.Close()
	fmt.Printf("created producer %v\n", producer)

	// create schema registry serializer
	ser, err := services.NewAvroSerializer(schemaregistryCfg)
	if err != nil {
		fmt.Printf("failed to create serializer: %s\n", err)
		os.Exit(1)
	}

	// register schema
	ser.RegisterType("UserStatusUpdated", UserStatusUpdated{})

	// serialize message
	value := UserStatusUpdated{
		UserID: 333,
		Status: "blocked",
		Name:   "john doe",
		Country: Country{
			Code: "US",
			Name: "United States",
		},
	}
	payload, err := ser.Serialize(topic, &value)
	if err != nil {
		fmt.Printf("failed to serialize payload: %s\n", err)
		os.Exit(1)
	}

	// create delivery channel and ensure its cleanup
	deliveryChan := make(chan kafka.Event)
	defer close(deliveryChan)

	// produce message
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
}
