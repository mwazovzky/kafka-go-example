package main

import (
	"fmt"
	"kafka-go-example/models"
	"kafka-go-example/services"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avrov2"
)

func main() {
	// read config
	kafkaCfg := services.LoadKafkaConfig()
	schemaregistryCfg := services.LoadSchemaRegistryConfig()
	group := kafkaCfg.Group
	topics := []string{kafkaCfg.Topic}

	// create consumer
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  kafkaCfg.BootstrapServers,
		"group.id":           group,
		"session.timeout.ms": 6000,
		"auto.offset.reset":  "earliest",
	})
	if err != nil {
		fmt.Printf("failed to create consumer: %s\n", err)
		os.Exit(1)
	}
	defer c.Close()
	fmt.Printf("created consumer %v\n", c)

	// create schema registry deserializer
	deserializer, err := services.NewAvroDeserializer(schemaregistryCfg)
	if err != nil {
		fmt.Printf("failed to create deserializer: %s\n", err)
		os.Exit(1)
	}

	// set up signal handling
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// subscribe to topics
	err = c.SubscribeTopics(topics, nil)
	if err != nil {
		fmt.Printf("failed to subscribe to topics: %s\n", err)
		os.Exit(1)
	}
	fmt.Printf("subscribed to topics: %v\n", topics)

	// handle incoming messages
	run := true
	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := c.Poll(100)
			if ev == nil {
				continue
			}
			handleKafkaEvent(ev, deserializer)
		}
	}

	fmt.Printf("closing consumer\n")
}

func handleKafkaEvent(e kafka.Event, deserializer *avrov2.Deserializer) {
	switch evt := e.(type) {
	case *kafka.Message:
		value := models.User{}
		err := deserializer.DeserializeInto(*evt.TopicPartition.Topic, evt.Value, &value)
		if err != nil {
			fmt.Printf("failed to deserialize payload: %s\n", err)
		} else {
			fmt.Printf("got new message on %s:\n%+v\n", evt.TopicPartition, value)
		}
		if evt.Headers != nil {
			fmt.Printf("headers: %v\n", evt.Headers)
		}
	// ...handle other event types as needed...
	default:
		fmt.Printf("Ignored event: %v\n", e)
	}
}
