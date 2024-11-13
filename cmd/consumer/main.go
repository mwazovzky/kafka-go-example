package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

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

	// if len(os.Args) < 5 {
	// 	fmt.Fprintf(os.Stderr, "Usage: %s <bootstrap-servers> <schema-registry> <group> <topics..>\n",
	// 		os.Args[0])
	// 	os.Exit(1)
	// }

	// bootstrapServers := os.Args[1]
	// url := os.Args[2]
	// group := os.Args[3]
	// topics := os.Args[4:]

	bootstrapServers := "localhost:9092"
	schemaregistryURL := "http://localhost:8081"
	group := "user-consumer-group"
	topics := []string{"user-status-updated"}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  bootstrapServers,
		"group.id":           group,
		"session.timeout.ms": 6000,
		"auto.offset.reset":  "earliest"})

	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("created consumer %v\n", c)

	client, err := schemaregistry.NewClient(schemaregistry.NewConfig(schemaregistryURL))

	if err != nil {
		fmt.Printf("failed to create schemaregistry client: %s\n", err)
		os.Exit(1)
	}

	deserializer, err := avro.NewGenericDeserializer(client, serde.ValueSerde, avro.NewDeserializerConfig())

	if err != nil {
		fmt.Printf("failed to create deserializer: %s\n", err)
		os.Exit(1)
	}

	err = c.SubscribeTopics(topics, nil)

	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to subscribe to topics: %s\n", err)
		os.Exit(1)
	}

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

			switch e := ev.(type) {
			case *kafka.Message:
				value := UserStatusUpdated{}
				err := deserializer.DeserializeInto(*e.TopicPartition.Topic, e.Value, &value)
				if err != nil {
					fmt.Printf("failed to deserialize payload: %s\n", err)
				} else {
					fmt.Printf("%% got message on %s:\n%+v\n", e.TopicPartition, value)
				}
				if e.Headers != nil {
					fmt.Printf("%% Headers: %v\n", e.Headers)
				}
			case kafka.Error:
				// Errors should generally be considered
				// informational, the client will try to
				// automatically recover.
				fmt.Fprintf(os.Stderr, "%% error: %v: %v\n", e.Code(), e)
			default:
				fmt.Printf("ignored %v\n", e)
			}
		}
	}

	fmt.Printf("closing consumer\n")
	c.Close()
}
