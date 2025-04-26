package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avrov2"
)

type UserStatusUpdated struct {
	UserID int64   `avro:"user_id"`
	Status string  `avro:"status"`
	Name   *string `avro:"user_name"`
}

func main() {
	bootstrapServers := "localhost:9092"
	srUrl := "http://localhost:8081"
	srUsername := ""
	srPassword := ""
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
		fmt.Printf("failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("created consumer %v\n", c)

	client, err := schemaregistry.NewClient(schemaregistry.NewConfigWithAuthentication(srUrl, srUsername, srPassword))

	if err != nil {
		fmt.Printf("failed to create schemaregistry client: %s\n", err)
		os.Exit(1)
	}

	deserializer, err := avrov2.NewDeserializer(client, serde.ValueSerde, avrov2.NewDeserializerConfig())

	if err != nil {
		fmt.Printf("failed to create deserializer: %s\n", err)
		os.Exit(1)
	}

	err = c.SubscribeTopics(topics, nil)

	if err != nil {
		fmt.Printf("failed to subscribe to topics: %s\n", err)
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
					fmt.Printf("got new message on %s:\n%+v\n", e.TopicPartition, value)
				}
				if e.Headers != nil {
					fmt.Printf("headers: %v\n", e.Headers)
				}
			case kafka.Error:
				// Errors should generally be considered informational, the client will try to automatically recover.
				fmt.Printf("error: %v: %v\n", e.Code(), e)
			case *kafka.AssignedPartitions:
				fmt.Printf("Assigned partitions: %v\n", e.Partitions)
				c.Assign(e.Partitions)
			case *kafka.RevokedPartitions:
				fmt.Printf("Revoked partitions: %v\n", e.Partitions)
				c.Unassign()
			case *kafka.PartitionEOF:
				fmt.Printf("Reached end of partition: %v\n", e)
			case *kafka.OffsetsCommitted:
				// Either ignore silently or log with more detail
				fmt.Printf("Offsets committed: %v\n", e)
			default:
				fmt.Printf("Ignored event: %v\n", e)
			}
		}
	}

	fmt.Printf("closing consumer\n")
	c.Close()
}
