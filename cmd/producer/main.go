package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"kafka-go-example/models"
	"kafka-go-example/repository"
	"kafka-go-example/services"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	_ "github.com/go-sql-driver/mysql"
)

const query = `
SELECT 
	u.id as id, 
	u.status as status, 
	u.name as name, 
	u.created_at as created_at, 
	u.updated_at as updated_at, 
	c.code as "country.code", 
	c.name as "country.name"
FROM users u
JOIN countries c ON u.country_id = c.id
WHERE u.updated_at > ?`

func main() {
	kafkaCfg := services.LoadKafkaConfig()
	topic := kafkaCfg.Topic
	producer, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaCfg.BootstrapServers})
	if err != nil {
		fmt.Printf("failed to create producer: %s\n", err)
		os.Exit(1)
	}
	defer producer.Close()

	ps := services.NewProducerService(producer, topic)
	defer ps.Close()

	// create serializer
	schemaregistryCfg := services.LoadSchemaRegistryConfig()
	ser, err := services.NewAvroSerializer(schemaregistryCfg)
	if err != nil {
		fmt.Printf("failed to create serializer: %s\n", err)
		os.Exit(1)
	}
	schema := schemaregistryCfg.Schema
	ser.RegisterType("User", models.User{})

	// create database connection
	dbCfg := services.LoadDatabaseConfig()
	db, err := services.NewDatabase(dbCfg)
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}

	userRepo := repository.NewRepository[models.User](db, query)
	users, errs := userRepo.Stream(time.Now().Add(-24 * time.Hour))

	for user := range users {
		payload, err := ser.Serialize(schema, &user)
		if err != nil {
			log.Printf("message serialisation failed for user %d: %v", user.ID, err)
			continue
		}

		err = ps.ProduceMessage(payload, []byte(strconv.FormatInt(user.ID, 10)))

		if err != nil {
			log.Printf("failed to produce message for user %d: %v", user.ID, err)
			continue
		}

		log.Printf("message produced for user %d", user.ID)
	}

	if err, ok := <-errs; ok && err != nil {
		log.Printf("Error streaming users: %v", err)
	}
}
