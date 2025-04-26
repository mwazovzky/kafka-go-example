# Kafka example

Go, Kafka, Avro Schema with [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go)

## Start services

## Create topic

## Create Schema

# Run Consumer

## Run Producer

## Todos

## Schema modification with optional fields

Compatibility:backward

1. add optional optional field to schema (at the end), producer model and consumer model are not modified - ok
   messages have no additonal field in kafka
   messages have no additonal field in consumer
2. add optional field to producer model, add field to producer model, consumer model is not modified
   messages have additonal optional field in kafka
   messages have no additonal field in consumer
3. add optional field to consumer model
   messages have additonal optional field in consumer

## Create abstractions
