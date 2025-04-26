# Kafka example

Go, Kafka, Avro Schema with [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go)

## Start services

```bash
docker compose up -d
```

## Create topic

## Create Schema

# Run Consumer

```bash
go run cmd/consumer/main.go
```

## Run Producer

```bash
go run cmd/producer/main.go
```

## Schema modification with optional fields

Compatibility:backward

### Step 1. Modify Schema

Add optional field to schema (at the end), producer model and consumer model are not modified - ok

- messages have no additonal field in kafka
- messages have no additonal field in consumer

### Step 2. Update Producer

Add optional field to producer model, add field to producer model, consumer model is not modified - ok

- messages have additonal optional field in kafka
- messages have no additonal field in consumer

### Step 3. Update Consumer

Add optional field to consumer model

- messages have additonal optional field in consumer
