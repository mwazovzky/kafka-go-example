package services

import (
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avrov2"
)

// NewAvroSerializer returns an Avro serializer using the provided configuration.
func NewAvroSerializer(cfg SchemaRegistryConfig) (*AvroSerializer, error) {
	client, err := createSchemaRegistryClient(cfg)
	if err != nil {
		return nil, err
	}
	srCfg := avrov2.NewSerializerConfig()
	srCfg.AutoRegisterSchemas = cfg.AutoRegisterSchemas
	srCfg.UseLatestVersion = cfg.UseLatestVersion
	serializer, err := avrov2.NewSerializer(client, serde.ValueSerde, srCfg)
	if err != nil {
		return nil, err
	}
	return &AvroSerializer{serializer: serializer}, nil
}

func createSchemaRegistryClient(cfg SchemaRegistryConfig) (schemaregistry.Client, error) {
	return schemaregistry.NewClient(schemaregistry.NewConfigWithAuthentication(
		cfg.SchemaRegistryUrl,
		cfg.SchemaRegistryUsername,
		cfg.SchemaRegistryPassword,
	))
}

// NewAvroDeserializer returns an Avro deserializer using the provided configuration.
func NewAvroDeserializer(cfg SchemaRegistryConfig) (*avrov2.Deserializer, error) {
	client, err := createSchemaRegistryClient(cfg)
	if err != nil {
		return nil, err
	}
	return avrov2.NewDeserializer(client, serde.ValueSerde, avrov2.NewDeserializerConfig())
}

// AvroSerializer wraps the avrov2.Serializer to add custom methods.
type AvroSerializer struct {
	serializer *avrov2.Serializer
}

// Serialize serializes the given data into Avro format using the specified schema.
func (s *AvroSerializer) Serialize(schema string, data interface{}) ([]byte, error) {
	return s.serializer.Serialize(schema, data)
}
