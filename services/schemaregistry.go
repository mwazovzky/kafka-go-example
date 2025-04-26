package services

import (
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avrov2"
)

func createSchemaRegistryClient(cfg SchemaRegistryConfig) (schemaregistry.Client, error) {
	return schemaregistry.NewClient(schemaregistry.NewConfigWithAuthentication(
		cfg.SchemaRegistryUrl,
		cfg.SchemaRegistryUsername,
		cfg.SchemaRegistryPassword,
	))
}

// NewAvroSerializer returns an Avro serializer using the provided configuration.
func NewAvroSerializer(cfg SchemaRegistryConfig) (*avrov2.Serializer, error) {
	client, err := createSchemaRegistryClient(cfg)
	if err != nil {
		return nil, err
	}
	srCfg := avrov2.NewSerializerConfig()
	srCfg.AutoRegisterSchemas = cfg.AutoRegisterSchemas
	srCfg.UseLatestVersion = cfg.UseLatestVersion
	return avrov2.NewSerializer(client, serde.ValueSerde, srCfg)
}

// NewAvroDeserializer returns an Avro deserializer using the provided configuration.
func NewAvroDeserializer(cfg SchemaRegistryConfig) (*avrov2.Deserializer, error) {
	client, err := createSchemaRegistryClient(cfg)
	if err != nil {
		return nil, err
	}
	return avrov2.NewDeserializer(client, serde.ValueSerde, avrov2.NewDeserializerConfig())
}
