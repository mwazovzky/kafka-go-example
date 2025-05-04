package avro

import (
	"kafka-go-example/infra/config"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avrov2"
)

// NewAvroSerializer returns an Avro serializer using the provided configuration.
func NewAvroSerializer(cfg config.SchemaRegistryConfig) (*avrov2.Serializer, error) {
	client, err := schemaregistry.NewClient(schemaregistry.NewConfigWithAuthentication(
		cfg.SchemaRegistryUrl,
		cfg.SchemaRegistryUsername,
		cfg.SchemaRegistryPassword,
	))
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
	return serializer, nil
}

// NewAvroDeserializer returns an Avro deserializer using the provided configuration.
func NewAvroDeserializer(cfg config.SchemaRegistryConfig) (*avrov2.Deserializer, error) {
	client, err := schemaregistry.NewClient(schemaregistry.NewConfigWithAuthentication(
		cfg.SchemaRegistryUrl,
		cfg.SchemaRegistryUsername,
		cfg.SchemaRegistryPassword,
	))
	if err != nil {
		return nil, err
	}
	return avrov2.NewDeserializer(client, serde.ValueSerde, avrov2.NewDeserializerConfig())
}
