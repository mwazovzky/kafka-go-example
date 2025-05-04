package avro

type AvroSerializer interface {
	Serialize(schema string, data interface{}) ([]byte, error)
}

// Serializer wraps
type Serializer struct {
	serializer AvroSerializer
}

// NewAvroSerializer returns an Avro serializer using the provided configuration.
func NewSerializer(serializer AvroSerializer) (*Serializer, error) {
	return &Serializer{serializer: serializer}, nil
}

// Serialize serializes the given data into Avro format using the specified schema.
func (s *Serializer) Serialize(schema string, data interface{}) ([]byte, error) {
	return s.serializer.Serialize(schema, data)
}
