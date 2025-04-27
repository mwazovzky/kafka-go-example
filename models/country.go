package models

// MessageCountry represents a country in Kafka messages
type Country struct {
	Code string `db:"code" avro:"code"`
	Name string `db:"name" avro:"name"`
}
