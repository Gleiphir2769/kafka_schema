package kafkaschema

import (
	"kafka_schema/schema/buffer"
)

type Type interface {

	// Read the typed object from the buffer
	Read(buffer *buffer.ByteBuffer) (interface{}, error)

	// Validate the object. If succeeded return its typed object.
	Validate(o interface{}) (interface{}, error)

	// SizeOf Return the size of the object in bytes
	SizeOf(o interface{}) (int, error)

	// Check if the type supports null values
	isNullable() bool

	// String Return the string of type
	String() string
}
