package deserialize

import (
	"kafka_schema/deserialize/common"
	buffer2 "kafka_schema/schema/buffer"
)

type Assignment struct {
	partitions []*common.TopicPartition
	userData   *buffer2.ByteBuffer
}

func NewAssignment(partitions []*common.TopicPartition, userData *buffer2.ByteBuffer) *Assignment {
	return &Assignment{
		partitions: partitions,
		userData:   userData,
	}
}

func (a *Assignment) Partitions() []*common.TopicPartition {
	return a.partitions
}
