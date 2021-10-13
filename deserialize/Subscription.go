package deserialize

import (
	"kafka_schema/deserialize/common"
	buffer2 "kafka_schema/schema/buffer"
)

type Subscription struct {
	topics          []string
	userData        *buffer2.ByteBuffer
	ownedPartitions []*common.TopicPartition
	groupInstanceId string
}

func NewSubscription(topics []string, userData *buffer2.ByteBuffer, ownedPartitions []*common.TopicPartition) *Subscription {
	return &Subscription{
		topics:          topics,
		userData:        userData,
		ownedPartitions: ownedPartitions,
	}
}

func (s *Subscription) Topics() []string {
	return s.topics
}
