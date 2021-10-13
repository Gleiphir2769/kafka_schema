package common

import (
	"fmt"
)


type GroupTopicPartition struct {
	group          string
	topicPartition *TopicPartition
}

func NewGroupTopicPartition(group string, topicPartition *TopicPartition) *GroupTopicPartition {
	return &GroupTopicPartition{
		group:          group,
		topicPartition: topicPartition,
	}
}

func (gpt *GroupTopicPartition) Group() string {
	return gpt.group
}

func (gpt *GroupTopicPartition) TopicPartition() *TopicPartition {
	return gpt.topicPartition
}

type BaseKey interface{}

type OffsetKey struct {
	BaseKey
	version int16
	key     *GroupTopicPartition
}

func NewOffsetKey(version int16, key *GroupTopicPartition) *OffsetKey {
	return &OffsetKey{
		version: version,
		key:     key,
	}
}

func (offKey *OffsetKey) Key() *GroupTopicPartition {
	return offKey.key
}

type GroupMetadataKey struct {
	BaseKey
	version int16
	key     string
}

func NewGroupMetadataKey(version int16, key string) *GroupMetadataKey {
	return &GroupMetadataKey{
		version: version,
		key:     key,
	}
}

func (gmk *GroupMetadataKey) Key() string {
	return gmk.key
}

type ConsumerIdentity struct {
	name         string
	consumerType string
}

func NewConsumerIdentity(name string, consumerType string) *ConsumerIdentity {
	return &ConsumerIdentity{name: name, consumerType: consumerType}
}

func (c *ConsumerIdentity) Name() string {
	return c.name
}

func (c *ConsumerIdentity) Type() string {
	return c.consumerType
}

type ConsumedTopicDescription struct {
	ConsumerGroup          string
	TopicName              string
	PartitionLatestOffsets map[int]int64
	PartitionOwners        map[int]string
	PartitionOffsets       map[int]int64
}

func (ctd *ConsumedTopicDescription) String() string {
	return fmt.Sprintf("ctd detail: ConsumerGroup: %s, topic: %s, LatestOffsets: %v, PartitionOwners: %v, PartitionOffsets: %v",
		ctd.ConsumerGroup,
		ctd.TopicName,
		ctd.PartitionLatestOffsets,
		ctd.PartitionOwners,
		ctd.PartitionOffsets)
}

// FormatPrint 测试用
func (ctd *ConsumedTopicDescription) FormatPrint() {
	fmt.Printf("ConsumedTopicDescription: group: %s, topicName: %s\n", ctd.ConsumerGroup, ctd.TopicName)
	fmt.Printf("partition latest offset: %v\n", ctd.PartitionLatestOffsets)
	fmt.Printf("partition owners: %v\n", ctd.PartitionOwners)
	fmt.Printf("partition offsets: %v\n", ctd.PartitionOffsets)
}
