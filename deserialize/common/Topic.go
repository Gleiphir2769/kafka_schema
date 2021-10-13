package common

type TopicPartition struct {
	partition int
	topic     string
}

func NewTopicPartition(topic string, partition int) *TopicPartition {
	return &TopicPartition{
		partition: partition,
		topic:     topic,
	}
}

func (tp *TopicPartition) Partition() int {
	return tp.partition
}

func (tp *TopicPartition) Topic() string {
	return tp.topic
}
