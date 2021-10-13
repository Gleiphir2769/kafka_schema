package deserialize

import (
	"fmt"
	"kafka_schema/deserialize/common"
	"kafka_schema/schema"
	buffer2 "kafka_schema/schema/buffer"
	"kafka_schema/util"
)

const (
	VersionKeyName         = "version"
	TopicKeyName           = "topic"
	PartitionsKeyName      = "partitions"
	OwnedPartitionsKeyName = "owned_partitions"
	TopicPartitionsKeyName = "topic_partitions"
	UserDataKeyName        = "user_data"
	ConsumerProtocolV0     = int16(0)
	ConsumerProtocolV1     = int16(1)
)

var (
	ConsumerProtocolHeaderSchema *kafkaschema.Schema
	AssignmentV0                 *kafkaschema.Schema
	SubscriptionV0               *kafkaschema.Schema
	SubscriptionV1               *kafkaschema.Schema
	TopicAssignmentV0            *kafkaschema.Schema
)

func InitConsumerProtocol() (err error) {
	if ConsumerProtocolHeaderSchema, err = kafkaschema.NewSchema(
		kafkaschema.NewField(VersionKeyName, kafkaschema.INT16),
	); err != nil {
		return fmt.Errorf("initConsumerProtocol %s", err)
	}

	if TopicAssignmentV0, err = kafkaschema.NewSchema(
		kafkaschema.NewField(TopicKeyName, kafkaschema.STRING),
		kafkaschema.NewField(PartitionsKeyName, kafkaschema.NewArrayOf(kafkaschema.INT32)),
	); err != nil {
		return fmt.Errorf("initConsumerProtocol %s", err)
	}

	if AssignmentV0, err = kafkaschema.NewSchema(
		kafkaschema.NewField(TopicPartitionsKeyName, kafkaschema.NewArrayOf(TopicAssignmentV0)),
		kafkaschema.NewField(UserDataKeyName, kafkaschema.NullableBytes),
	); err != nil {
		return fmt.Errorf("initConsumerProtocol %s", err)
	}

	if SubscriptionV0, err = kafkaschema.NewSchema(
		kafkaschema.NewField(TopicKeyName, kafkaschema.NewArrayOf(kafkaschema.STRING)),
		kafkaschema.NewField(UserDataKeyName, kafkaschema.NullableBytes),
	); err != nil {
		return fmt.Errorf("initConsumerProtocol %s", err)
	}

	if SubscriptionV1, err = kafkaschema.NewSchema(
		kafkaschema.NewField(TopicKeyName, kafkaschema.NewArrayOf(kafkaschema.STRING)),
		kafkaschema.NewField(UserDataKeyName, kafkaschema.NullableBytes),
		kafkaschema.NewField(OwnedPartitionsKeyName, kafkaschema.NewArrayOf(TopicAssignmentV0)),
	); err != nil {
		return fmt.Errorf("initConsumerProtocol %s", err)
	}

	return
}

type consumerProtocol struct{}

func ConsumerProtocol() *consumerProtocol {
	return &consumerProtocol{}
}

func (cp *consumerProtocol) deserializeVersion(buffer *buffer2.ByteBuffer) (int16, error) {
	header, err := ConsumerProtocolHeaderSchema.Read(buffer)
	if err != nil {
		return 0, fmt.Errorf("deserialize version field: %s", err)
	}

	if headerStruct, ok := header.(*kafkaschema.Struct); ok {
		return headerStruct.GetInt16(VersionKeyName)
	} else {
		return 0, fmt.Errorf("deserialize version field")
	}
}

func (cp *consumerProtocol) deserializeSubscriptionV0(buffer *buffer2.ByteBuffer) (*Subscription, error) {
	subscriptionV0, err := SubscriptionV0.Read(buffer)
	if err != nil {
		return nil, err
	}

	if Struct, ok := subscriptionV0.(*kafkaschema.Struct); ok {
		userData, err := Struct.GetByteBuffer(UserDataKeyName)
		topics, err := cp.deserializeTopics(Struct)
		if err != nil {
			return nil, err
		}
		return NewSubscription(topics, userData, nil), nil
	}
	return nil, fmt.Errorf("subscription V0 conversion error")
}

func (cp *consumerProtocol) deserializeSubscriptionV1(buffer *buffer2.ByteBuffer) (*Subscription, error) {
	subscriptionV1, err := SubscriptionV1.Read(buffer)
	if err != nil {
		return NewSubscription(nil, nil, nil), nil
	}

	if Struct, ok := subscriptionV1.(*kafkaschema.Struct); ok {
		userData, err := Struct.GetByteBuffer(UserDataKeyName)
		topics, err := cp.deserializeTopics(Struct)
		ownedPartitions, err := cp.deserializeOwnedPartitions(Struct)
		if err != nil {
			return nil, err
		}
		return NewSubscription(topics, userData, ownedPartitions), nil
	}
	return nil, fmt.Errorf("subscription V0 conversion error")
}

func (cp *consumerProtocol) deserializeTopics(Struct *kafkaschema.Struct) ([]string, error) {
	topics, err := Struct.GetArray(TopicKeyName)
	if err != nil {
		return nil, err
	}

	ts := make([]string, 0)
	for _, t := range topics {
		if stringT, err := util.Interface2String(t); err == nil {
			ts = append(ts, stringT)
		}
	}
	return ts, nil
}

func (cp *consumerProtocol) deserializePartitions(assignment *kafkaschema.Struct, topic string) []*common.TopicPartition {
	tps := make([]*common.TopicPartition, 0)
	partitions, err := assignment.GetArray(PartitionsKeyName)
	if err != nil {
		return tps
	}

	for _, partition := range partitions {
		if intP, err := util.Interface2Int(partition); err == nil {
			tps = append(tps, common.NewTopicPartition(topic, intP))
		}
	}
	return tps
}

func (cp *consumerProtocol) deserializeOwnedPartitions(Struct *kafkaschema.Struct) ([]*common.TopicPartition, error) {
	topicPartitions, err := Struct.GetArray(OwnedPartitionsKeyName)
	return cp.deserializeTopicPartitions1(err, topicPartitions)
}

func (cp *consumerProtocol) deserializeTopicPartitions(Struct *kafkaschema.Struct) ([]*common.TopicPartition, error) {
	topicPartitions, err := Struct.GetArray(TopicPartitionsKeyName)
	return cp.deserializeTopicPartitions1(err, topicPartitions)
}

func (cp *consumerProtocol) deserializeTopicPartitions1(err error, topicPartitions []interface{}) ([]*common.TopicPartition, error) {
	if err != nil {
		return nil, err
	}

	tps := make([]*common.TopicPartition, 0)
	for _, tp := range topicPartitions {
		assignment, ok := tp.(*kafkaschema.Struct)
		if !ok {
			return nil, fmt.Errorf("topicPartitions conversion error")
		}
		topic, err := assignment.GetString(TopicKeyName)
		if err != nil {
			return nil, err
		}
		tps = append(tps, cp.deserializePartitions(assignment, topic)...)
	}
	return tps, nil
}

func (cp *consumerProtocol) deserializeAssignmentV0(buffer *buffer2.ByteBuffer) (*Assignment, error) {
	assignmentV0, err := AssignmentV0.Read(buffer)
	if err != nil {
		return nil, err
	}

	if Struct, ok := assignmentV0.(*kafkaschema.Struct); ok {
		userData, err := Struct.GetByteBuffer(UserDataKeyName)
		pts, err := cp.deserializeTopicPartitions(Struct)
		if err != nil {
			return nil, err
		}
		return NewAssignment(pts, userData), nil
	}
	return nil, fmt.Errorf("assignment V0 conversion error")
}

func (cp *consumerProtocol) deserializeAssignmentV1(buffer *buffer2.ByteBuffer) (*Assignment, error) {
	return cp.deserializeAssignmentV0(buffer)
}

func (cp *consumerProtocol) DeserializeSubscription(buffer *buffer2.ByteBuffer) (*Subscription, error) {
	version, err := cp.deserializeVersion(buffer)
	if err != nil {
		return nil, err
	}

	if version < ConsumerProtocolV0 {
		return nil, fmt.Errorf("unsupported subscription version: %v", version)
	}

	switch version {
	case ConsumerProtocolV0:
		return cp.deserializeSubscriptionV0(buffer)
	case ConsumerProtocolV1:
		return cp.deserializeSubscriptionV1(buffer)
	default:
		return cp.deserializeSubscriptionV1(buffer)
	}
}

func (cp *consumerProtocol) DeserializeAssignment(buffer *buffer2.ByteBuffer) (*Assignment, error) {
	version, err := cp.deserializeVersion(buffer)
	if err != nil {
		return nil, err
	}

	if version < ConsumerProtocolV0 {
		return nil, fmt.Errorf("unsupported assignment version: %v", version)
	}

	switch version {
	case ConsumerProtocolV0:
		return cp.deserializeAssignmentV0(buffer)
	case ConsumerProtocolV1:
		return cp.deserializeAssignmentV1(buffer)
	default:
		return cp.deserializeAssignmentV1(buffer)
	}
}
