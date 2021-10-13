package deserialize

import (
	"fmt"
	"kafka_schema/deserialize/common"
	"kafka_schema/schema"
	buffer2 "kafka_schema/schema/buffer"
	"sync"
	"time"
)

const (
	CurrentOffsetKeySchemaVersion = int16(1)
	CurrentGroupKeySchemaVersion  = int16(2)
)

var once sync.Once
var Gmm *groupMetadataManager

type groupMetadataManager struct{}

func InitGroupMetadataManager() error {
	var err error
	once.Do(func() {
		err = initSchemas()
		Gmm = &groupMetadataManager{}
	})
	return err
}

func (gmm *groupMetadataManager) ReadMessageKey(buffer *buffer2.ByteBuffer) (common.BaseKey, error) {
	if buffer == nil {
		return nil, fmt.Errorf("read buffer is null")
	}

	version, key, err := gmm.readBuffer(buffer, common.MessageKey)
	if err != nil {
		return nil, err
	}

	if version <= CurrentOffsetKeySchemaVersion {
		return gmm.readOffsetKey(version, key)
	} else if version == CurrentGroupKeySchemaVersion {
		return gmm.readGroupMetaDataKey(version, key)
	} else {
		return nil, fmt.Errorf("unknown group metadata message version: %v", version)
	}
}

func (gmm *groupMetadataManager) ReadOffsetMessageValue(buffer *buffer2.ByteBuffer) (*common.OffsetAndMetadata, error) {
	if buffer == nil {
		return nil, fmt.Errorf("read buffer is null")
	}

	version, value, err := gmm.readBuffer(buffer, common.OffsetValue)
	if err != nil {
		return nil, err
	}

	if version == 0 {
		return gmm.readOffsetMessageValueV0(value)
	} else if version == 1 {
		return gmm.readOffsetMessageValueV1(value)
	} else if version == 2 {
		return gmm.readOffsetMessageValueV2(value)
	} else if version == 3 {
		return gmm.readOffsetMessageValueV3(value)
	} else {
		return nil, fmt.Errorf("unknown offset message version: %v", version)
	}
}

func (gmm *groupMetadataManager) ReadGroupMessageValue(groupID string, buffer *buffer2.ByteBuffer) (*common.GroupMetadata, error) {
	if buffer == nil {
		return nil, fmt.Errorf("read buffer is null")
	}

	version, value, err := gmm.readBuffer(buffer, common.GroupValue)
	if err != nil {
		return nil, err
	}

	if version >= 0 && version <= 3 {
		return gmm.readGroupMessageValue(groupID, value, version)
	} else {
		return nil, fmt.Errorf("unknown group metadata message version: %v", version)
	}
}

func (gmm *groupMetadataManager) readBuffer(buffer *buffer2.ByteBuffer, bt common.BufferType) (version int16, data interface{}, err error) {
	if version, err = buffer.GetInt16(); err != nil {
		err = fmt.Errorf("get version failed: %v", err)
		return
	}

	var schema *kafkaschema.Schema
	switch bt {
	case common.MessageKey:
		schema, err = gmm.schemaForKey(int(version))
	case common.OffsetValue:
		schema, err = gmm.schemaForOffsetValue(int(version))
	case common.GroupValue:
		schema, err = gmm.schemaForGroupValue(int(version))
	}
	if err != nil {
		return
	}

	data, err = schema.Read(buffer)
	return
}

func (gmm *groupMetadataManager) schemaForKey(version int) (*kafkaschema.Schema, error) {
	schema, ok := MessageTypeSchemas[version]
	if !ok {
		return nil, fmt.Errorf("unknown message key schema version: %v", version)
	}
	return schema, nil
}

func (gmm *groupMetadataManager) schemaForOffsetValue(version int) (*kafkaschema.Schema, error) {
	schema, ok := OffsetValueSchemas[version]
	if !ok {
		return nil, fmt.Errorf("unknown offset schema version: %v", version)
	}
	return schema, nil
}

func (gmm *groupMetadataManager) schemaForGroupValue(version int) (*kafkaschema.Schema, error) {
	schema, ok := GroupValueSchemas[version]
	if !ok {
		return nil, fmt.Errorf("unknown group metadata version: %v", version)
	}
	return schema, nil
}

func (gmm *groupMetadataManager) readOffsetKey(version int16, key interface{}) (*common.OffsetKey, error) {
	if Struct, ok := key.(*kafkaschema.Struct); ok {
		group, err := Struct.GetStringByField(OffsetKeyGroupField)
		if err != nil {
			return nil, err
		}
		topic, err := Struct.GetStringByField(OffsetKeyTopicField)
		if err != nil {
			return nil, err
		}
		partition, err := Struct.GetIntByField(OffsetKeyPartitionField)
		if err != nil {
			return nil, err
		}
		return common.NewOffsetKey(version, common.NewGroupTopicPartition(group, common.NewTopicPartition(topic, partition))), nil
	}
	return nil, fmt.Errorf("offset key type conversion error")
}

func (gmm *groupMetadataManager) readGroupMetaDataKey(version int16, key interface{}) (*common.GroupMetadataKey, error) {
	if Struct, ok := key.(*kafkaschema.Struct); ok {
		group, err := Struct.GetStringByField(GroupKeyGroupField)
		if err != nil {
			return nil, err
		}
		return common.NewGroupMetadataKey(version, group), nil
	}
	return nil, fmt.Errorf("group meta data key type conversion error")
}

func (gmm *groupMetadataManager) readOffsetMessageValueV0(value interface{}) (*common.OffsetAndMetadata, error) {
	if Struct, ok := value.(*kafkaschema.Struct); ok {
		offset, err := Struct.GetInt64ByField(OffsetValueOffsetFieldV0)
		if err != nil {
			return nil, err
		}
		metadata, err := Struct.GetStringByField(OffsetValueMetadataFieldV0)
		if err != nil {
			return nil, err
		}
		timestamp, err := Struct.GetInt64ByField(OffsetValueTimestampFieldV0)
		if err != nil {
			return nil, err
		}
		return common.NewOffsetAndMetadata1(offset, metadata, timestamp), nil
	}
	return nil, fmt.Errorf("offset message value V0 type conversion error")
}

func (gmm *groupMetadataManager) readOffsetMessageValueV1(value interface{}) (*common.OffsetAndMetadata, error) {
	if Struct, ok := value.(*kafkaschema.Struct); ok {
		offset, err := Struct.GetInt64ByField(OffsetValueOffsetFieldV1)
		if err != nil {
			return nil, err
		}
		metadata, err := Struct.GetStringByField(OffsetValueMetadataFieldV1)
		if err != nil {
			return nil, err
		}
		commitTimestamp, err := Struct.GetInt64ByField(OffsetValueCommitTimestampFieldV1)
		if err != nil {
			return nil, err
		}
		expireTimestamp, err := Struct.GetInt64ByField(OffsetValueExpireTimestampFieldV1)
		if err != nil {
			return nil, err
		}

		if expireTimestamp == int64(-1) {
			return common.NewOffsetAndMetadata1(offset, metadata, commitTimestamp), nil
		} else {
			return common.NewOffsetAndMetadata2(offset, metadata, commitTimestamp, expireTimestamp), nil
		}
	}
	return nil, fmt.Errorf("offset message value V1 type conversion error")
}

func (gmm *groupMetadataManager) readOffsetMessageValueV2(value interface{}) (*common.OffsetAndMetadata, error) {
	if Struct, ok := value.(*kafkaschema.Struct); ok {
		offset, err := Struct.GetInt64ByField(OffsetValueOffsetFieldV2)
		if err != nil {
			return nil, err
		}
		metadata, err := Struct.GetStringByField(OffsetValueMetadataFieldV2)
		if err != nil {
			return nil, err
		}
		commitTimestamp, err := Struct.GetInt64ByField(OffsetValueCommitTimestampFieldV2)
		if err != nil {
			return nil, err
		}
		return common.NewOffsetAndMetadata1(offset, metadata, commitTimestamp), nil
	}
	return nil, fmt.Errorf("offset message value V2 type conversion error")
}

func (gmm *groupMetadataManager) readOffsetMessageValueV3(value interface{}) (*common.OffsetAndMetadata, error) {
	if Struct, ok := value.(*kafkaschema.Struct); ok {
		offset, err := Struct.GetInt64ByField(OffsetValueOffsetFieldV3)
		if err != nil {
			return nil, err
		}
		leaderEpoch, err := Struct.GetIntByField(OffsetValueLeaderEpochFieldV3)
		if err != nil {
			return nil, err
		}
		metadata, err := Struct.GetStringByField(OffsetValueMetadataFieldV3)
		if err != nil {
			return nil, err
		}
		commitTimestamp, err := Struct.GetInt64ByField(OffsetValueCommitTimestampFieldV3)
		if err != nil {
			return nil, err
		}

		if leaderEpoch < 0 {
			return common.NewOffsetAndMetadata3(offset, nil, metadata, commitTimestamp), nil
		} else {
			return common.NewOffsetAndMetadata3(offset, leaderEpoch, metadata, commitTimestamp), nil
		}
	}
	return nil, fmt.Errorf("offset message value V3 type conversion error")
}

func (gmm *groupMetadataManager) readGroupMessageValue(groupId string, value interface{}, version int16) (*common.GroupMetadata, error) {
	if Struct, ok := value.(*kafkaschema.Struct); ok {
		generationId, err := Struct.GetInt(GenerationKey)
		if err != nil {
			return nil, err
		}
		protocolType, err := Struct.GetString(ProtocolTypeKey)
		if err != nil {
			return nil, err
		}
		protocol, err := Struct.GetString(ProtocolKey)
		if err != nil {
			return nil, err
		}
		leaderId, err := Struct.GetString(LeaderKey)
		if err != nil {
			return nil, err
		}
		memberMetadataArray, err := Struct.GetArray(MembersKey)
		if err != nil {
			return nil, err
		}
		initialState := gmm.readInitialState(err, memberMetadataArray)
		currentStateTimestamp, err := gmm.readCurrentStateTimestamp(version, Struct)
		if err != nil {
			return nil, err
		}
		members, err := gmm.readMembers(version, groupId, protocol, protocolType, memberMetadataArray)
		if err != nil {
			return nil, err
		}
		return common.LoadGroup(
			groupId,
			initialState,
			generationId,
			protocolType,
			protocol,
			leaderId,
			currentStateTimestamp,
			members,
			time.Now().UnixNano()/1e6,
		), nil
	}
	return nil, fmt.Errorf("group message value type conversion error")
}

func (gmm *groupMetadataManager) readInitialState(err error, memberMetadataArray []interface{}) (initialState common.GroupState) {
	if err != nil || memberMetadataArray == nil {
		return common.Empty
	} else {
		return common.Stable
	}
}

func (gmm *groupMetadataManager) readCurrentStateTimestamp(version int16, Struct *kafkaschema.Struct) (timestamp int64, err error) {
	if version == 2 && Struct.HasField(CurrentStateTimestampKey) {
		timestamp, err = Struct.GetInt64(CurrentStateTimestampKey)
		return
	}
	return
}

func (gmm *groupMetadataManager) readMembers(version int16, groupId, protocol, protocolType string, memberMetadataArray []interface{}) ([]*common.MemberMetadata, error) {

	memberMetadataResult := make([]*common.MemberMetadata, 0)
	for _, memberMetadata := range memberMetadataArray {
		Struct, ok := memberMetadata.(*kafkaschema.Struct)
		if !ok {
			return nil, fmt.Errorf("member metadata conversion error")
		}

		memberId, err := Struct.GetString(MemberIdKey)
		if err != nil {
			return nil, err
		}
		clientId, err := Struct.GetString(ClientIdKey)
		if err != nil {
			return nil, err
		}
		groupInstanceId, err := gmm.readGroupInstanceId(version, Struct)
		if err != nil {
			return nil, err
		}
		clientHost, err := Struct.GetString(ClientHostKey)
		if err != nil {
			return nil, err
		}
		subscription, err := gmm.readSubscription(protocol, Struct)
		if err != nil {
			return nil, err
		}
		partitions, err := gmm.readAssignment(Struct)
		if err != nil {
			return nil, err
		}

		memberMetadataResult = append(memberMetadataResult, common.NewMemberMetadata(
			memberId,
			groupId,
			groupInstanceId,
			clientId,
			clientHost,
			protocolType,
			subscription,
			partitions))
	}
	return memberMetadataResult, nil
}

func (gmm *groupMetadataManager) readGroupInstanceId(version int16, Struct *kafkaschema.Struct) (groupInstanceId string, err error) {
	if version >= 3 {
		groupInstanceId, err = Struct.GetString(GroupInstanceIdKey)
		return
	}
	return
}

func (gmm *groupMetadataManager) readSubscription(protocol string, Struct *kafkaschema.Struct) (map[string][]string, error) {
	subscriptionBuffer, err := Struct.GetByteBuffer(SubscriptionKey)
	if err != nil {
		return nil, err
	}
	subscription, err := ConsumerProtocol().DeserializeSubscription(subscriptionBuffer)
	if err != nil {
		return nil, err
	}

	s := make(map[string][]string)
	s[protocol] = subscription.Topics()
	return s, nil
}

func (gmm *groupMetadataManager) readAssignment(Struct *kafkaschema.Struct) ([]*common.TopicPartition, error) {
	assignmentBuffer, err := Struct.GetByteBuffer(AssignmentKey)
	if err != nil {
		return nil, err
	}
	assignment, err := ConsumerProtocol().DeserializeAssignment(assignmentBuffer)
	if err != nil {
		return nil, err
	}

	return assignment.Partitions(), nil
}
