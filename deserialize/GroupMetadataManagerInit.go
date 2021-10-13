package deserialize

import (
	"fmt"
	"kafka_schema/schema"

)

const (
	ProtocolTypeKey          = "protocol_type"
	GenerationKey            = "generation"
	ProtocolKey              = "protocol"
	LeaderKey                = "leader"
	CurrentStateTimestampKey = "current_state_timestamp"
	MembersKey               = "members"

	MemberIdKey         = "member_id"
	GroupInstanceIdKey  = "group_instance_id"
	ClientIdKey         = "client_id"
	ClientHostKey       = "client_host"
	RebalanceTimeoutKey = "rebalance_timeout"
	SessionTimeoutKey   = "session_timeout"
	SubscriptionKey     = "subscription"
	AssignmentKey       = "assignment"
)

var (
	MessageTypeSchemas map[int]*kafkaschema.Schema
	OffsetValueSchemas map[int]*kafkaschema.Schema
	GroupValueSchemas  map[int]*kafkaschema.Schema

	OffsetCommitKeySchema   *kafkaschema.Schema
	OffsetKeyGroupField     *kafkaschema.BoundField
	OffsetKeyTopicField     *kafkaschema.BoundField
	OffsetKeyPartitionField *kafkaschema.BoundField

	GroupMetadataKeySchema *kafkaschema.Schema
	GroupKeyGroupField     *kafkaschema.BoundField

	OffsetCommitValueSchemaV0   *kafkaschema.Schema
	OffsetValueOffsetFieldV0    *kafkaschema.BoundField
	OffsetValueMetadataFieldV0  *kafkaschema.BoundField
	OffsetValueTimestampFieldV0 *kafkaschema.BoundField

	OffsetCommitValueSchemaV1         *kafkaschema.Schema
	OffsetValueOffsetFieldV1          *kafkaschema.BoundField
	OffsetValueMetadataFieldV1        *kafkaschema.BoundField
	OffsetValueCommitTimestampFieldV1 *kafkaschema.BoundField
	OffsetValueExpireTimestampFieldV1 *kafkaschema.BoundField

	OffsetCommitValueSchemaV2         *kafkaschema.Schema
	OffsetValueOffsetFieldV2          *kafkaschema.BoundField
	OffsetValueMetadataFieldV2        *kafkaschema.BoundField
	OffsetValueCommitTimestampFieldV2 *kafkaschema.BoundField

	OffsetCommitValueSchemaV3         *kafkaschema.Schema
	OffsetValueOffsetFieldV3          *kafkaschema.BoundField
	OffsetValueLeaderEpochFieldV3     *kafkaschema.BoundField
	OffsetValueMetadataFieldV3        *kafkaschema.BoundField
	OffsetValueCommitTimestampFieldV3 *kafkaschema.BoundField

	GroupMetadataValueSchemaV0 *kafkaschema.Schema
	GroupMetadataValueSchemaV1 *kafkaschema.Schema
	GroupMetadataValueSchemaV2 *kafkaschema.Schema
	GroupMetadataValueSchemaV3 *kafkaschema.Schema

	MemberMetadataV0 *kafkaschema.Schema
	MemberMetadataV1 *kafkaschema.Schema
	MemberMetadataV2 *kafkaschema.Schema
	MemberMetadataV3 *kafkaschema.Schema
)

func initSchemas() (err error) {
	err = initKeySchemas()
	if err != nil {
		return
	}
	err = initOffsetValueSchemas()
	if err != nil {
		return
	}
	err = InitConsumerProtocol()
	if err != nil {
		return
	}
	err = initGroupValueSchemas()
	return
}

func initKeySchemas() (err error) {
	// offset key
	if OffsetCommitKeySchema, err = kafkaschema.NewSchema(
		kafkaschema.NewField("group", kafkaschema.STRING),
		kafkaschema.NewField("topic", kafkaschema.STRING),
		kafkaschema.NewField("partition", kafkaschema.INT32),
	); err != nil {
		return fmt.Errorf("initKeySchemas %s", err)
	}

	if OffsetKeyGroupField, err = OffsetCommitKeySchema.Get("group"); err != nil {
		return fmt.Errorf("initKeySchemas %s", err)
	}
	if OffsetKeyTopicField, err = OffsetCommitKeySchema.Get("topic"); err != nil {
		return fmt.Errorf("initKeySchemas %s", err)
	}
	if OffsetKeyPartitionField, err = OffsetCommitKeySchema.Get("partition"); err != nil {
		return fmt.Errorf("initKeySchemas %s", err)
	}

	// group metadata key
	if GroupMetadataKeySchema, err = kafkaschema.NewSchema(
		kafkaschema.NewField("group", kafkaschema.STRING),
	); err != nil {
		return fmt.Errorf("initKeySchemas %s", err)
	}

	if GroupKeyGroupField, err = GroupMetadataKeySchema.Get("group"); err != nil {
		return fmt.Errorf("initKeySchemas %s", err)
	}

	MessageTypeSchemas = make(map[int]*kafkaschema.Schema)
	MessageTypeSchemas[0] = OffsetCommitKeySchema
	MessageTypeSchemas[1] = OffsetCommitKeySchema
	MessageTypeSchemas[2] = GroupMetadataKeySchema
	return
}

func initOffsetValueSchemas() (err error) {
	if err = initOffsetValueSchemasV0(); err != nil {
		return
	}
	if err = initOffsetValueSchemasV1(); err != nil {
		return
	}
	if err = initOffsetValueSchemasV2(); err != nil {
		return
	}
	if err = initOffsetValueSchemasV3(); err != nil {
		return
	}

	OffsetValueSchemas = make(map[int]*kafkaschema.Schema)
	OffsetValueSchemas[0] = OffsetCommitValueSchemaV0
	OffsetValueSchemas[1] = OffsetCommitValueSchemaV1
	OffsetValueSchemas[2] = OffsetCommitValueSchemaV2
	OffsetValueSchemas[3] = OffsetCommitValueSchemaV3
	return
}

func initOffsetValueSchemasV0() (err error) {
	if OffsetCommitValueSchemaV0, err = kafkaschema.NewSchema(
		kafkaschema.NewField("offset", kafkaschema.INT64),
		kafkaschema.NewField1("metadata", kafkaschema.STRING, "Associated metadata.", ""),
		kafkaschema.NewField("timestamp", kafkaschema.INT64),
	); err != nil {
		return fmt.Errorf("initOffsetValueSchemas %s", err)
	}
	if OffsetValueOffsetFieldV0, err = OffsetCommitValueSchemaV0.Get("offset"); err != nil {
		return fmt.Errorf("initOffsetValueSchemas %s", err)
	}
	if OffsetValueMetadataFieldV0, err = OffsetCommitValueSchemaV0.Get("metadata"); err != nil {
		return fmt.Errorf("initOffsetValueSchemas %s", err)
	}
	if OffsetValueTimestampFieldV0, err = OffsetCommitValueSchemaV0.Get("timestamp"); err != nil {
		return fmt.Errorf("initOffsetValueSchemas %s", err)
	}
	return
}

func initOffsetValueSchemasV1() (err error) {
	if OffsetCommitValueSchemaV1, err = kafkaschema.NewSchema(
		kafkaschema.NewField("offset", kafkaschema.INT64),
		kafkaschema.NewField1("metadata", kafkaschema.STRING, "Associated metadata.", ""),
		kafkaschema.NewField("commit_timestamp", kafkaschema.INT64),
		kafkaschema.NewField("expire_timestamp", kafkaschema.INT64),
	); err != nil {
		return fmt.Errorf("initOffsetValueSchemas %s", err)
	}
	if OffsetValueOffsetFieldV1, err = OffsetCommitValueSchemaV1.Get("offset"); err != nil {
		return fmt.Errorf("initOffsetValueSchemas %s", err)
	}
	if OffsetValueMetadataFieldV1, err = OffsetCommitValueSchemaV1.Get("metadata"); err != nil {
		return fmt.Errorf("initOffsetValueSchemas %s", err)
	}
	if OffsetValueCommitTimestampFieldV1, err = OffsetCommitValueSchemaV1.Get("commit_timestamp"); err != nil {
		return fmt.Errorf("initOffsetValueSchemas %s", err)
	}
	if OffsetValueExpireTimestampFieldV1, err = OffsetCommitValueSchemaV1.Get("expire_timestamp"); err != nil {
		return fmt.Errorf("initOffsetValueSchemas %s", err)
	}
	return
}

func initOffsetValueSchemasV2() (err error) {
	if OffsetCommitValueSchemaV2, err = kafkaschema.NewSchema(
		kafkaschema.NewField("offset", kafkaschema.INT64),
		kafkaschema.NewField1("metadata", kafkaschema.STRING, "Associated metadata.", ""),
		kafkaschema.NewField("commit_timestamp", kafkaschema.INT64),
	); err != nil {
		return fmt.Errorf("initOffsetValueSchemas %s", err)
	}
	if OffsetValueOffsetFieldV2, err = OffsetCommitValueSchemaV2.Get("offset"); err != nil {
		return fmt.Errorf("initOffsetValueSchemas %s", err)
	}
	if OffsetValueMetadataFieldV2, err = OffsetCommitValueSchemaV2.Get("metadata"); err != nil {
		return fmt.Errorf("initOffsetValueSchemas %s", err)
	}
	if OffsetValueCommitTimestampFieldV2, err = OffsetCommitValueSchemaV2.Get("commit_timestamp"); err != nil {
		return fmt.Errorf("initOffsetValueSchemas %s", err)
	}
	return
}

func initOffsetValueSchemasV3() (err error) {
	if OffsetCommitValueSchemaV3, err = kafkaschema.NewSchema(
		kafkaschema.NewField("offset", kafkaschema.INT64),
		kafkaschema.NewField("leader_epoch", kafkaschema.INT32),
		kafkaschema.NewField1("metadata", kafkaschema.STRING, "Associated metadata.", ""),
		kafkaschema.NewField("commit_timestamp", kafkaschema.INT64),
	); err != nil {
		return fmt.Errorf("initOffsetValueSchemas %s", err)
	}
	if OffsetValueOffsetFieldV3, err = OffsetCommitValueSchemaV3.Get("offset"); err != nil {
		return fmt.Errorf("initOffsetValueSchemas %s", err)
	}
	if OffsetValueLeaderEpochFieldV3, err = OffsetCommitValueSchemaV3.Get("leader_epoch"); err != nil {
		return fmt.Errorf("initOffsetValueSchemas %s", err)
	}
	if OffsetValueMetadataFieldV3, err = OffsetCommitValueSchemaV3.Get("metadata"); err != nil {
		return fmt.Errorf("initOffsetValueSchemas %s", err)
	}
	if OffsetValueCommitTimestampFieldV3, err = OffsetCommitValueSchemaV3.Get("commit_timestamp"); err != nil {
		return fmt.Errorf("initOffsetValueSchemas %s", err)
	}
	return
}

func initGroupValueSchemas() (err error) {
	if err = initMemberMetadataSchemas(); err != nil {
		return
	}
	if err = initGroupMetadataValueSchemas(); err != nil {
		return
	}

	GroupValueSchemas = make(map[int]*kafkaschema.Schema)
	GroupValueSchemas[0] = GroupMetadataValueSchemaV0
	GroupValueSchemas[1] = GroupMetadataValueSchemaV1
	GroupValueSchemas[2] = GroupMetadataValueSchemaV2
	GroupValueSchemas[3] = GroupMetadataValueSchemaV3
	return
}

func initGroupMetadataValueSchemas() (err error) {
	if GroupMetadataValueSchemaV0, err = kafkaschema.NewSchema(
		kafkaschema.NewField(ProtocolTypeKey, kafkaschema.STRING),
		kafkaschema.NewField(GenerationKey, kafkaschema.INT32),
		kafkaschema.NewField(ProtocolKey, kafkaschema.NullableString),
		kafkaschema.NewField(LeaderKey, kafkaschema.NullableString),
		kafkaschema.NewField(MembersKey, kafkaschema.NewArrayOf(MemberMetadataV0)),
	); err != nil {
		return fmt.Errorf("initGroupMetadataValueSchemas %s", err)
	}
	if GroupMetadataValueSchemaV1, err = kafkaschema.NewSchema(
		kafkaschema.NewField(ProtocolTypeKey, kafkaschema.STRING),
		kafkaschema.NewField(GenerationKey, kafkaschema.INT32),
		kafkaschema.NewField(ProtocolKey, kafkaschema.NullableString),
		kafkaschema.NewField(LeaderKey, kafkaschema.NullableString),
		kafkaschema.NewField(MembersKey, kafkaschema.NewArrayOf(MemberMetadataV1)),
	); err != nil {
		return fmt.Errorf("initGroupMetadataValueSchemas %s", err)
	}
	if GroupMetadataValueSchemaV2, err = kafkaschema.NewSchema(
		kafkaschema.NewField(ProtocolTypeKey, kafkaschema.STRING),
		kafkaschema.NewField(GenerationKey, kafkaschema.INT32),
		kafkaschema.NewField(ProtocolKey, kafkaschema.NullableString),
		kafkaschema.NewField(LeaderKey, kafkaschema.NullableString),
		kafkaschema.NewField(CurrentStateTimestampKey, kafkaschema.INT64),
		kafkaschema.NewField(MembersKey, kafkaschema.NewArrayOf(MemberMetadataV2)),
	); err != nil {
		return fmt.Errorf("initGroupMetadataValueSchemas %s", err)
	}
	if GroupMetadataValueSchemaV3, err = kafkaschema.NewSchema(
		kafkaschema.NewField(ProtocolTypeKey, kafkaschema.STRING),
		kafkaschema.NewField(GenerationKey, kafkaschema.INT32),
		kafkaschema.NewField(ProtocolKey, kafkaschema.NullableString),
		kafkaschema.NewField(LeaderKey, kafkaschema.NullableString),
		kafkaschema.NewField(CurrentStateTimestampKey, kafkaschema.INT64),
		kafkaschema.NewField(MembersKey, kafkaschema.NewArrayOf(MemberMetadataV3)),
	); err != nil {
		return fmt.Errorf("initGroupMetadataValueSchemas %s", err)
	}
	return
}

func initMemberMetadataSchemas() (err error) {
	if MemberMetadataV0, err = kafkaschema.NewSchema(
		kafkaschema.NewField(MemberIdKey, kafkaschema.STRING),
		kafkaschema.NewField(ClientIdKey, kafkaschema.STRING),
		kafkaschema.NewField(ClientHostKey, kafkaschema.STRING),
		kafkaschema.NewField(SessionTimeoutKey, kafkaschema.INT32),
		kafkaschema.NewField(SubscriptionKey, kafkaschema.BYTES),
		kafkaschema.NewField(AssignmentKey, kafkaschema.BYTES),
	); err != nil {
		return fmt.Errorf("initMemberMetadataSchemas %s", err)
	}
	if MemberMetadataV1, err = kafkaschema.NewSchema(
		kafkaschema.NewField(MemberIdKey, kafkaschema.STRING),
		kafkaschema.NewField(ClientIdKey, kafkaschema.STRING),
		kafkaschema.NewField(ClientHostKey, kafkaschema.STRING),
		kafkaschema.NewField(RebalanceTimeoutKey, kafkaschema.INT32),
		kafkaschema.NewField(SessionTimeoutKey, kafkaschema.INT32),
		kafkaschema.NewField(SubscriptionKey, kafkaschema.BYTES),
		kafkaschema.NewField(AssignmentKey, kafkaschema.BYTES),
	); err != nil {
		return fmt.Errorf("initMemberMetadataSchemas %s", err)
	}
	MemberMetadataV2 = MemberMetadataV1
	if MemberMetadataV3, err = kafkaschema.NewSchema(
		kafkaschema.NewField(MemberIdKey, kafkaschema.STRING),
		kafkaschema.NewField(GroupInstanceIdKey, kafkaschema.NullableString),
		kafkaschema.NewField(ClientIdKey, kafkaschema.STRING),
		kafkaschema.NewField(ClientHostKey, kafkaschema.STRING),
		kafkaschema.NewField(SessionTimeoutKey, kafkaschema.INT32),
		kafkaschema.NewField(SubscriptionKey, kafkaschema.BYTES),
		kafkaschema.NewField(AssignmentKey, kafkaschema.BYTES),
	); err != nil {
		return fmt.Errorf("initMemberMetadataSchemas %s", err)
	}
	return
}
