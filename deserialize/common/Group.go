package common

type GroupState int

const (
	PreparingRebalance = iota
	CompletingRebalance
	Stable
	Dead
	Empty
)

type GroupMetadata struct {
	groupID               string
	initialState          GroupState
	time                  int64
	generationID          int
	protocolType          string
	protocol              string
	leaderID              string
	currentStateTimestamp int64
	members               map[string]*MemberMetadata
}

func NewGroupMetadata(groupID string, initialState GroupState, time int64) *GroupMetadata {
	return &GroupMetadata{
		groupID:      groupID,
		initialState: initialState,
		time:         time,
	}
}

func LoadGroup(
	groupID string,
	initialState GroupState,
	generationID int,
	protocolType,
	protocol,
	leaderID string,
	currentStateTimestamp int64,
	members []*MemberMetadata,
	time int64,
) *GroupMetadata {
	groupMetadata := NewGroupMetadata(groupID, initialState, time)
	groupMetadata.generationID = generationID
	groupMetadata.protocolType = protocolType
	groupMetadata.protocol = protocol
	groupMetadata.leaderID = leaderID
	groupMetadata.currentStateTimestamp = currentStateTimestamp
	for _, member := range members {
		groupMetadata.add(member)
	}
	return groupMetadata
}

func (g *GroupMetadata) add(member *MemberMetadata) {
	if g.members == nil {
		g.members = make(map[string]*MemberMetadata)
	}
	if len(g.members) == 0 {
		g.protocolType = member.protocolType
	}

	if g.leaderID == "" {
		g.leaderID = member.memberID
	}

	g.members[member.memberID] = member
}

func (g *GroupMetadata) AllMemberMetadata() []*MemberMetadata {
	memberMetadataList := make([]*MemberMetadata, 0)
	for _, member := range g.members {
		memberMetadataList = append(memberMetadataList, member)
	}
	return memberMetadataList
}

type MemberMetadata struct {
	memberID           string
	groupID            string
	groupInstanceId    string
	clientID           string
	clientHost         string
	protocolType       string
	supportedProtocols map[string][]string
	topicPartitions    []*TopicPartition
}

func NewMemberMetadata(
	memberID,
	groupID,
	groupInstanceId,
	clientID,
	clientHost,
	protocolType string,
	supportedProtocols map[string][]string,
	topicPartitions []*TopicPartition,
) *MemberMetadata {
	return &MemberMetadata{
		memberID:           memberID,
		groupID:            groupID,
		groupInstanceId:    groupInstanceId,
		clientID:           clientID,
		clientHost:         clientHost,
		protocolType:       protocolType,
		supportedProtocols: supportedProtocols,
		topicPartitions:    topicPartitions,
	}
}

func (m *MemberMetadata) TopicPartitions() []*TopicPartition {
	return m.topicPartitions
}

func (m *MemberMetadata) MemberID() string {
	return m.memberID
}

func (m *MemberMetadata) ClientHost() string {
	return m.clientHost
}

