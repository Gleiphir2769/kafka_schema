package common

type Int interface{}

type OffsetAndMetadata struct {
	Offset          int64
	leaderEpoch     Int
	MetaData        string
	CommitTimestamp int64
	ExpireTimestamp int64
}

func NewOffsetAndMetadata1(offset int64, metadata string, commitTimestamp int64) *OffsetAndMetadata {
	return &OffsetAndMetadata{
		Offset:          offset,
		leaderEpoch:     nil,
		MetaData:        metadata,
		CommitTimestamp: commitTimestamp,
		ExpireTimestamp: 0,
	}
}

func NewOffsetAndMetadata2(offset int64, metadata string, commitTimestamp int64, expireTimestamp int64) *OffsetAndMetadata {
	return &OffsetAndMetadata{
		Offset:          offset,
		leaderEpoch:     nil,
		MetaData:        metadata,
		CommitTimestamp: commitTimestamp,
		ExpireTimestamp: expireTimestamp,
	}
}

func NewOffsetAndMetadata3(offset int64, leaderEpoch Int, metadata string, commitTimestamp int64) *OffsetAndMetadata {
	return &OffsetAndMetadata{
		Offset:          offset,
		leaderEpoch:     leaderEpoch,
		MetaData:        metadata,
		CommitTimestamp: commitTimestamp,
		ExpireTimestamp: 0,
	}
}
