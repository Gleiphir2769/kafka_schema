package common

type BufferType int

const (
	MessageKey BufferType = iota
	OffsetValue
	GroupValue
)
