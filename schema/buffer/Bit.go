package buffer

import "sync"

var Bits = newBits()
var once sync.Once

type bits struct{}

func newBits() *bits {
	var b *bits
	once.Do(func() {
		b = &bits{}
	})
	return b
}

func (b *bits) getInt16(buf *ByteBuffer, index int) int16 {
	return b.makeInt16(buf.get(index), buf.get(index+1))
}

func (b *bits) getInt32(buf *ByteBuffer, index int) int32 {
	return b.makeInt32(
		buf.get(index),
		buf.get(index+1),
		buf.get(index+2),
		buf.get(index+3))
}

func (b *bits) getInt64(buf *ByteBuffer, index int) int64 {
	return b.makeInt64(
		buf.get(index),
		buf.get(index+1),
		buf.get(index+2),
		buf.get(index+3),
		buf.get(index+4),
		buf.get(index+5),
		buf.get(index+6),
		buf.get(index+7))
}

func (b *bits) makeInt16(b1, b0 byte) int16 {
	return int16((uint16(b1) << 8) | uint16(b0&0xff))
}

func (b *bits) makeInt32(b3, b2, b1, b0 byte) int32 {
	ub3 := uint32(b3)
	ub2 := uint32(b2)
	ub1 := uint32(b1)
	ub0 := uint32(b0)

	return int32(
		(ub3 << 24) |
			(ub2 & 0xff << 16) |
			(ub1 & 0xff << 8) |
			(ub0 & 0xff))
}

func (b *bits) makeInt64(b7, b6, b5, b4, b3, b2, b1, b0 byte) int64 {
	ub7 := uint64(b7)
	ub6 := uint64(b6)
	ub5 := uint64(b5)
	ub4 := uint64(b4)
	ub3 := uint64(b3)
	ub2 := uint64(b2)
	ub1 := uint64(b1)
	ub0 := uint64(b0)
	return int64(
		(ub7 << 56) |
			(ub6 & 0xff << 48) |
			(ub5 & 0xff << 40) |
			(ub4 & 0xff << 32) |
			(ub3 & 0xff << 24) |
			(ub2 & 0xff << 16) |
			(ub1 & 0xff << 8) |
			(ub0 & 0xff))
}
