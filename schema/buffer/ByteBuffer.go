package buffer

import (
	"fmt"
)

type ByteBuffer struct {
	buffer   []byte
	mark     int
	position int
	limit    int
	capacity int
	offset   int
}

func NewByteBuffer(buf []byte) *ByteBuffer {
	return Wrap(buf)
}

func newByteBuffer(mark, pos, lim, cap, offset int, buf []byte) *ByteBuffer {
	return &ByteBuffer{
		buffer:   buf,
		mark:     mark,
		position: pos,
		limit:    lim,
		capacity: cap,
		offset:   offset,
	}
}

func Wrap(array []byte) *ByteBuffer {
	return wrap(array, 0, len(array))
}

func wrap(array []byte, offset, length int) *ByteBuffer {
	return newByteBuffer(-1, offset, offset+length, len(array), 0, array)
}

func (b *ByteBuffer) GetInt16() (int16, error) {
	index, err := b.nextGetIndex(2)
	if err != nil {
		return -1, err
	}
	return Bits.getInt16(b, b.ix(index)), nil
}

func (b *ByteBuffer) GetInt32() (int32, error) {
	index, err := b.nextGetIndex(4)
	if err != nil {
		return -1, fmt.Errorf(fmt.Sprintf("ByteBuffer get int32 failed: %s", err))
	}
	return Bits.getInt32(b, b.ix(index)), nil
}

func (b *ByteBuffer) GetInt64() (int64, error) {
	index, err := b.nextGetIndex(8)
	if err != nil {
		return -1, err
	}
	return Bits.getInt64(b, b.ix(index)), nil
}

func (b *ByteBuffer) GetString(offset, length int) (string, error) {
	array, err := b.array()
	if err != nil {
		return "", err
	}
	arrayOffset, err := b.arrayOffset()
	if err != nil {
		return "", err
	}
	start := arrayOffset + b.position + offset
	return string(array[start : start+length]), nil
}

func (b *ByteBuffer) Remaining() int {
	return b.limit - b.position
}

func (b *ByteBuffer) GetPosition() int {
	return b.position
}
func (b *ByteBuffer) SetPosition(newPosition int) error {
	if (newPosition > b.limit) || (newPosition < 0) {
		return fmt.Errorf("illegal argument exception")
	}
	b.position = newPosition
	if b.mark > b.limit {
		b.mark = -1
	}
	return nil
}

func (b *ByteBuffer) Slice() *ByteBuffer {
	return newByteBuffer(-1, 0, b.Remaining(), b.Remaining(), b.GetPosition()+b.offset, b.buffer)
}

func (b *ByteBuffer) SetLimit(newLimit int) error {
	if (newLimit > b.capacity) || (newLimit < 0) {
		return fmt.Errorf("illegal argument exception")
	}
	b.limit = newLimit
	if b.position > b.limit {
		b.position = b.limit
	}
	if b.mark > b.limit {
		b.mark = -1
	}
	return nil
}

func (b *ByteBuffer) nextGetIndex(nb int) (int, error) {
	if b.limit-b.position < nb {
		return -1, fmt.Errorf("buffer Underflow Exception")
	}
	p := b.position
	b.position = p + nb
	return p, nil
}

func (b *ByteBuffer) ix(i int) int {
	return i + b.offset
}

func (b *ByteBuffer) get(index int) byte {
	return b.buffer[index]
}

func (b *ByteBuffer) array() ([]byte, error) {
	if b.buffer == nil {
		return nil, fmt.Errorf("unsupported operation exception")
	}
	return b.buffer, nil
}

func (b *ByteBuffer) arrayOffset() (int, error) {
	if b.buffer == nil {
		return 0, fmt.Errorf("UnsupportedOperationException")
	}
	return b.offset, nil
}

func (b *ByteBuffer) PrintBuffer() {
	fmt.Printf("\n[")
	for _, bs := range b.buffer {
		fmt.Printf("%d, ", bs)
	}
	fmt.Printf("]\n")
}
