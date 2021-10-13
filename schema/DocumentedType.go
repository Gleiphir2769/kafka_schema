package kafkaschema

import (
	"fmt"
	"kafka_schema/schema/buffer"
	"strconv"
)

var (
	INT16          = new(i16)
	INT32          = new(i32)
	INT64          = new(i64)
	STRING         = new(s)
	NullableString = new(nullableString)
	BYTES          = &bytes{}
	NullableBytes  = &nullableBytes{}
)

type DocumentedType interface {
	Type
	TypeName() string
}

type i16 int

func (i i16) Read(buffer *buffer.ByteBuffer) (interface{}, error) {
	return buffer.GetInt16()
}

func (i i16) SizeOf(interface{}) (int, error) {
	return 2, nil
}

func (i i16) TypeName() string {
	return "INT16"
}

func (i i16) Validate(o interface{}) (interface{}, error) {
	if s, ok := o.(i16); ok {
		return s, nil
	} else {
		return nil, fmt.Errorf("%v is not a INT16", o)
	}
}

func (i i16) isNullable() bool {
	return false
}

func (i i16) String() string {
	return strconv.FormatInt(int64(i), 10)
}

type i32 int

func (i i32) Read(buffer *buffer.ByteBuffer) (interface{}, error) {
	return buffer.GetInt32()
}

func (i i32) SizeOf(interface{}) (int, error) {
	return 4, nil
}

func (i i32) TypeName() string {
	return "INT32"
}

func (i i32) Validate(o interface{}) (interface{}, error) {
	if s, ok := o.(int32); ok {
		return s, nil
	} else {
		return nil, fmt.Errorf("%v is not a int", o)
	}
}

func (i i32) isNullable() bool {
	return false
}

func (i i32) String() string {
	return string(rune(i))
}

type i64 int64

func (i i64) Read(buffer *buffer.ByteBuffer) (interface{}, error) {
	return buffer.GetInt64()
}

func (i i64) SizeOf(interface{}) (int, error) {
	return 8, nil
}

func (i i64) TypeName() string {
	return "INT64"
}

func (i i64) Validate(o interface{}) (interface{}, error) {
	if s, ok := o.(i64); ok {
		return s, nil
	} else {
		return nil, fmt.Errorf("%v is not a INT64", o)
	}
}

func (i i64) isNullable() bool {
	return false
}

func (i i64) String() string {
	return strconv.FormatInt(int64(i), 10)
}

type bytes struct{}

func (b bytes) Read(buffer *buffer.ByteBuffer) (interface{}, error) {
	size, err := buffer.GetInt32()
	if err != nil {
		return nil, fmt.Errorf("bytes length read faild: %v", err)
	}
	if size < 0 {
		return nil, fmt.Errorf("bytes size %v cannot be negative", size)
	}
	if size > int32(buffer.Remaining()) {
		return nil, fmt.Errorf("error reading bytes of size %d, only %d bytes available", size, buffer.Remaining())
	}
	slice := buffer.Slice()
	_ = slice.SetLimit(int(size))
	_ = buffer.SetPosition(buffer.GetPosition() + int(size))
	return slice, nil
}

func (b bytes) SizeOf(o interface{}) (int, error) {
	switch o.(type) {
	case *buffer.ByteBuffer:
		byteBuffer := o.(*buffer.ByteBuffer)
		return 4 + byteBuffer.Remaining(), nil
	default:
		return 0, fmt.Errorf("the type is not a ByteBuffer")
	}
}

func (b bytes) String() string {
	return ""
}

func (b bytes) isNullable() bool {
	return false
}

func (b bytes) Validate(o interface{}) (interface{}, error) {
	switch o.(type) {
	case *buffer.ByteBuffer:
		byteBuffer := o.(*buffer.ByteBuffer)
		return byteBuffer, nil
	default:
		return nil, fmt.Errorf("the type is not a ByteBuffer")
	}
}

func (b bytes) TypeName() string {
	return "BYTES"
}

type nullableBytes struct{}

func (b nullableBytes) Read(buffer *buffer.ByteBuffer) (interface{}, error) {
	size, err := buffer.GetInt32()
	if err != nil {
		return nil, fmt.Errorf("bytes length read faild: %v", err)
	}
	if size < 0 {
		return nil, nil
	}
	if size > int32(buffer.Remaining()) {
		return nil, fmt.Errorf("error reading bytes of size %d, only %d bytes available", size, buffer.Remaining())
	}
	slice := buffer.Slice()
	_ = slice.SetLimit(int(size))
	_ = buffer.SetPosition(buffer.GetPosition() + int(size))
	return slice, nil
}

func (b nullableBytes) SizeOf(o interface{}) (int, error) {
	if o == nil {
		return 4, nil
	}
	switch o.(type) {
	case *buffer.ByteBuffer:
		byteBuffer := o.(*buffer.ByteBuffer)
		return 4 + byteBuffer.Remaining(), nil
	default:
		return 0, fmt.Errorf("the type is not a ByteBuffer")
	}
}

func (b nullableBytes) String() string {
	return ""
}

func (b nullableBytes) isNullable() bool {
	return true
}

func (b nullableBytes) Validate(o interface{}) (interface{}, error) {
	if o == nil {
		return nil, nil
	}

	switch o.(type) {
	case *buffer.ByteBuffer:
		byteBuffer := o.(*buffer.ByteBuffer)
		return byteBuffer, nil
	default:
		return nil, fmt.Errorf("the type is not a ByteBuffer")
	}
}

func (b nullableBytes) TypeName() string {
	return "NULLABLE_BYTES"
}

type s string

func (s s) Read(buffer *buffer.ByteBuffer) (interface{}, error) {
	// length is the bytes length of the string
	length, err := buffer.GetInt16()
	if err != nil {
		return nil, fmt.Errorf("string length read faild: %v", err)
	}
	if length < 0 {
		return nil, fmt.Errorf("string length %d cannot be negative", length)
	}
	if length > int16(buffer.Remaining()) {
		return nil, fmt.Errorf("error reading string of length %d, only %d bytes available", length, buffer.Remaining())
	}
	str, err := buffer.GetString(0, int(length))
	err = buffer.SetPosition(buffer.GetPosition() + int(length))
	return str, err
}

func (s s) SizeOf(o interface{}) (int, error) {
	return 2 + len(o.(string)), nil
}

func (s s) String() string {
	return string(s)
}

func (s s) isNullable() bool {
	return false
}

func (s s) Validate(o interface{}) (interface{}, error) {
	if s, ok := o.(string); ok {
		return s, nil
	} else {
		return nil, fmt.Errorf("%v is not a s", o)
	}
}

func (s s) TypeName() string {
	return "STRING"
}

type nullableString string

func (s nullableString) Read(buffer *buffer.ByteBuffer) (interface{}, error) {
	// length is the bytes length of the string
	length, err := buffer.GetInt16()
	if err != nil {
		return nil, fmt.Errorf("string length read faild: %v", err)
	}
	if length < 0 {
		return "", nil
	}
	if length > int16(buffer.Remaining()) {
		return nil, fmt.Errorf("error reading string of length %d, only %d bytes available", length, buffer.Remaining())
	}
	str, err := buffer.GetString(0, int(length))
	err = buffer.SetPosition(buffer.GetPosition() + int(length))
	return str, err
}

func (s nullableString) SizeOf(o interface{}) (int, error) {
	if o == nil {
		return 2, nil
	}

	return 2 + len(o.(string)), nil
}

func (s nullableString) String() string {
	return string(s)
}

func (s nullableString) isNullable() bool {
	return true
}

func (s nullableString) Validate(o interface{}) (interface{}, error) {
	if o == nil {
		return nil, nil
	}

	if s, ok := o.(string); ok {
		return s, nil
	} else {
		return nil, fmt.Errorf("%v is not a s", o)
	}
}

func (s nullableString) TypeName() string {
	return "NULLABLE_STRING"
}

type arrayOf struct {
	t        Type
	nullable bool
}

func NewArrayOf(t Type) *arrayOf {
	return NewArrayOf1(t, false)
}

func NewArrayOf1(t Type, nullable bool) *arrayOf {
	return &arrayOf{t: t, nullable: nullable}
}

func (a arrayOf) Read(buffer *buffer.ByteBuffer) (interface{}, error) {
	size, err := buffer.GetInt32()
	if err != nil {
		return nil, fmt.Errorf("arrayOf read buffer failed: %s", err)
	}
	if size < 0 && a.isNullable() {
		return nil, nil
	} else if size < 0 {
		return nil, fmt.Errorf("array size %v cannot be negative", size)
	}

	if int(size) > buffer.Remaining() {
		return nil, fmt.Errorf(fmt.Sprintf("Error reading array of size '%d', only '%d' bytes available", size, buffer.Remaining()))
	}
	objs := make([]interface{}, size)
	for i := range objs {
		buff, err := a.t.Read(buffer)
		if err != nil {
			return nil, err
		}
		objs[i] = buff
	}
	return objs, nil
}

func (a arrayOf) SizeOf(o interface{}) (int, error) {
	size := 4
	if o == nil {
		return size, nil
	}

	array := o.([]interface{})
	for _, obj := range array {
		objSize, _ := a.t.SizeOf(obj)
		size = size + objSize
	}
	return size, nil
}

func (a arrayOf) String() string {
	return fmt.Sprintf("ARRAY(%s)", a.t.String())
}

func (a arrayOf) isNullable() bool {
	return a.nullable
}

func (a arrayOf) Validate(o interface{}) (interface{}, error) {
	if a.isNullable() && o == nil {
		return nil, nil
	}

	array := o.([]interface{})
	var err error
	for _, obj := range array {
		_, err = a.t.Validate(obj)
	}
	return array, err
}

func (a arrayOf) TypeName() string {
	return "ARRAY"
}
