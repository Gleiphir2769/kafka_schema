package kafkaschema

import (
	"fmt"
	"kafka_schema/schema/buffer"
	"kafka_schema/util"
	"reflect"
)

type Struct struct {
	schema *Schema
	values []interface{}
}

func NewStruct(schema *Schema, values []interface{}) *Struct {
	return &Struct{schema: schema, values: values}
}

func (ks *Struct) GetField(field *BoundField) (interface{}, error) {
	err := ks.validateField(field)
	if err != nil {
		return nil, err
	}
	return ks.getFieldOrDefault(field)
}

func (ks *Struct) validateField(field *BoundField) error {
	if ks.schema != field.schema {
		return fmt.Errorf("attempt to access field '%s' from a different schema instance", field.def.name)
	}
	if field.index > len(ks.values) {
		return fmt.Errorf("invalid field index: %d", field.index)
	}
	return nil
}

func (ks *Struct) getFieldOrDefault(field *BoundField) (interface{}, error) {
	v := ks.values[field.index]
	if v != nil {
		return v, nil
	} else if field.def.hasDefaultValue {
		return field.def.defaultValue, nil
	} else if field.def.t.isNullable() {
		return nil, nil
	} else {
		return nil, fmt.Errorf("missing value for field '%s' which has no default value", field.def.name)
	}
}

func (ks Struct) GetString(name string) (string, error) {
	f, err := ks.get(name)
	if err != nil {
		return "", err
	}
	interface2String, err := util.Interface2String(f)
	return interface2String, err
}

func (ks Struct) GetStringByField(field *BoundField) (string, error) {
	f, err := ks.getByField(field)
	if err != nil {
		return "", err
	}
	return util.Interface2String(f)
}

func (ks Struct) GetInt16(name string) (int16, error) {
	f, err := ks.get(name)
	if err != nil {
		return 0, err
	}
	return util.Interface2Int16(f)
}

func (ks Struct) GetInt(name string) (int, error) {
	f, err := ks.get(name)
	if err != nil {
		return 0, err
	}
	return util.Interface2Int(f)
}

func (ks Struct) GetIntByField(field *BoundField) (int, error) {
	f, err := ks.getByField(field)
	if err != nil {
		return 0, err
	}
	return util.Interface2Int(f)
}

func (ks Struct) GetInt64(name string) (int64, error) {
	f, err := ks.get(name)
	if err != nil {
		return 0, err
	}
	return util.Interface2Int64(f)
}

func (ks Struct) GetInt64ByField(field *BoundField) (int64, error) {
	f, err := ks.getByField(field)
	if err != nil {
		return 0, err
	}
	return util.Interface2Int64(f)
}

func (ks Struct) GetByteBuffer(name string) (*buffer.ByteBuffer, error) {
	f, err := ks.get(name)
	if err != nil {
		return nil, err
	}
	if f != nil {
		return f.(*buffer.ByteBuffer), nil
	} else {
		return nil, nil
	}
}

func (ks Struct) getByField(field *BoundField) (interface{}, error) {
	err := ks.validateField(field)
	if err != nil {
		return nil, fmt.Errorf("kafkaStruct validate field faild: %v", err)
	}
	return ks.getFieldOrDefault(field)
}

func (ks Struct) get(name string) (interface{}, error) {
	boundField, err := ks.schema.Get(name)
	if err != nil || boundField == nil {
		return nil, fmt.Errorf("no such field:%s, %s", name, err)
	}
	return ks.getFieldOrDefault(boundField)
}

func (ks Struct) GetArray(name string) ([]interface{}, error) {
	f, err := ks.get(name)
	if err != nil {
		return nil, err
	}
	if f == nil {
		return nil, nil
	}

	if partition, ok := f.(int32); ok {
		result := make([]interface{}, 0)
		result = append(result, partition)
		return result, nil
	}

	if _, ok := f.([]interface{}); ok {
		return f.([]interface{}), nil
	} else {
		return nil, nil
	}
}

func (ks Struct) HasField(name string) bool {
	_, err := ks.schema.Get(name)
	return err != nil
}

func (ks Struct) GetBytes(name string) (*buffer.ByteBuffer, error) {
	f, err := ks.get(name)
	if err != nil {
		return nil, err
	}

	if reflect.TypeOf(f).Kind() != reflect.Slice || reflect.TypeOf(f).Kind() != reflect.Array {
		return f.(*buffer.ByteBuffer), nil
	}

	bs := []byte(fmt.Sprintf("%v", f))
	return buffer.NewByteBuffer(bs), err
}
