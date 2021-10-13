package kafkaschema

import (
	"fmt"
	"kafka_schema/schema/buffer"
)

type Schema struct {
	fields       []*BoundField
	fieldsByName map[string]*BoundField
	Type
}

func NewSchema(fs ...*Field) (*Schema, error) {
	schema := &Schema{}
	err := schema.init(fs...)
	return schema, err
}

func (sch *Schema) init(fs ...*Field) error {
	sch.fields = make([]*BoundField, len(fs))
	sch.fieldsByName = make(map[string]*BoundField)
	for i := 0; i < len(sch.fields); i++ {
		def := fs[i]
		if _, ok := sch.fieldsByName[def.name]; ok {
			return fmt.Errorf("schema contains a duplicate field: %s", def.name)
		}
		sch.fields[i] = NewBoundField(def, sch, i)
		sch.fieldsByName[def.name] = sch.fields[i]
	}
	return nil
}

func (sch *Schema) Read(buffer *buffer.ByteBuffer) (interface{}, error) {
	objects := make([]interface{}, len(sch.fields))
	for i := 0; i < len(sch.fields); i++ {
		obj, err := sch.fields[i].def.t.Read(buffer)
		if err != nil {
			return nil, fmt.Errorf("error reading field '%s': %v", sch.fields[i].def.name, err)
		}
		objects[i] = obj
	}

	return NewStruct(sch, objects), nil
}

func (sch *Schema) SizeOf(o interface{}) (int, error) {
	r := o.(Struct)
	var size int
	for _, field := range sch.fields {
		f, err := r.GetField(field)
		if err != nil {
			return 0, fmt.Errorf("error computing size for field '%s': %v", field.def.name, err)
		}
		s, err := field.def.t.SizeOf(f)
		if err != nil {
			return 0, fmt.Errorf("error computing size for field '%s': %v", field.def.name, err)
		}
		size += s
	}
	return size, nil
}

func (sch *Schema) Validate(o interface{}) (interface{}, error) {
	r := o.(*Struct)
	for _, field := range sch.fields {
		f, err := r.GetField(field)
		if err != nil {
			return nil, fmt.Errorf("invalid value for field '%s': %v", field.def.name, err)
		}
		_, err = field.def.t.Validate(f)
		if err != nil {
			return nil, fmt.Errorf("invalid value for field '%s': %v", field.def.name, err)
		}
	}
	return r, nil
}

func (sch *Schema) isNullable() bool {
	return false
}

func (sch *Schema) String() string {
	return fmt.Sprintf("schema with fields by name: %v", sch.fieldsByName)
}

func (sch *Schema) Get(name string) (*BoundField, error) {
	if v, ok := sch.fieldsByName[name]; ok {
		return v, nil
	} else {
		return nil, fmt.Errorf("invalid field name: %s", name)
	}
}
