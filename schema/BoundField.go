package kafkaschema

import "fmt"

type BoundField struct {
	def    *Field
	index  int
	schema *Schema
}

func NewBoundField(def *Field, schema *Schema, index int) *BoundField {
	return &BoundField{
		def:    def,
		index:  index,
		schema: schema,
	}
}

func (bf *BoundField) String() string {
	return fmt.Sprintf("%s:%s", bf.def.name, bf.def.t.String())
}
