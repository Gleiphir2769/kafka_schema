package kafkaschema

type Field struct {
	name            string
	t               Type
	docString       string
	hasDefaultValue bool
	defaultValue    interface{}
}

func NewField(name string, t Type) *Field {
	return newField(name, t, "", false, nil)
}

func NewField1(name string, t Type, docString string, defaultValue interface{}) *Field {
	return newField(name, t, docString, false, defaultValue)
}

func newField(name string, t Type, docString string, hasDefaultValue bool, defaultValue interface{}) *Field {
	field := &Field{
		name:            name,
		t:               t,
		docString:       docString,
		hasDefaultValue: hasDefaultValue,
		defaultValue:    defaultValue,
	}
	//if hasDefaultValue {
	//	_, err := field.t.Validate(defaultValue)
	//	return nil, err
	//}
	return field
}
