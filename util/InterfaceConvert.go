package util

import (
	"fmt"
	"reflect"
)

func Interface2Int16(inter interface{}) (int16, error) {
	if inter == nil {
		return 0, fmt.Errorf("param is empty")
	}
	if reflect.TypeOf(inter).Kind() != reflect.Int16 {
		return 0, fmt.Errorf("interface %v con not convert to int16", inter)
	}
	return inter.(int16), nil
}

func Interface2Int(inter interface{}) (int, error) {
	if inter == nil {
		return 0, fmt.Errorf("param is empty")
	}
	if reflect.TypeOf(inter).Kind() != reflect.Int32 {
		return 0, fmt.Errorf("interface %v con not convert to int", inter)
	}
	return int(inter.(int32)), nil
}

func Interface2Int64(inter interface{}) (int64, error) {
	if inter == nil {
		return 0, fmt.Errorf("param is empty")
	}
	if reflect.TypeOf(inter).Kind() != reflect.Int64 {
		return 0, fmt.Errorf("interface %v con not convert to int64", inter)
	}
	return inter.(int64), nil
}

func Interface2String(inter interface{}) (string, error) {
	if inter == nil {
		return "", fmt.Errorf("param is empty")
	}
	if reflect.TypeOf(inter).Kind() != reflect.String {
		return "", fmt.Errorf("interface %v con not convert to string", inter)
	}
	return inter.(string), nil
}
