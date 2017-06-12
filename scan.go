package fluent

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

const scannerTag = "sql"

type scanner struct {
	value interface{}
}

type one struct{}
type all struct{}

type scannerType interface {
	scan(s interface{}, vals map[string]interface{}) error
}

func (o *one) scan(s interface{}, vals map[string]interface{}) error {
	return scanStruct(s, vals)
}

func (a *all) scan(s interface{}, vals map[string]interface{}) error {
	return scanStructSlice(s, vals)
}

func (sc *scanner) Scan(val interface{}) error {
	switch val.(type) {
	case []byte:
		// Strings and floats come back as []uint8
		if v, ok := val.([]uint8); ok {
			val = string(v)
			if f, err := strconv.ParseFloat(val.(string), 64); err == nil {
				val = f
			}
		}
	}
	sc.value = val
	return nil
}

func scanStruct(s interface{}, vals map[string]interface{}) error {
	valsLen := len(vals)
	if valsLen == 0 {
		return fmt.Errorf("The values map shouldn't be empty")
	}

	valOf := reflect.Indirect(reflect.ValueOf(s))
	if valOf.Kind() != reflect.Struct {
		return fmt.Errorf("The provided interface is not a struct")
	}

	for i := 0; i < valOf.Type().NumField(); i++ {
		// Get the field
		field := valOf.Field(i)

		// Get the tags associated with the field
		tag := valOf.Type().Field(i).Tag.Get(scannerTag)
		if len(strings.TrimSpace(tag)) == 0 {
			// Skip empty tags
			continue
		}
		fieldName := valOf.Type().Field(i).Name

		if !field.CanSet() {
			return fmt.Errorf("Can't set the value for field: %s", fieldName)
		}

		if val := vals[tag]; val != nil {
			// fmt.Println(reflect.TypeOf(val).String())
			// if field.Type() != reflect.TypeOf(val) {
			// 	return fmt.Errorf("The field %s and the value are not of the same type", fieldName)
			// }

			if err := setFieldValue(field, val); err != nil {
				return fmt.Errorf("Field %s: %s", fieldName, err)
			}
		}
	}

	return nil
}

func setFieldValue(field reflect.Value, v interface{}) error {
	switch field.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		val, ok := v.(int64)
		if !ok {
			return fmt.Errorf("unable to set the integer value")
		}
		field.SetInt(val)
	case reflect.Float32, reflect.Float64:
		val, ok := v.(float64)
		if !ok {
			return fmt.Errorf("unable to set the float value")
		}
		field.SetFloat(val)
	case reflect.String:
		val, ok := v.(string)
		if !ok {
			return fmt.Errorf("unable to set the string value")
		}
		field.SetString(val)
	case reflect.Bool:
		val, ok := v.(bool)
		if !ok {
			return fmt.Errorf("unable to set the bool value")
		}
		field.SetBool(val)
	default:
		field.Set(reflect.ValueOf(v))
	}

	return nil
}

func scanStructSlice(s interface{}, vals map[string]interface{}) error {
	if s == nil {
		return fmt.Errorf("The slice shouldn't be empty")
	}

	if len(vals) == 0 {
		return fmt.Errorf("The values map shouldn't be empty")
	}
	if reflect.TypeOf(s).Kind() != reflect.Ptr {
		return fmt.Errorf("The provided type is not a pointer")
	}

	valOf := reflect.ValueOf(s).Elem()
	if valOf.Kind() != reflect.Slice {
		return fmt.Errorf("The provided value is not a slice")
	}

	// New slice pointer to write to
	ptr := reflect.New(valOf.Type().Elem()).Interface()
	if err := scanStruct(ptr, vals); err != nil {
		return err
	}

	ptrVal := reflect.ValueOf(ptr).Elem()
	valOf.Set(reflect.Append(valOf, ptrVal))

	return nil
}
