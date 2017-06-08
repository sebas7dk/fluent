package fluent

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

const fluentTag = "sql"

type Fluent struct {
	query *query
}

type query struct {
}

type scanner struct {
	value interface{}
}

func (s *scanner) Scan(v interface{}) error {
	var val = v
	switch v.(type) {
	case int:
		if i, ok := v.(int); ok {
			val = int64(i)
		}
	case []byte:
		if u, ok := v.([]uint8); ok {
			val = string(u)
			if f, err := strconv.ParseFloat(val.(string), 64); err == nil {
				val = f
			}
		}
	}
	return nil
}

// func (f *Fluent) Scan(s interface{}) error {

// 	columns, _ := rows.Columns()

// 	for rows.Next() {

// 		row := make([]interface{}, len(columns))
// 		for i := range columns {
// 			row[i] = &scanner{}
// 		}

// 		err := rows.Scan(row...)
// 		if err != nil {
// 			return err
// 		}

// 		for i, column := range columns {
// 			var scanner = row[i].(*scanner)
// 			fmt.Println(column, ":", scanner.value)
// 		}
// 	}
// }

func scanStruct(s interface{}, vals map[string]interface{}) error {
	valOf := reflect.Indirect(reflect.ValueOf(s))
	if valOf.Kind() != reflect.Struct {
		return fmt.Errorf("The provided interface is not a struct")
	}

	for i := 0; i < valOf.Type().NumField(); i++ {
		// Get the field
		field := valOf.Field(i)

		// Get the tags associated with the field
		tag := valOf.Type().Field(i).Tag.Get(fluentTag)
		if len(strings.TrimSpace(tag)) == 0 {
			// Skip empty tags
			continue
		}

		if v := vals[tag]; v != nil {
			if field.Kind() == reflect.Int {
				field.SetInt(int64(v.(int)))
			} else {
				field.Set(reflect.ValueOf(v))
			}
		}
	}

	return nil
}

func scanStructSlice(s interface{}, vals map[string]interface{}) error {
	if reflect.TypeOf(s).Kind() != reflect.Ptr {
		return fmt.Errorf("The provided type is not a pointer")
	}

	valOf := reflect.ValueOf(s).Elem()
	if valOf.Kind() != reflect.Slice {
		return fmt.Errorf("The provided value is not a slice")
	}
	//make a pointer of type contain in the slice (in this example Item)
	ptr := reflect.New(valOf.Type().Elem()).Interface()

	if err := scanStruct(ptr, vals); err != nil {
		return err
	}

	ptrVal := reflect.ValueOf(ptr).Elem()

	valOf.Set(reflect.Append(valOf, ptrVal))

	return nil
}
