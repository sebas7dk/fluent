package fluent

import (
	"fmt"
	"testing"
)

type test struct {
	ID   int    `sql:"id"`
	Name string `sql:"name"`
}

func Test_ScanStruct(t *testing.T) {
	s := &test{}

	m := map[string]interface{}{
		"id":   1,
		"name": "gerald",
	}

	scanStruct(s, m)

	fmt.Println(s)
}

func Test_ScanStructSlice(t *testing.T) {
	var s = []test{}
	tests := []map[string]interface{}{
		{
			"id":   1,
			"name": "gerald",
		},
		{
			"id":   2,
			"name": "mike",
		},
	}

	for _, tc := range tests {
		if err := scanStructSlice(&s, tc); err != nil {
			t.Fatal(err)
		}
	}

	fmt.Println(s)
}
