package fluent

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type scanTest struct {
	ID        int       `sql:"id"`
	Name      string    `sql:"name"`
	Total     float64   `sql:"total"`
	IsActive  bool      `sql:"is_active"`
	CreatedAt time.Time `sql:"created_at"`
}

func Test_Scan(t *testing.T) {
	require := require.New(t)

	sc := &scanner{}

	tests := []struct {
		value    interface{}
		expected interface{}
	}{
		{
			value:    int(7),
			expected: int64(7),
		},
		{
			value:    []uint8("test"),
			expected: string("test"),
		},
		{
			value:    []uint8("12.00"),
			expected: float64(12.00),
		},
		{
			value:    false,
			expected: false,
		},
	}

	for _, tc := range tests {
		sc.Scan(tc.value)
		require.Equal(tc.expected, sc.value)
	}
}

func Test_ScanStruct(t *testing.T) {
	require := require.New(t)

	timestamp := time.Now()

	tests := []struct {
		testStruct  scanTest
		sqlResults  map[string]interface{}
		expectedErr bool
	}{
		{
			sqlResults: map[string]interface{}{
				"id":         1,
				"name":       "gerald",
				"total":      12.00,
				"created_at": timestamp,
				"is_active":  true,
			},
			expectedErr: false,
		},
		{
			sqlResults: map[string]interface{}{
				"id":         1,
				"name":       "",
				"total":      12.00,
				"created_at": timestamp,
				"is_active":  true,
			},
			expectedErr: false,
		},
		{
			sqlResults: map[string]interface{}{
				"id":         1,
				"name":       "gerald",
				"total":      12.00,
				"created_at": timestamp,
				"is_active":  true,
			},
			expectedErr: false,
		},
		{
			sqlResults: map[string]interface{}{
				"id":         1,
				"name":       "gerald",
				"total":      12.00,
				"created_at": nil,
				"is_active":  true,
			},
			expectedErr: false,
		},
		{
			sqlResults: map[string]interface{}{
				"id":         1,
				"name":       "gerald",
				"total":      12.00,
				"created_at": nil,
				"is_active":  false,
			},
			expectedErr: false,
		},
		{
			sqlResults: map[string]interface{}{
				"id":         "1",
				"name":       "gerald",
				"total":      12.00,
				"created_at": timestamp,
				"is_active":  true,
			},
			expectedErr: true,
		},
		{
			sqlResults: map[string]interface{}{
				"id":         1,
				"name":       "gerald",
				"total":      "",
				"created_at": timestamp,
				"is_active":  true,
			},
			expectedErr: true,
		},
		{
			sqlResults:  map[string]interface{}{},
			expectedErr: true,
		},
	}

	for _, tc := range tests {
		err := scanStruct(&tc.testStruct, tc.sqlResults)
		if tc.expectedErr {
			require.NotNil(err)
		} else {
			require.Nil(err)
			require.Equal(tc.sqlResults["id"].(int), tc.testStruct.ID)
			require.Equal(tc.sqlResults["name"].(string), tc.testStruct.Name)
			require.Equal(tc.sqlResults["total"].(float64), tc.testStruct.Total)
			require.Equal(tc.sqlResults["is_active"].(bool), tc.testStruct.IsActive)

			if v, _ := tc.sqlResults["created_at"]; v != nil {
				require.Equal(timestamp, tc.testStruct.CreatedAt)
			}
		}
	}

	var validMap = map[string]interface{}{"id": 1}

	err := scanStruct(&scanTest{}, map[string]interface{}{})
	require.NotNil(err)

	var ptrTest *scanTest
	err = scanStruct(ptrTest, validMap)
	require.NotNil(err)

	var nilTest interface{}
	err = scanStruct(nilTest, validMap)
	require.NotNil(err)
}

func Test_ScanStructSlice(t *testing.T) {
	require := require.New(t)

	timestamp := time.Now()

	tests := []struct {
		testStruct  []scanTest
		sqlResults  []map[string]interface{}
		expectedErr bool
		expected    int
	}{
		{
			sqlResults: []map[string]interface{}{
				{
					"id":         1,
					"name":       "gerald",
					"total":      12.00,
					"created_at": timestamp,
					"is_active":  true,
				},
			},
			expectedErr: false,
			expected:    1,
		},
		{
			sqlResults: []map[string]interface{}{
				{
					"id":         1,
					"name":       "gerald",
					"total":      12.00,
					"created_at": timestamp,
					"is_active":  true,
				},
				{
					"id":         2,
					"name":       "henry",
					"total":      0.00,
					"created_at": timestamp,
					"is_active":  false,
				},
			},
			expectedErr: false,
			expected:    2,
		},
		{
			sqlResults: []map[string]interface{}{
				{
					"id":         1,
					"name":       "gerald",
					"total":      12.00,
					"created_at": timestamp,
					"is_active":  true,
				},
				{
					"id":         2,
					"name":       "henry",
					"total":      0.00,
					"created_at": timestamp,
					"is_active":  false,
				},
				{
					"id":         3,
					"name":       "marcel",
					"total":      "",
					"created_at": timestamp,
					"is_active":  false,
				},
			},
			expectedErr: true,
		},
		{
			sqlResults:  []map[string]interface{}{},
			expectedErr: true,
		},
	}

	for _, tc := range tests {
		err := scanStructSlice(&tc.testStruct, tc.sqlResults)
		if tc.expectedErr {
			require.NotNil(err)
		} else {
			require.Nil(err)
			require.Equal(tc.expected, len(tc.testStruct))

			for i, r := range tc.sqlResults {
				require.Equal(r["id"].(int), tc.testStruct[i].ID)
				require.Equal(r["name"].(string), tc.testStruct[i].Name)
				require.Equal(r["total"].(float64), tc.testStruct[i].Total)
				require.Equal(r["is_active"].(bool), tc.testStruct[i].IsActive)

				if v, _ := tc.sqlResults[i]["created_at"]; v != nil {
					require.Equal(timestamp, tc.testStruct[i].CreatedAt)
				}
			}
		}
	}

	var validMap = []map[string]interface{}{{"id": 1}}

	err := scanStructSlice([]scanTest{}, []map[string]interface{}{})
	require.NotNil(err)

	var ptrTest []*scanTest
	err = scanStructSlice(ptrTest, validMap)
	require.NotNil(err)

	var noSliceTest *scanTest
	err = scanStructSlice(noSliceTest, validMap)
	require.NotNil(err)

	var nilTest interface{}
	err = scanStructSlice(nilTest, validMap)
	require.NotNil(err)
}
