package fluent

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type scanTest struct {
	*Inherit  `sql:"inherit"`
	ID        int       `sql:"id"`
	Name      string    `sql:"name"`
	Total     float64   `sql:"total"`
	IsActive  bool      `sql:"is_active"`
	CreatedAt time.Time `sql:"created_at"`
}

type Inherit struct {
	RowCount int `sql:"row_count"`
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
			expected: int(7),
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
				"row_count":  2,
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
				"row_count":  5,
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
				"row_count":  0,
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
				"row_count":  2,
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
				"row_count":  2,
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
				"row_count":  2,
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
				"row_count":  2,
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
			require.Equal(tc.sqlResults["row_count"].(int), tc.testStruct.RowCount)

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
					"row_count":  2,
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
					"row_count":  2,
				},
				{
					"id":         2,
					"name":       "henry",
					"total":      0.00,
					"created_at": timestamp,
					"is_active":  false,
					"row_count":  2,
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
					"row_count":  2,
				},
				{
					"id":         2,
					"name":       "henry",
					"total":      0.00,
					"created_at": timestamp,
					"is_active":  false,
					"row_count":  2,
				},
				{
					"id":         3,
					"name":       "marcel",
					"total":      "",
					"created_at": timestamp,
					"is_active":  false,
					"row_count":  2,
				},
			},
			expectedErr: true,
			expected:    2,
		},
		{
			sqlResults:  []map[string]interface{}{},
			expectedErr: true,
		},
	}

	for _, tc := range tests {
		for i, r := range tc.sqlResults {
			err := scanStructSlice(&tc.testStruct, r)
			if err == nil {
				require.Equal(r["id"].(int), tc.testStruct[i].ID)
				require.Equal(r["name"].(string), tc.testStruct[i].Name)
				require.Equal(r["total"].(float64), tc.testStruct[i].Total)
				require.Equal(r["is_active"].(bool), tc.testStruct[i].IsActive)
				require.Equal(r["row_count"].(int), tc.testStruct[i].RowCount)

				if v, _ := tc.sqlResults[i]["created_at"]; v != nil {
					require.Equal(timestamp, tc.testStruct[i].CreatedAt)
				}
			}
		}
		require.Equal(tc.expected, len(tc.testStruct))
	}

	var validMap = map[string]interface{}{"id": 1}

	err := scanStructSlice([]scanTest{}, map[string]interface{}{})
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

func Test_GetStructValues(t *testing.T) {
	require := require.New(t)

	timestamp := time.Now()

	tests := []struct {
		testStruct   scanTest
		expectedCols []string
		expectedArgs []interface{}
	}{
		{
			testStruct: scanTest{
				Name: "gerald",
			},
			expectedCols: []string{"name"},
			expectedArgs: []interface{}{"gerald"},
		},
		{
			testStruct: scanTest{
				ID:        1,
				Name:      "gerald",
				Total:     12.00,
				CreatedAt: timestamp,
			},
			expectedCols: []string{"id", "name", "total", "created_at"},
			expectedArgs: []interface{}{1, "gerald", 12.00, timestamp},
		},
		{
			expectedCols: []string{},
			expectedArgs: []interface{}{},
		},
	}

	for _, tc := range tests {
		cols, args, _ := getStructValues(tc.testStruct)

		for i, col := range cols {
			require.Equal(tc.expectedCols[i], col)
			require.Equal(tc.expectedArgs[i], args[i])
		}
	}
}
