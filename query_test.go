package fluent

import (
	"testing"

	"time"

	"github.com/stretchr/testify/require"
)

func Test_Where(t *testing.T) {
	require := require.New(t)

	f := &Fluent{}
	timestamp := time.Now()

	tests := []struct {
		where              [][]interface{}
		expectedArgs       []interface{}
		expectedArgCounter int
		expectedStmt       string
	}{
		{
			where: [][]interface{}{
				{"id", "=", 1},
			},
			expectedArgs:       []interface{}{1},
			expectedArgCounter: 2,
			expectedStmt:       " WHERE id = $1",
		},
		{
			where: [][]interface{}{
				{"id", "=", 1},
				{"name", "=", "gerald"},
			},
			expectedArgs:       []interface{}{1, "gerald"},
			expectedArgCounter: 3,
			expectedStmt:       " WHERE id = $1 AND name = $2",
		},
		{
			where: [][]interface{}{
				{"id", "=", 1},
				{"created_at", ">", timestamp},
			},
			expectedArgs:       []interface{}{1, timestamp},
			expectedArgCounter: 3,
			expectedStmt:       " WHERE id = $1 AND created_at > $2",
		},
		{
			where:              [][]interface{}{},
			expectedArgs:       nil,
			expectedArgCounter: 1,
			expectedStmt:       "",
		},
	}

	for _, tc := range tests {
		f.query = newQuery()

		for _, where := range tc.where {
			f.Where(where[0].(string), where[1].(string), where[2])
		}

		require.Equal(len(tc.where), len(f.query.where))

		f.query.buildquery(setWhere())
		require.Equal(tc.expectedStmt, f.query.stmt)
		require.Equal(tc.expectedArgs, f.query.args)
		require.Equal(tc.expectedArgCounter, f.query.argCounter)
	}
}

func Test_Join(t *testing.T) {
	require := require.New(t)

	f := &Fluent{}

	tests := []struct {
		join         []string
		expectedStmt string
	}{
		{
			join:         []string{"test", "user.id", "test.user_id"},
			expectedStmt: " INNER JOIN test ON user.id = test.user_id",
		},
		{
			join:         []string{"test", "user.id"},
			expectedStmt: "",
		},
	}

	for _, tc := range tests {
		f.query = newQuery()
		f.Join(tc.join)

		f.query.buildquery(setJoin())
		require.Equal(tc.expectedStmt, f.query.stmt)
	}
}

func Test_LeftJoin(t *testing.T) {
	require := require.New(t)

	f := &Fluent{}

	tests := []struct {
		leftJoin     []string
		expectedStmt string
	}{
		{
			leftJoin:     []string{"test", "user.id", "test.user_id"},
			expectedStmt: " LEFT JOIN test ON user.id = test.user_id",
		},
		{
			leftJoin:     []string{"test", "user.id"},
			expectedStmt: "",
		},
	}

	for _, tc := range tests {
		f.query = newQuery()
		f.LeftJoin(tc.leftJoin)

		f.query.buildquery(setLeftJoin())
		require.Equal(tc.expectedStmt, f.query.stmt)
	}
}

func Test_OrderBy(t *testing.T) {
	require := require.New(t)

	f := &Fluent{}

	tests := []struct {
		orderBy      []string
		expectedStmt string
	}{
		{
			orderBy:      []string{"id", "name"},
			expectedStmt: " ORDER BY id,name",
		},
		{
			orderBy:      nil,
			expectedStmt: "",
		},
	}

	for _, tc := range tests {
		f.query = newQuery()
		f.OrderBy(tc.orderBy)

		f.query.buildquery(setOrderBy())
		require.Equal(tc.expectedStmt, f.query.stmt)
	}
}

func Test_GroupBy(t *testing.T) {
	require := require.New(t)

	f := &Fluent{}

	tests := []struct {
		groupBy      []string
		expectedStmt string
	}{
		{
			groupBy:      []string{"id", "name"},
			expectedStmt: " GROUP BY id,name",
		},
		{
			groupBy:      nil,
			expectedStmt: "",
		},
	}

	for _, tc := range tests {
		f.query = newQuery()
		f.GroupBy(tc.groupBy)

		f.query.buildquery(setGroupBy())
		require.Equal(tc.expectedStmt, f.query.stmt)
	}
}

func Test_Limit(t *testing.T) {
	require := require.New(t)

	f := &Fluent{}

	tests := []struct {
		limit              int
		expectedArgs       []interface{}
		expectedArgCounter int
		expectedStmt       string
	}{
		{
			limit:              1,
			expectedArgs:       []interface{}{1},
			expectedArgCounter: 2,
			expectedStmt:       " LIMIT $1",
		},
		{
			expectedArgs:       []interface{}{0},
			expectedArgCounter: 2,
			expectedStmt:       " LIMIT $1",
		},
	}

	for _, tc := range tests {
		f.query = newQuery()
		f.Limit(tc.limit)

		f.query.buildquery(setLimit())
		require.Equal(tc.expectedStmt, f.query.stmt)
		require.Equal(tc.expectedArgs, f.query.args)
		require.Equal(tc.expectedArgCounter, f.query.argCounter)
	}
}

func Test_BuildSelect(t *testing.T) {
	require := require.New(t)

	f := &Fluent{}

	tests := []struct {
		table              string
		cols               []string
		where              []interface{}
		groupBy            []string
		orderBy            []string
		limit              int
		offset             int
		expectedStmt       string
		expectedArgs       []interface{}
		expectedArgCounter int
	}{
		{
			table:              "test",
			cols:               []string{"id", "name", "total", "created_at", "is_active"},
			where:              []interface{}{"id", "=", 1},
			groupBy:            []string{"name"},
			orderBy:            []string{"id"},
			offset:             0,
			limit:              5,
			expectedStmt:       "SELECT id,name,total,created_at,is_active FROM test GROUP BY name ORDER BY id WHERE id = $1 OFFSET $2 LIMIT $3",
			expectedArgs:       []interface{}{1, 0, 5},
			expectedArgCounter: 4,
		},
		{
			table:              "test",
			cols:               []string{"*"},
			where:              []interface{}{"total", "=", 12.00},
			groupBy:            []string{"is_active"},
			orderBy:            []string{"total"},
			offset:             5,
			limit:              10,
			expectedStmt:       "SELECT * FROM test GROUP BY is_active ORDER BY total WHERE total = $1 OFFSET $2 LIMIT $3",
			expectedArgs:       []interface{}{12.00, 5, 10},
			expectedArgCounter: 4,
		},
		{
			table:              "test",
			cols:               []string{"*"},
			offset:             0,
			limit:              5,
			expectedStmt:       "SELECT * FROM test OFFSET $1 LIMIT $2",
			expectedArgs:       []interface{}{0, 5},
			expectedArgCounter: 3,
		},
	}

	for _, tc := range tests {
		f.query = newQuery()

		f.Table(tc.table).
			GroupBy(tc.groupBy).
			OrderBy(tc.orderBy)

		if tc.where != nil {
			f.Where(tc.where[0].(string), tc.where[1].(string), tc.where[2])
		}

		f.Offset(tc.offset).
			Limit(tc.limit).
			Get(tc.cols)

		require.Equal(tc.expectedStmt, f.query.stmt)
		require.Equal(tc.expectedArgs, f.query.args)
		require.Equal(tc.expectedArgCounter, f.query.argCounter)

	}
}

func Test_Insert(t *testing.T) {
	require := require.New(t)

	f := &Fluent{}

	tests := []struct {
		table        string
		cols         []string
		args         []interface{}
		expectedStmt string
	}{
		{
			table:        "test",
			cols:         []string{"name", "total"},
			args:         []interface{}{"gerald", 12.00},
			expectedStmt: "INSERT INTO test (name,total) VALUES ($1,$2)",
		},
		{
			table:        "test",
			cols:         []string{"name", "total", "is_active"},
			args:         []interface{}{"gerald", 12.00, 1},
			expectedStmt: "INSERT INTO test (name,total,is_active) VALUES ($1,$2,$3)",
		},
	}

	for _, tc := range tests {
		f.query = newQuery()

		f.Table(tc.table).Insert(tc.cols, tc.args)
		require.Equal(tc.expectedStmt, f.query.stmt)
	}
}

func Test_Update(t *testing.T) {
	require := require.New(t)

	f := &Fluent{}

	tests := []struct {
		table              string
		values             map[string]interface{}
		where              []interface{}
		expectedStmt       string
		expectedArgs       []interface{}
		expectedArgCounter int
	}{
		{
			table: "test",
			values: map[string]interface{}{
				"name":  "gerarld",
				"total": 12.00,
			},
			expectedArgs:       []interface{}{"gerald", 12.00},
			expectedStmt:       "UPDATE test SET name = $1, total = $2",
			expectedArgCounter: 3,
		},
		{
			table: "test",
			values: map[string]interface{}{
				"name":  "gerarld",
				"total": 12.00,
			},
			where:              []interface{}{"id", "=", 1},
			expectedArgs:       []interface{}{"gerald", 12.00, 1},
			expectedStmt:       "UPDATE test SET name = $1, total = $2 WHERE id = $3",
			expectedArgCounter: 3,
		},
	}

	for _, tc := range tests {
		f.query = newQuery()

		f.Table(tc.table)
		if tc.where != nil {
			f.Where(tc.where[0].(string), tc.where[1].(string), tc.where[2])
		}

		f.query.buildquery(
			setWhere(),
			setUpdate(),
		)
		require.Equal(tc.expectedStmt, f.query.stmt)
	}
}
