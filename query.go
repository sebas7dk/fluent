package fluent

import (
	"fmt"
	"strings"
)

const (
	selectStatement   = "SELECT %s FROM %s"
	insertStatement   = "INSERT INTO %s (%s) VALUES (%s)"
	updateStatement   = "UPDATE %s SET"
	joinStatement     = " INNER JOIN %s ON %s = %s"
	leftJoinStatement = " LEFT JOIN %s ON %s = %s"
	andStatement      = " AND %s %s $%d"
	whereStatement    = " WHERE %s %s $%d"
	groupByStatement  = " GROUP BY %s"
	orderByStatement  = " ORDER BY %s"
	limitStatement    = " LIMIT $%d"
	offsetStatement   = " OFFSET $%d"
)

type query struct {
	stmt             string
	columns          []string
	table            string
	join, leftJoin   []string
	where            [][]interface{}
	orderBy, groupBy []string
	limit, offset    int
	args             []interface{}
	argCounter       int
}

func newQuery() *query {
	return &query{
		argCounter: 1,
	}
}

type queryOption func(q *query)

func (q *query) buildquery(options ...queryOption) {
	for _, option := range options {
		option(q)
	}
}

func setInsert() queryOption {
	return func(q *query) {
		vals := []string{}
		for i := 1; i <= len(q.args); i++ {
			vals = append(vals, fmt.Sprintf("$%d", i))
		}

		q.stmt = fmt.Sprintf(insertStatement, q.table, strings.Join(q.columns, ","), strings.Join(vals, ","))
	}
}

func setUpdate() queryOption {
	return func(q *query) {
		stmt := fmt.Sprintf(updateStatement, q.table)

		for _, col := range q.columns {
			stmt += fmt.Sprintf(" %s = $%d,", col, q.argCounter)
			q.argCounter++
		}
		// Remove the last comma
		q.stmt = stmt[:len(stmt)-1]
	}
}

func setSelect() queryOption {
	return func(q *query) {
		q.stmt = fmt.Sprintf(selectStatement, strings.Join(q.columns, ","), q.table)
	}
}

func setWhere() queryOption {
	return func(q *query) {
		if len(q.where) == 0 {
			return
		}

		var stmtType string
		for _, where := range q.where {
			if len(where) != 3 {
				continue
			}

			column := where[0].(string)
			operator := where[1].(string)
			arg := where[2]

			q.args = append(q.args, arg)

			stmtType = whereStatement
			if strings.Contains(q.stmt, "WHERE") {
				stmtType = andStatement
			}

			q.stmt += fmt.Sprintf(stmtType, column, operator, q.argCounter)
			q.argCounter++
		}
	}
}

func setJoin() queryOption {
	return func(q *query) {
		if q.join != nil && len(q.join) == 3 {
			q.stmt += fmt.Sprintf(joinStatement, q.join[0], q.join[1], q.join[2])
		}
	}
}

func setLeftJoin() queryOption {
	return func(q *query) {
		if q.leftJoin != nil && len(q.leftJoin) == 3 {
			q.stmt += fmt.Sprintf(leftJoinStatement, q.leftJoin[0], q.leftJoin[1], q.leftJoin[2])
		}
	}
}

func setGroupBy() queryOption {
	return func(q *query) {
		if q.groupBy != nil {
			q.stmt += fmt.Sprintf(groupByStatement, strings.Join(q.groupBy, ","))
		}
	}
}

func setOrderBy() queryOption {
	return func(q *query) {
		if q.orderBy != nil {
			q.stmt += fmt.Sprintf(orderByStatement, strings.Join(q.orderBy, ","))
		}
	}
}

func setLimit() queryOption {
	return func(q *query) {
		if q.limit > 0 {
			q.args = append(q.args, q.limit)
			q.stmt += fmt.Sprintf(limitStatement, q.argCounter)
			q.argCounter++
		}
	}
}

func setOffest() queryOption {
	return func(q *query) {
		q.args = append(q.args, q.offset)
		q.stmt += fmt.Sprintf(offsetStatement, q.argCounter)
		q.argCounter++
	}
}
