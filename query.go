package fluent

import (
	"fmt"
	"strings"
)

const (
	andStatement     = " AND = $%d"
	whereStatement   = " WHERE = $%d"
	groupByStatement = " GROUP BY %s"
	orderByStatement = " ORDER BY %s"
	limitStatement   = " LIMIT %s"
	offsetStatement  = " OFFSET %s"
)

type query struct {
	stmt    string
	columns []string
	from    string
	where   map[string]interface{}
	orderBy []string
	groupBy []string
	limit   string
	offset  string
	args    []interface{}
}

func (q *query) buildSelect(cols []string) string {
	stmt := fmt.Sprintf("SELECT %s FROM %s", strings.Join(cols, ","), q.from)
	stmt += q.whereStatement(stmt)
	stmt += q.groupByStatement(stmt)
	stmt += q.orderByStatement(stmt)
	stmt += q.offsetStatement(stmt)
	stmt += q.limitStatement(stmt)

	return stmt
}

func (q *query) whereStatement(stmt string) string {
	var (
		count    = 1
		stmtType string
	)
	for column, value := range q.where {
		q.args = append(q.args, value)

		stmtType = whereStatement
		if count > 1 {
			stmtType = andStatement
		}

		stmt += fmt.Sprintf(stmtType, column, count)
		count++
	}

	return stmt
}

func (q *query) groupByStatement(stmt string) string {
	if q.groupBy != nil {
		stmt += fmt.Sprintf(groupByStatement, strings.Join(q.groupBy, ","))
	}
	return stmt
}

func (q *query) orderByStatement(stmt string) string {
	if q.groupBy != nil {
		stmt += fmt.Sprintf(orderByStatement, strings.Join(q.orderBy, ","))
	}
	return stmt
}

func (q *query) limitStatement(stmt string) string {
	if len(strings.TrimSpace(q.limit)) != 0 {
		stmt += fmt.Sprintf(limitStatement, q.limit)
	}
	return stmt
}

func (q *query) offsetStatement(stmt string) string {
	if len(strings.TrimSpace(q.offset)) != 0 {
		stmt += fmt.Sprintf(offsetStatement, q.limit)
	}
	return stmt
}

func (q *query) From(table string) *query {
	q.from = table
	return q
}

func (q *query) Where(column string, value interface{}) *query {
	q.where[column] = value
	return q
}

func (q *query) OrderBy(columns []string) *query {
	q.orderBy = columns
	return q
}

func (q *query) GroupBy(columns []string) *query {
	q.groupBy = columns
	return q
}

func (q *query) Limit(limit string) *query {
	q.limit = limit
	return q
}

func (q *query) Offset(offset string) *query {
	q.offset = offset
	return q
}

// func (m *Model) Update() {

// }

// func (m *Model) Insert() {

// }
// func (m *Model) Set(key, value string) {

// }

// func (m *Model) Get(columns []string) *Model {
// 	m.query.queryType = singleQueryType
// 	m.query.columns = columns
// 	m.query.stmt = m.query.buildSelect(columns)
// 	return m
// }

// func (m *Model) Scan(s interface{}) error {
// 	prepare, err := m.db.Prepare(m.query.stmt)
// 	if err != nil {
// 		return err
// 	}
// 	defer prepare.Close()

// 	rows, err := prepare.Query(m.query.args...)
// 	if err != nil {
// 		return err
// 	}

// 	var i int
// 	result := make(map[string]interface{}, len(m.query.columns))
// 	for rows.Next() {
// 		row := make([]interface{}, len(m.query.columns))
// 		for i := range m.query.columns {
// 			row[i] = &Scanner{}
// 		}

// 		if err := rows.Scan(row...); err != nil {
// 			return err
// 		}

// 		for i, column := range m.query.columns {
// 			scanner := row[i].(*Scanner)
// 			result[column] = scanner.value
// 		}

// 		scanStruct(s, result, i)
// 	}

// 	return nil
// }

// func (m *Model) All(s []interface{}) error {
// 	prepare, err := m.db.Prepare(m.query.stmt)
// 	if err != nil {
// 		return err
// 	}
// 	defer prepare.Close()

// 	rows, err := prepare.Query(m.query.args...)
// 	if err != nil {
// 		return err
// 	}

// 	result := make(map[string]interface{}, len(m.query.columns))
// 	var i int
// 	for rows.Next() {
// 		row := make([]interface{}, len(m.query.columns))
// 		for i := range m.query.columns {
// 			row[i] = &Scanner{}
// 		}

// 		if err := rows.Scan(row...); err != nil {
// 			return err
// 		}

// 		for i, column := range m.query.columns {
// 			scanner := row[i].(*Scanner)
// 			result[column] = scanner.value
// 		}

// 		scanStruct(s, result, i)
// 	}

// 	return nil
// }
