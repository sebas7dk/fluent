package fluent

import (
	"fmt"
	"log"
	"strings"
	"sync"
)

const (
	whereClause        = "WHERE"
	andClause          = "AND"
	orClause           = "OR"
	isNullClause       = "IS NULL"
	isNotNullClause    = "IS NOT NULL"
	selectStatement    = "SELECT %s FROM %s"
	insertStatement    = "INSERT INTO %s (%s) VALUES (%s) RETURNING id"
	updateStatement    = "UPDATE %s SET"
	joinStatement      = " INNER JOIN %s ON %s = %s"
	leftJoinStatement  = " LEFT JOIN %s ON %s = %s"
	whereStatement     = " %s %s %s $%d"
	whereNullStatement = " %s %s %s"
	groupByStatement   = " GROUP BY %s"
	orderByStatement   = " ORDER BY %s"
	limitStatement     = " LIMIT $%d"
	offsetStatement    = " OFFSET $%d"
)

type query struct {
	stmt             string
	columns          []string
	table            string
	join, leftJoin   [][]interface{}
	where, whereNull [][]interface{}
	orderBy, groupBy []string
	limit, offset    int
	args             []interface{}
	argCounter       int
	debug            bool
	mutex            *sync.RWMutex
}

func newQuery() *query {
	return &query{
		argCounter: 1,
		mutex:      &sync.RWMutex{},
	}
}

func (q *query) log() {
	if q.debug {
		log.Println(q.stmt, q.args)
	}
}

type queryOption func(q *query)

func (q *query) builder(options ...queryOption) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	for _, option := range options {
		option(q)
	}
}

func buildInsert(cols []string, args []interface{}) queryOption {
	return func(q *query) {
		q.columns = cols
		q.args = args

		vals := []string{}
		for i := 1; i <= len(q.args); i++ {
			vals = append(vals, fmt.Sprintf("$%d", i))
		}

		q.stmt = fmt.Sprintf(insertStatement, q.table, strings.Join(q.columns, ","), strings.Join(vals, ","))
	}
}

func buildUpdate(cols []string, args []interface{}) queryOption {
	return func(q *query) {
		q.columns = cols
		q.args = args

		stmt := fmt.Sprintf(updateStatement, q.table)
		for _, col := range q.columns {
			stmt += fmt.Sprintf(" %s = $%d,", col, q.argCounter)
			q.argCounter++
		}
		// Remove the last comma
		q.stmt = stmt[:len(stmt)-1]
	}
}

func buildSelect() queryOption {
	return func(q *query) {
		q.stmt = fmt.Sprintf(selectStatement, strings.Join(q.columns, ","), q.table)
	}
}

func buildWhere() queryOption {
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

			stmtType = whereClause
			if strings.Contains(q.stmt, whereClause) {
				stmtType = andClause
			}

			q.stmt += fmt.Sprintf(whereStatement, stmtType, column, operator, q.argCounter)
			q.argCounter++
		}
	}
}

func buildWhereNull() queryOption {
	return func(q *query) {
		if len(q.whereNull) == 0 {
			return
		}

		var stmtType string
		for _, where := range q.whereNull {
			if len(where) != 2 {
				continue
			}

			col := where[0].(string)
			isNull := where[1].(bool)

			stmtType = whereClause
			if strings.Contains(q.stmt, whereClause) {
				stmtType = andClause
			}

			var nullStmt = isNotNullClause
			if isNull {
				nullStmt = isNullClause
			}

			q.stmt += fmt.Sprintf(whereNullStatement, stmtType, col, nullStmt)
		}
	}
}

func buildJoin() queryOption {
	return func(q *query) {
		for _, join := range q.join {
			if join != nil && len(join) == 3 {
				q.stmt += fmt.Sprintf(joinStatement, join[0], join[1], join[2])
			}
		}
	}
}

func buildLeftJoin() queryOption {
	return func(q *query) {
		for _, join := range q.leftJoin {
			if join != nil && len(join) == 3 {
				q.stmt += fmt.Sprintf(leftJoinStatement, join[0], join[1], join[2])
			}
		}
	}
}

func buildGroupBy() queryOption {
	return func(q *query) {
		if q.groupBy != nil {
			q.stmt += fmt.Sprintf(groupByStatement, strings.Join(q.groupBy, ","))
		}
	}
}

func buildOrderBy() queryOption {
	return func(q *query) {
		if q.orderBy != nil {
			q.stmt += fmt.Sprintf(orderByStatement, strings.Join(q.orderBy, ","))
		}
	}
}

func buildLimit() queryOption {
	return func(q *query) {
		if q.limit > 0 {
			q.args = append(q.args, q.limit)
			q.stmt += fmt.Sprintf(limitStatement, q.argCounter)
			q.argCounter++
		}
	}
}

func buildOffset() queryOption {
	return func(q *query) {
		q.args = append(q.args, q.offset)
		q.stmt += fmt.Sprintf(offsetStatement, q.argCounter)
		q.argCounter++
	}
}

func setDebug(s bool) queryOption {
	return func(q *query) {
		q.debug = s
	}
}

func setTable(t string) queryOption {
	return func(q *query) {
		q.table = t
	}
}

func setColumns(c []string) queryOption {
	return func(q *query) {
		q.columns = c
	}
}

func setWhere(w []interface{}) queryOption {
	return func(q *query) {
		q.where = append(q.where, w)
	}
}

func setWhereNull(wn []interface{}) queryOption {
	return func(q *query) {
		q.whereNull = append(q.whereNull, wn)
	}
}

func setJoin(j []interface{}) queryOption {
	return func(q *query) {
		q.join = append(q.join, j)
	}
}

func setLeftJoin(lj []interface{}) queryOption {
	return func(q *query) {
		q.leftJoin = append(q.leftJoin, lj)
	}
}

func setGroupBy(gb []string) queryOption {
	return func(q *query) {
		q.groupBy = gb
	}
}

func setOrderBy(ob []string) queryOption {
	return func(q *query) {
		q.orderBy = ob
	}
}

func setLimit(l int) queryOption {
	return func(q *query) {
		q.limit = l
	}
}

func setOffset(o int) queryOption {
	return func(q *query) {
		q.offset = o
	}
}
