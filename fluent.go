package fluent

import "database/sql"

// Fluent is the struct that holds
// the database connection and
// the query information
type Fluent struct {
	db    *sql.DB
	query *query
}

// ORM is the interface for the Fluent ORM
type ORM interface {
	Table(table string) *Fluent
	Join(table, column1, column2 string) *Fluent
	LeftJoin(table, column1, column2 string) *Fluent
	Where(column, operator string, value interface{}) *Fluent
	OrderBy(columns []string) *Fluent
	GroupBy(columns []string) *Fluent
	Limit(limit int) *Fluent
	Get(columns []string) *Fluent
	One(s interface{}) error
	All(s interface{}) error
	Insert(s interface{}) error
	Update(s interface{}) error
	Raw(stmt string, args []interface{}) error
	Debug(status bool) *Fluent
}

// New set the DB connection
func New(db *sql.DB) ORM {
	return &Fluent{db, newQuery()}
}

// Table add the table name to the query
func (f *Fluent) Table(table string) *Fluent {
	f.query.table = table
	return f
}

// Join add the table and columns to the query
func (f *Fluent) Join(table, column1, column2 string) *Fluent {
	f.query.join = []interface{}{table, column1, column2}
	return f
}

// LeftJoin add the table and columns to the query
func (f *Fluent) LeftJoin(table, column1, column2 string) *Fluent {
	f.query.leftJoin = []interface{}{table, column1, column2}
	return f
}

// Where add the column, operator and the value to the query
func (f *Fluent) Where(column, operator string, value interface{}) *Fluent {
	where := []interface{}{column, operator, value}
	f.query.where = append(f.query.where, where)
	return f
}

// WhereNull add the null or not null column to the query
func (f *Fluent) WhereNull(column string, isNull bool) *Fluent {
	where := []interface{}{column, isNull}
	f.query.whereNull = append(f.query.whereNull, where)
	return f
}

// OrderBy add the order by columns tot the query
func (f *Fluent) OrderBy(columns []string) *Fluent {
	f.query.orderBy = columns
	return f
}

// GroupBy add the group by columns tot the query
func (f *Fluent) GroupBy(columns []string) *Fluent {
	f.query.groupBy = columns
	return f
}

// Limit add the limit to the query
func (f *Fluent) Limit(limit int) *Fluent {
	f.query.limit = limit
	return f
}

// Offset add the offset to the query
func (f *Fluent) Offset(offset int) *Fluent {
	f.query.offset = offset
	return f
}

// Get add the columns to select from and build the query
func (f *Fluent) Get(columns []string) *Fluent {
	f.query.columns = columns
	f.query.buildquery(
		setSelect(),
		setJoin(),
		setLeftJoin(),
		setGroupBy(),
		setOrderBy(),
		setWhere(),
		setWhereNull(),
		setOffest(),
		setLimit(),
	)
	return f
}

// Debug if set to true it will log the query
func (f *Fluent) Debug(status bool) *Fluent {
	f.query.debug = status
	return f
}

// Insert a record by building the query and scanning
// the values from the struct to insert
func (f *Fluent) Insert(s interface{}) error {
	cols, args, err := getStructValues(s)
	if err != nil {
		return err
	}

	f.query.buildquery(setInsert(cols, args))
	return f.execute()
}

// Update a record by building the query and scanning
// the values from the struct to update
func (f *Fluent) Update(s interface{}) error {
	cols, args, err := getStructValues(s)
	if err != nil {
		return err
	}

	f.query.buildquery(
		setUpdate(cols, args),
		setWhere(),
		setWhereNull(),
	)

	return f.execute()
}

// Raw exectutes the statement
func (f *Fluent) Raw(stmt string, args []interface{}) error {
	f.query.args = args
	f.query.stmt = stmt
	return f.execute()
}

// One fetch a single record
func (f *Fluent) One(s interface{}) error {
	st := &one{}
	return f.scan(s, st)
}

// All fetch all the records in the table
func (f *Fluent) All(s interface{}) error {
	st := &all{}
	return f.scan(s, st)
}

func (f *Fluent) execute() error {
	defer f.reset()

	prepare, err := f.db.Prepare(f.query.stmt)
	if err != nil {
		return err
	}
	defer prepare.Close()

	_, err = prepare.Exec(f.query.args...)
	return err
}

// scan prepares the statement and scans the values of each row
// into the provided struct or slice
func (f *Fluent) scan(s interface{}, st scannerType) error {
	defer f.reset()

	prepare, err := f.db.Prepare(f.query.stmt)
	if err != nil {
		return err
	}
	defer prepare.Close()

	rows, err := prepare.Query(f.query.args...)
	if err != nil {
		return err
	}
	columns, err := rows.Columns()
	if err != nil {
		return err
	}

	result := make(map[string]interface{}, len(columns))
	for rows.Next() {
		row := make([]interface{}, len(columns))
		for i := range columns {
			// Scan the row with the custom scanner
			row[i] = &scanner{}
		}

		if err := rows.Scan(row...); err != nil {
			return err
		}

		for i, column := range columns {
			scanner := row[i].(*scanner)
			result[column] = scanner.value
		}

		// Based on the provided interface it will either
		// scan the result to the struct or slice
		if err := st.scan(s, result); err != nil {
			return err
		}
	}

	return nil
}

// Reset the values from the query struct
// and log the query if debug is set to true
func (f *Fluent) reset() {
	f.query.log()
	f.query = newQuery()
}
