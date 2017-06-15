package fluent

import "database/sql"

// Fluent is the struct that holds
// the database connection and
// the query information
type Fluent struct {
	db    *sql.DB
	query *query
}

// Mapper exposes the functionalities
// to start building the query
type Mapper interface {
	Table(table string) QueryMapper
	GetDB() *sql.DB
	Debug(status bool) Mapper
}

// QueryMapper exposes the functionalities
// to build the query
type QueryMapper interface {
	Join(table, column1, column2 string) QueryMapper
	LeftJoin(table, column1, column2 string) QueryMapper
	Where(column, operator string, value interface{}) QueryMapper
	WhereNull(column string, isNull bool) QueryMapper
	OrderBy(columns []string) QueryMapper
	GroupBy(columns []string) QueryMapper
	Limit(limit int) QueryMapper
	Offset(offset int) QueryMapper
	Get(columns []string) ScanMapper
	ExecuteMapper
}

// ScanMapper exposes the functionalities to
// scan and fetch the rows
type ScanMapper interface {
	One(s interface{}) error
	All(s interface{}) error
}

// ExecuteMapper exposes the functionalities
// to execute the query
type ExecuteMapper interface {
	Insert(s interface{}) error
	Update(s interface{}) error
}

// New set the DB connection and query struct
func New(db *sql.DB) Mapper {
	return &Fluent{db, newQuery()}
}

// clone the fluent struct for concurrent use
func (f *Fluent) clone() *Fluent {
	return &Fluent{f.db, newQuery()}
}

// Debug if set to true it will log the query
func (f *Fluent) Debug(status bool) Mapper {
	f.query.builder(setDebug(status))
	return f
}

// GetDB returns the database connection
func (f *Fluent) GetDB() *sql.DB {
	return f.db
}

// Table set the table name
func (f *Fluent) Table(table string) QueryMapper {
	f = f.clone()
	f.query.builder(setTable(table))
	return f
}

// Join set the table and columns for the join query
func (f *Fluent) Join(table, column1, column2 string) QueryMapper {
	join := []interface{}{table, column1, column2}
	f.query.builder(setJoin(join))
	return f
}

// LeftJoin set the table and columns for the left join query
func (f *Fluent) LeftJoin(table, column1, column2 string) QueryMapper {
	join := []interface{}{table, column1, column2}
	f.query.builder(setLeftJoin(join))
	return f
}

// Where set the column, operator and the value for the where clause
func (f *Fluent) Where(column, operator string, value interface{}) QueryMapper {
	where := []interface{}{column, operator, value}
	f.query.builder(setWhere(where))
	return f
}

// WhereNull set if the column is null or not null
func (f *Fluent) WhereNull(column string, isNull bool) QueryMapper {
	where := []interface{}{column, isNull}
	f.query.builder(setWhereNull(where))
	return f
}

// OrderBy set to columns to order by
func (f *Fluent) OrderBy(columns []string) QueryMapper {
	f.query.builder(setOrderBy(columns))
	return f
}

// GroupBy set to columns to group by
func (f *Fluent) GroupBy(columns []string) QueryMapper {
	f.query.builder(setGroupBy(columns))
	return f
}

// Limit set the limit of records to return
func (f *Fluent) Limit(limit int) QueryMapper {
	f.query.builder(setLimit(limit))
	return f
}

// Offset set the offset for the records to return
func (f *Fluent) Offset(offset int) QueryMapper {
	f.query.builder(setOffset(offset))
	return f
}

// Get set the columns to select from and build the query
func (f *Fluent) Get(columns []string) ScanMapper {
	f.query.builder(
		setColumns(columns),
		buildSelect(),
		buildJoin(),
		buildLeftJoin(),
		buildGroupBy(),
		buildOrderBy(),
		buildWhere(),
		buildWhereNull(),
		buildOffset(),
		buildLimit(),
	)
	return f
}

// Insert a record by building the query and scanning
// the values from the struct to insert
func (f *Fluent) Insert(s interface{}) error {
	cols, args, err := getStructValues(s)
	if err != nil {
		return err
	}

	f.query.builder(buildInsert(cols, args))
	return f.execute()
}

// Update a record by building the query and scanning
// the values from the struct to update
func (f *Fluent) Update(s interface{}) error {
	cols, args, err := getStructValues(s)
	if err != nil {
		return err
	}

	f.query.builder(
		buildUpdate(cols, args),
		buildWhere(),
		buildWhereNull(),
	)

	return f.execute()
}

// One fetch a single record
func (f *Fluent) One(s interface{}) error {
	st := &one{}
	return f.scan(s, st)
}

// All fetch all the records
func (f *Fluent) All(s interface{}) error {
	st := &all{}
	return f.scan(s, st)
}

func (f *Fluent) execute() error {
	defer f.query.log()

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
	defer f.query.log()

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
