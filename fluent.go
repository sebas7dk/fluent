package fluent

import "database/sql"

type Fluent struct {
	db    *sql.DB
	query *query
}

func New(db *sql.DB) *Fluent {
	return &Fluent{db, newQuery()}
}

func (f *Fluent) Table(table string) *Fluent {
	f.query = newQuery()
	f.query.table = table
	return f
}

// Join the first argument passed to the join method is the name of the table you need to join to,
// while the remaining arguments specify the column constraints for the join
func (f *Fluent) Join(join []string) *Fluent {
	f.query.join = join
	return f
}

// LeftJoin method has the same signature as the join method
func (f *Fluent) LeftJoin(join []string) *Fluent {
	f.query.leftJoin = join
	return f
}

func (f *Fluent) Where(column, operator string, value interface{}) *Fluent {
	where := []interface{}{column, operator, value}
	f.query.where = append(f.query.where, where)
	return f
}

func (f *Fluent) OrderBy(columns []string) *Fluent {
	f.query.orderBy = columns
	return f
}

func (f *Fluent) GroupBy(columns []string) *Fluent {
	f.query.groupBy = columns
	return f
}

func (f *Fluent) Limit(limit int) *Fluent {
	f.query.limit = limit
	return f
}

func (f *Fluent) Offset(offset int) *Fluent {
	f.query.offset = offset
	return f
}

func (f *Fluent) Get(columns []string) *Fluent {
	f.query.columns = columns
	f.query.buildquery(
		setSelect(),
		setJoin(),
		setLeftJoin(),
		setGroupBy(),
		setOrderBy(),
		setWhere(),
		setOffest(),
		setLimit(),
	)
	return f
}

func (f *Fluent) Insert(colums []string, args []interface{}) error {
	f.query.columns = colums
	f.query.args = args

	f.query.buildquery(setInsert())
	return f.execute()
}

func (f *Fluent) Update(values map[string]interface{}) error {
	for col, arg := range values {
		f.query.columns = append(f.query.columns, col)
		f.query.args = append(f.query.args, arg)
	}

	f.query.buildquery(
		setUpdate(),
		setWhere(),
	)

	return f.execute()
}

// One returns a single row
func (f *Fluent) One(s interface{}) error {
	st := &one{}
	return f.scan(s, st)
}

// All returns all the rows
func (f *Fluent) All(s interface{}) error {
	st := &all{}
	return f.scan(s, st)
}

func (f *Fluent) execute() error {
	prepare, err := f.db.Prepare(f.query.stmt)
	if err != nil {
		return err
	}
	defer prepare.Close()

	_, err = prepare.Exec(f.query.args...)
	return err
}

func (f *Fluent) scan(s interface{}, st scannerType) error {
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
		for i := range f.query.columns {
			row[i] = &scanner{}
		}

		if err := rows.Scan(row...); err != nil {
			return err
		}

		for i, column := range columns {
			scanner := row[i].(*scanner)
			result[column] = scanner.value
		}

		if err := st.scan(s, result); err != nil {
			return err
		}
	}

	return nil
}
