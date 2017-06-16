/*
Package fluent is an ORM build around the lib/pq package.

Overview
	- SQL builder
	- Supported database(s): Postgres

Quickstart

Before you can use the Fluent ORM you have to connect to a database, for example:

	import (
		"database/sql"
		_ "github.com/lib/pq"
		github.com/sebas7dk/fluent

	)
	func main() {
		db, err := sql.Open("postgres", "postgres://pqgotest:password@localhost/pqgotest?sslmode=verify-full")
		if err != nil {
			log.Fatal(err)
		}

		fluent := fluent.New(db)
		…
	}

Query Builder

The Fluent ORM uses the tags in the struct to find and set values.
	type Record struct {
	    ID int       `sql:"id"`
	    Name string  `sql:"name"`
	    Total string `sql:"total"`
	}

Create Record
  record := Record{Name: "user_1", Total: 12.00}
  id, err := fluent.Table("test").Insert(record)

Update Record
  record := Record{Name: "user_2"}
  err := fluent.Table("test").Where("id","=", 1).Update(record)

Fetch Record
  record := Record{}
  err := fluent.Table("test").Where("id","=", 1).Get("id","name","total").One(&record)

  err = fluent.Table("test").GroupBy("id").WhereNull("name", true).Get("*").One(&record)

  records := []Record{}
  err = fluent.Table("test").Get("id","name", "total").All(&records)

Join Records
  record := Record{}
  err := fluent.Table("test_1 as t2").Join("test_2 as t2", "t2.user_id", "t1.id").Get("t1.name").One(&record)
*/
package fluent
