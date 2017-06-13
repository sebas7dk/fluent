/*
Package fluent is an ORM build around the database/sql package.

Overview
	- SQL builder
	- Supported database(s): Postgres

Quickstart
	import (
		"database/sql"
		_ "github.com/lib/pq"
		bitbucket.org/sebas_dk/fluent

	)
	func main() {
		db, err := sql.Open("postgres", "postgres://pqgotest:password@localhost/pqgotest?sslmode=verify-full")
		if err != nil {
			log.Fatal(err)
		}

		fluent := fluent.New(db)
		…
	}

Tags
The Fluent ORM uses the tags in the struct to find and set values.
		type Test struct {
			ID int `sql:"id"`
			Name string `sql:"name"`
			Total string `sql:"total"`
		}

Create Record
		test := Test{Name: "user_1", Total: 12.00}
        fluent.Table("test").Insert(test)

Update Record
		test := Test{Name: "user_2"}
        fluent.Table("test").Where("id","=", 1).Update(test)

Fetch Record
To fetch a single record:
		test := Test{}
        fluent.Table("test").Where("id","=", 1).Get([]string("id","name","total").One(&test)

		fluent.Table("test").WhereNull("name", true).Get([]string("*").One(&test)
To fetch all the records:
		tests := []Test{}
        fluent.Table("test").Get([]string("id","name", "total").All(&tests)

Join Records
		test := Join{}
        fluent.Table("test_1 as t2").Join("test_2 as t2", "t2.user_id", "t1.id").Get("t1.name").One(&test)

*/
package fluent
