package integration

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"bitbucket.org/sebas_dk/fluent"

	_ "github.com/lib/pq" //postgres package
	"github.com/stretchr/testify/require"
)

type test1 struct {
	ID    int     `sql:"id"`
	Name  string  `sql:"name"`
	Total float64 `sql:"total"`
}
type joinboth struct {
	ID       int     `sql:"id"`
	Name     string  `sql:"name"`
	Total    float64 `sql:"total"`
	IsActive int     `sql:"is_active"`
}

func connect() (*fluent.Fluent, error) {
	connStr := fmt.Sprintf(
		"postgresql://%s:%s@%s:%d/%s?sslmode=disable",
		"fluent",
		"fluent",
		"localhost",
		5432,
		"fluent",
	)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	return fluent.New(db), nil
}

func Test_Insert_Postgres(t *testing.T) {
	// require := require.New(t)

	fluent, err := connect()
	if err != nil {
		t.Fatalf("Unable to connect to the database: %s", err)
	}

	err = fluent.Table("test_1").Insert(
		[]string{"name", "total"},
		[]interface{}{"greald", 12.00},
	)
	if err != nil {
		t.Fatal(err)
	}

	err = fluent.Table("test_2").Insert(
		[]string{"test_id", "is_active"},
		[]interface{}{1, 1},
	)
	if err != nil {
		t.Fatal(err)
	}
}

func Test_Update_Postgres(t *testing.T) {
	// require := require.New(t)

	fluent, err := connect()
	if err != nil {
		t.Fatalf("Unable to connect to the database: %s", err)
	}

	err = fluent.Table("test_1").Update(
		map[string]interface{}{
			"name":       "gerald",
			"updated_at": time.Now(),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
}

func Test_One_Postgres(t *testing.T) {
	require := require.New(t)

	fluent, err := connect()
	if err != nil {
		t.Fatalf("Unable to connect to the database: %s", err)
	}

	test := test1{}
	err = fluent.
		Table("test_1").
		Where("id", "=", 1).
		Get(
			[]string{
				"id",
				"name",
				"total",
			},
		).
		One(&test)
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(1, test.ID)
	require.Equal("gerald", test.Name)
	require.Equal(12.00, test.Total)
}

func Test_All_Postgres(t *testing.T) {
	require := require.New(t)

	fluent, err := connect()
	if err != nil {
		t.Fatalf("Unable to connect to the database: %s", err)
	}

	tests := []test1{}
	err = fluent.
		Table("test_1").
		Offset(0).
		Limit(5).
		Get(
			[]string{
				"id",
				"name",
				"total",
			},
		).
		All(&tests)
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(5, len(tests))
}

func Test_Join_Postgres(t *testing.T) {
	// require := require.New(t)

	fluent, err := connect()
	if err != nil {
		t.Fatalf("Unable to connect to the database: %s", err)
	}

	test := joinboth{}
	err = fluent.
		Table("test_1 as t1").
		LeftJoin([]string{"test_2 as t2", "t2.test_id", "t1.id"}).
		Get(
			[]string{
				"t1.id",
				"t1.name",
				"t1.total",
				"t2.is_active",
			},
		).
		One(&test)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(test)
}
