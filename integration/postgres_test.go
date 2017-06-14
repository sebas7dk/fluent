package integration

import (
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"bitbucket.org/sebas_dk/fluent"

	_ "github.com/lib/pq" //postgres package
	"github.com/stretchr/testify/require"
)

type test1 struct {
	ID        int       `sql:"id"`
	Name      string    `sql:"name"`
	Total     float64   `sql:"total"`
	UpdatedAt time.Time `sql:"deleted_at"`
}

type test2 struct {
	TestID   int `sql:"test_id"`
	IsActive int `sql:"is_active"`
}

type joinboth struct {
	ID       int     `sql:"id"`
	Name     string  `sql:"name"`
	Total    float64 `sql:"total"`
	IsActive int     `sql:"is_active"`
}

func connect() (fluent.Mapper, error) {
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

func Test_Postgres(t *testing.T) {
	f, err := connect()
	if err != nil {
		t.Fatalf("Unable to connect to the database: %s", err)
	}

	t.Run("Insert records in table test 1", func(t *testing.T) {
		for i := 1; i <= 10; i++ {
			test := test1{
				Name:  fmt.Sprintf("user_%d", i),
				Total: 10.00 + float64(i),
			}

			if err := f.Table("test_1").Insert(test); err != nil {
				t.Fatal(err)
			}
		}
	})

	t.Run("Insert records in table test 2", func(t *testing.T) {
		for i := 1; i <= 10; i++ {
			test := test2{
				TestID:   i,
				IsActive: 0,
			}

			if err := f.Table("test_2").Insert(test); err != nil {
				t.Fatal(err)
			}
		}
	})

	t.Run("Update records in table test 2", func(t *testing.T) {
		for i := 1; i <= 10; i++ {
			test := test2{
				IsActive: 1,
			}

			if err := f.Debug(true).Table("test_2").Where("id", "=", i).Update(test); err != nil {
				t.Fatal(err)
			}
		}
	})

	t.Run("Update a single record in table test 1", func(t *testing.T) {
		require := require.New(t)

		update := test1{
			UpdatedAt: time.Now(),
		}
		if err := f.Table("test_1").Where("id", "=", 1).Update(update); err != nil {
			t.Fatal(err)
		}

		get := test1{}
		err := f.Table("test_1").WhereNull("updated_at", false).Get([]string{"*"}).One(&get)
		if err != nil {
			t.Fatal(err)
		}

		require.NotNil(get)
	})

	t.Run("Get a single records from table test 1", func(t *testing.T) {
		require := require.New(t)

		for i := 1; i <= 10; i++ {
			test := test1{}
			err := f.Table("test_1").Where("id", "=", i).Get([]string{"id", "name", "total"}).One(&test)
			if err != nil {
				t.Fatal(err)
			}

			require.Equal(i, test.ID)
			require.Equal(fmt.Sprintf("user_%d", i), test.Name)
			require.Equal(10.00+float64(i), test.Total)
		}
	})

	t.Run("Get all records from table test 1", func(t *testing.T) {
		require := require.New(t)

		tests := []test1{}
		err := f.Table("test_1").OrderBy([]string{"id"}).Limit(10).Get([]string{"id", "name", "total"}).All(&tests)
		if err != nil {
			t.Fatal(err)
		}
		for i, test := range tests {
			i++
			require.Equal(i, test.ID)
			require.Equal(fmt.Sprintf("user_%d", i), test.Name)
			require.Equal(10.00+float64(i), test.Total)
		}
	})

	t.Run("Join both test tables", func(t *testing.T) {
		require := require.New(t)

		test := joinboth{}

		var id = 1
		err := f.Table("test_1 as t1").
			Join("test_2 as t2", "t2.test_id", "t1.id").
			Where("t1.id", "=", id).
			Get([]string{"t1.id", "t1.name", "t1.total", "t2.is_active"}).
			One(&test)
		if err != nil {
			t.Fatal(err)
		}

		require.Equal(id, test.ID)
		require.Equal(fmt.Sprintf("user_%d", id), test.Name)
		require.Equal(10.00+float64(id), test.Total)

	})

}

func Test_Concurrency(t *testing.T) {
	f, err := connect()
	if err != nil {
		t.Fatalf("Unable to connect to the database: %s", err)
	}

	wg := sync.WaitGroup{}
	queryChan := make(chan int, 10)

	for i := 1; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			test := test1{
				Name:  fmt.Sprintf("user_%d", i),
				Total: 10.00 + float64(i),
			}

			if err := f.Table("test_1").Insert(test); err != nil {
				t.Fatal(err)
			}

			queryChan <- i
		}(i)
	}
	wg.Wait()
	close(queryChan)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range queryChan {
				require := require.New(t)

				test := test1{}
				err := f.Debug(true).Table("test_1").Where("id", "=", id).Get([]string{"id", "name", "total"}).One(&test)
				if err != nil {
					fmt.Println(err)
				}

				require.Equal(id, test.ID)
				require.Equal(fmt.Sprintf("user_%d", id), test.Name)
				require.Equal(10.00+float64(id), test.Total)
			}
		}()
	}
	wg.Wait()
}
