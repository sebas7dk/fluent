package integration

import (
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq" //postgres package
	"github.com/sebas7dk/fluent"
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
		require := require.New(t)

		for i := 1; i <= 10; i++ {
			record := test1{
				Name:  fmt.Sprintf("user_%d", i),
				Total: 10.00 + float64(i),
			}

			id, err := f.Table("test_1").Insert(record)
			if err != nil {
				t.Fatal(err)
			}

			require.True(id > 0)
		}
	})

	t.Run("Insert records in table test 2", func(t *testing.T) {
		require := require.New(t)

		for i := 1; i <= 10; i++ {
			record := test2{
				TestID:   i,
				IsActive: 0,
			}

			id, err := f.Table("test_2").Insert(record)
			if err != nil {
				t.Fatal(err)
			}

			require.True(id > 0)
		}
	})

	t.Run("Update records in table test 2", func(t *testing.T) {
		for i := 1; i <= 10; i++ {
			record := test2{
				IsActive: 1,
			}

			if err := f.Table("test_2").Where("id", "=", i).Update(record); err != nil {
				t.Fatal(err)
			}
		}
	})

	t.Run("Update a single record in table test 1", func(t *testing.T) {
		require := require.New(t)

		record := test1{
			UpdatedAt: time.Now(),
		}
		if err := f.Table("test_1").Where("id", "=", 1).Update(record); err != nil {
			t.Fatal(err)
		}

		record = test1{}
		err = f.Table("test_1").WhereNull("updated_at", false).Get("*").One(&record)
		if err != nil {
			t.Fatal(err)
		}

		require.NotNil(record)
	})

	t.Run("Get a single records from table test 1", func(t *testing.T) {
		require := require.New(t)

		for i := 1; i <= 10; i++ {
			record := test1{}
			err := f.Table("test_1").Where("id", "=", i).Get("id", "name", "total").One(&record)
			if err != nil {
				t.Fatal(err)
			}

			require.Equal(i, record.ID)
			require.Equal(fmt.Sprintf("user_%d", i), record.Name)
			require.Equal(10.00+float64(i), record.Total)
		}
	})

	t.Run("Get all records from table test 1", func(t *testing.T) {
		require := require.New(t)

		records := []test1{}
		err := f.Table("test_1").OrderBy("id").Limit(10).Get("id", "name", "total").All(&records)
		if err != nil {
			t.Fatal(err)
		}
		for i, record := range records {
			i++
			require.Equal(i, record.ID)
			require.Equal(fmt.Sprintf("user_%d", i), record.Name)
			require.Equal(10.00+float64(i), record.Total)
		}
	})

	t.Run("Join both test tables", func(t *testing.T) {
		require := require.New(t)

		record := joinboth{}

		var id = 1
		err := f.Table("test_1 as t1").
			Join("test_2 as t2", "t2.test_id", "t1.id").
			Where("t1.id", "=", id).
			Get("t1.id", "t1.name", "t1.total", "t2.is_active").
			One(&record)
		if err != nil {
			t.Fatal(err)
		}

		require.Equal(id, record.ID)
		require.Equal(fmt.Sprintf("user_%d", id), record.Name)
		require.Equal(10.00+float64(id), record.Total)

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

			record := test1{
				Name:  fmt.Sprintf("user_%d", i),
				Total: 10.00 + float64(i),
			}

			if _, err := f.Table("test_1").Insert(record); err != nil {
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

				record := test1{}
				err := f.Table("test_1").Where("id", "=", id).Get("id", "name", "total").One(&record)
				if err != nil {
					fmt.Println(err)
				}

				require.Equal(id, record.ID)
				require.Equal(fmt.Sprintf("user_%d", id), record.Name)
				require.Equal(10.00+float64(id), record.Total)
			}
		}()
	}
	wg.Wait()
}
