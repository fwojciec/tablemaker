package tablemaker_test

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	"github.com/fwojciec/tablemaker"
)

var testTables = []*tablemaker.Table{
	{
		Name: "test_table_1",
		Columns: []*tablemaker.Column{
			{Name: "col1", Type: "text"},
			{Name: "col2", Type: "numeric"},
		},
	},
	{
		Name: "test_table_2",
		Columns: []*tablemaker.Column{
			{Name: "col1", Type: "bool"},
			{Name: "col2", Type: "integer"},
		},
	},
}

func TestWriteTables(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "")
	ok(t, err)
	t.Cleanup(func() {
		os.RemoveAll(dir)
	})

	err = tablemaker.WriteTables(testTables, dir)
	ok(t, err)

	res1, err := os.ReadFile(path.Join(dir, "test_table_1.sql"))
	ok(t, err)
	exp1 := `create table if not exists "test_table_1" (
    col1 text,
    col2 numeric
);`
	equals(t, exp1, string(res1))

	res2, err := os.ReadFile(path.Join(dir, "test_table_2.sql"))
	ok(t, err)
	exp2 := `create table if not exists "test_table_2" (
    col1 bool,
    col2 integer
);`
	equals(t, exp2, string(res2))
}

// ok fails the test if an err is not nil.
func ok(tb testing.TB, err error) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: unexpected error: %s\033[39m\n\n", filepath.Base(file), line, err.Error())
		tb.FailNow()
	}
}

// equals fails the test if exp is not equal to act.
func equals(tb testing.TB, exp, act interface{}) {
	if !reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d:\n\n\texp: %#v\n\n\tgot: %#v\033[39m\n\n", filepath.Base(file), line, exp, act)
		tb.FailNow()
	}
}
