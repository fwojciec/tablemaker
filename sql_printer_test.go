package tablemaker_test

import (
	"bytes"
	"testing"

	"github.com/fwojciec/tablemaker"
)

var testIndent = "  "

func TestSqlPrinter(t *testing.T) {
	t.Parallel()

	subjest := &tablemaker.SqlPrinter{Indent: testIndent}

	res := &bytes.Buffer{}
	err := subjest.Print(res, []*tablemaker.Table{testTables[0]})

	ok(t, err)

	exp := `create table if not exists "test_table_1" (
  col1 text,
  col2 numeric
);`
	equals(t, exp, res.String())
}
