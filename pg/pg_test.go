package pg_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	"github.com/fwojciec/tablemaker/pg"
)

const TEST_DSN = "user=test dbname=test password=test sslmode=disable"

func MustOpenDB(tb testing.TB) *pg.DB {
	tb.Helper()

	db := pg.NewDB(TEST_DSN)
	if err := db.Open(); err != nil {
		tb.Fatal(err)
	}

	return db
}

func MustCloseDB(tb testing.TB, db *pg.DB) {
	tb.Helper()

	if err := db.Close(); err != nil {
		tb.Fatal(err)
	}
}

func TestDB(t *testing.T) {
	t.Parallel()

	db := MustOpenDB(t)
	MustCloseDB(t, db)
}

func TestIsolate(t *testing.T) {
	t.Parallel()

	db := MustOpenDB(t)
	t.Cleanup(func() {
		MustCloseDB(t, db)
	})

	isolate(t, db, func(t *testing.T, db *pg.DB) {
		t.Helper()
		ctx := context.Background()
		tx, err := db.BeginTx(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var res int
		tx.GetContext(ctx, &res, "select 1")
		equals(t, res, 1)
	})

}

func isolate(t *testing.T, tdb *pg.DB, testFn func(t *testing.T, db *pg.DB)) {
	t.Helper()

	ctx := context.Background()
	schema := randomID()

	createSchema(t, ctx, tdb, schema)
	newDSN := tdb.DSN + " search_path=" + schema
	sdb := pg.NewDB(newDSN)
	if err := sdb.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		dropSchema(t, ctx, tdb, schema)
		sdb.Close()
	})
	testFn(t, sdb)
}

func createSchema(tb testing.TB, ctx context.Context, tdb *pg.DB, schema string) {
	tb.Helper()
	tx, err := tdb.BeginTx(ctx)
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := tx.Exec("CREATE SCHEMA " + schema); err != nil {
		tb.Fatal(err)
	}
	tx.Commit()
}

func dropSchema(tb testing.TB, ctx context.Context, tdb *pg.DB, schema string) {
	tb.Helper()
	tx, err := tdb.BeginTx(ctx)
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := tx.Exec(fmt.Sprintf("DROP SCHEMA %s CASCADE", schema)); err != nil {
		tb.Fatal(err)
	}
	tx.Commit()
}

func randomID() string {
	var abc = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	var buf bytes.Buffer
	for i := 0; i < 10; i++ {
		buf.WriteByte(abc[rand.Intn(len(abc))])
	}
	return buf.String()
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
