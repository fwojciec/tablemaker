package pg_test

import (
	"context"
	"log"
	"testing"

	"github.com/fwojciec/tablemaker"
	"github.com/fwojciec/tablemaker/pg"
)

func TestList(t *testing.T) {
	t.Parallel()

	tdb := MustOpenDB(t)

	t.Cleanup(func() {
		MustCloseDB(t, tdb)
	})

	t.Run("table", func(t *testing.T) {
		t.Parallel()

		isolate(t, tdb, func(t *testing.T, db *pg.DB, schema string) {
			ctx := context.Background()
			createTable(t, ctx, db)
			subject := pg.NewTableService(db, schema)

			res, err := subject.List(ctx, []string{"test_table"})

			ok(t, err)
			exp := []*tablemaker.Table{
				{
					Name: "test_table",
					Columns: []tablemaker.Column{
						{Name: "numeric_column", Type: "numeric"},
						{Name: "text_column", Type: "text"},
						{Name: "boolean_column", Type: "bool"},
					},
				},
			}
			for _, row := range res {
				log.Println(row)
			}
			equals(t, exp, res)
		})
	})

	t.Run("view", func(t *testing.T) {
		t.Parallel()

		isolate(t, tdb, func(t *testing.T, db *pg.DB, schema string) {
			ctx := context.Background()
			createTable(t, ctx, db)
			createView(t, ctx, db)
			subject := pg.NewTableService(db, schema)

			res, err := subject.List(ctx, []string{"test_view"})

			ok(t, err)
			exp := []*tablemaker.Table{
				{
					Name: "test_view",
					Columns: []tablemaker.Column{
						{Name: "numeric_column", Type: "numeric"},
						{Name: "text_column", Type: "text"},
						{Name: "boolean_column", Type: "bool"},
					},
				},
			}
			equals(t, exp, res)
		})
	})

	t.Run("materialized view", func(t *testing.T) {
		t.Parallel()

		isolate(t, tdb, func(t *testing.T, db *pg.DB, schema string) {
			ctx := context.Background()
			createTable(t, ctx, db)
			createMaterializedView(t, ctx, db)
			subject := pg.NewTableService(db, schema)

			res, err := subject.List(ctx, []string{"test_materialized_view"})

			ok(t, err)
			exp := []*tablemaker.Table{
				{
					Name: "test_materialized_view",
					Columns: []tablemaker.Column{
						{Name: "numeric_column", Type: "numeric"},
						{Name: "text_column", Type: "text"},
						{Name: "boolean_column", Type: "bool"},
					},
				},
			}
			equals(t, exp, res)
		})
	})

	t.Run("a table and a view", func(t *testing.T) {
		t.Parallel()

		isolate(t, tdb, func(t *testing.T, db *pg.DB, schema string) {
			ctx := context.Background()
			createTable(t, ctx, db)
			createView(t, ctx, db)
			subject := pg.NewTableService(db, schema)

			res, err := subject.List(ctx, []string{"test_table", "test_view"})

			ok(t, err)
			exp := []*tablemaker.Table{
				{
					Name: "test_table",
					Columns: []tablemaker.Column{
						{Name: "numeric_column", Type: "numeric"},
						{Name: "text_column", Type: "text"},
						{Name: "boolean_column", Type: "bool"},
					},
				},
				{
					Name: "test_view",
					Columns: []tablemaker.Column{
						{Name: "numeric_column", Type: "numeric"},
						{Name: "text_column", Type: "text"},
						{Name: "boolean_column", Type: "bool"},
					},
				},
			}
			equals(t, exp, res)
		})
	})
}

func createTable(tb testing.TB, ctx context.Context, db *pg.DB) {
	tb.Helper()
	tx, err := db.BeginTx(ctx)
	if err != nil {
		tb.Fatal(err)
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `
		create table test_table (
			numeric_column numeric,
			text_column text,
			boolean_column bool
		)
	`)
	if err != nil {
		tb.Fatal(err)
	}
	tx.Commit()
}

func createView(tb testing.TB, ctx context.Context, db *pg.DB) {
	tb.Helper()
	tx, err := db.BeginTx(ctx)
	if err != nil {
		tb.Fatal(err)
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `
		create view test_view as select * from test_table
	`)
	if err != nil {
		tb.Fatal(err)
	}
	tx.Commit()
}

func createMaterializedView(tb testing.TB, ctx context.Context, db *pg.DB) {
	tb.Helper()
	tx, err := db.BeginTx(ctx)
	if err != nil {
		tb.Fatal(err)
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `
		create materialized view test_materialized_view as select * from test_table
	`)
	if err != nil {
		tb.Fatal(err)
	}
	tx.Commit()
}
