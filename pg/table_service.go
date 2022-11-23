package pg

import (
	"context"

	"github.com/fwojciec/tablemaker"
	"github.com/lib/pq"
)

type TableService struct {
	db     *DB
	schema string
}

var _ tablemaker.TableService = (*TableService)(nil)

func NewTableService(db *DB, schema string) *TableService {
	return &TableService{db: db, schema: schema}
}

type column struct {
	TableName  string `db:"table_name"`
	ColumnName string `db:"column_name"`
	DataType   string `db:"data_type"`
}

func (s *TableService) List(ctx context.Context, names []string) ([]*tablemaker.Table, error) {
	tx, err := s.db.BeginTx(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	q := `
		select
			cls.relname as table_name,
			attr.attname as column_name,
			trim(leading '_' from tp.typname) as data_type
		from pg_catalog.pg_attribute as attr
		join pg_catalog.pg_class as cls on cls.oid = attr.attrelid
		join pg_catalog.pg_namespace as ns on ns.oid = cls.relnamespace
		join pg_catalog.pg_type as tp on tp.typelem = attr.atttypid
		where
			ns.nspname = $1 and
			cls.relname = any($2) and
			not attr.attisdropped and
			cast(tp.typanalyze as text) = 'array_typanalyze' and
			attr.attnum > 0
		order by
			table_name, attr.attnum
	`
	var cols []column
	err = tx.SelectContext(ctx, &cols, q, s.schema, pq.Array(names))
	if err != nil {
		return nil, err
	}
	tm := make(map[string][]*tablemaker.Column)
	for _, col := range cols {
		if _, ok := tm[col.TableName]; !ok {
			tm[col.TableName] = make([]*tablemaker.Column, 0)
		}
		tm[col.TableName] = append(tm[col.TableName], &tablemaker.Column{
			Name: col.ColumnName,
			Type: col.DataType,
		})
	}
	res := make([]*tablemaker.Table, len(names))
	for i, name := range names {
		cols, ok := tm[name]
		if !ok || len(cols) == 0 {
			continue
		}
		res[i] = &tablemaker.Table{
			Name:    name,
			Columns: cols,
		}
	}
	return res, nil
}
