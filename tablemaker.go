package tablemaker

import (
	"context"
	"io"
)

type Column struct {
	Name string
	Type string
}

type Table struct {
	Name    string
	Columns []*Column
}

type TableService interface {
	List(ctx context.Context, names []string) ([]*Table, error)
}

type TablePrinter interface {
	Print(w io.Writer, tables []*Table) error
}

type FileWriter interface {
	Write(path string, table *Table) error
}
