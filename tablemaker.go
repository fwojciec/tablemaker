package tablemaker

import "context"

type Column struct {
	Name string
	Type string
}

type Table struct {
	Name    string
	Columns []Column
}

type TableService interface {
	List(ctx context.Context, names []string) ([]*Table, error)
}
