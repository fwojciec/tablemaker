package tablemaker

import (
	"fmt"
	"io"
	"strings"
)

type SqlPrinter struct {
	Indent string
}

var _ TablePrinter = (*SqlPrinter)(nil)

func (p *SqlPrinter) Print(w io.Writer, tables []*Table) error {
	parts := make([]string, len(tables))
	for i, table := range tables {
		parts[i] = p.table(table)
	}
	_, err := fmt.Fprint(w, strings.Join(parts, "\n"))
	if err != nil {
		return err
	}
	return nil
}

func (p *SqlPrinter) table(t *Table) string {
	return fmt.Sprintf("create table if not exists %q (\n%s\n);", t.Name, p.columns(t.Columns))
}

func (p *SqlPrinter) columns(cs []*Column) string {
	parts := make([]string, len(cs))
	for i, c := range cs {
		parts[i] = c.Name + " " + c.Type
	}
	return p.Indent + strings.Join(parts, ",\n"+p.Indent)
}
