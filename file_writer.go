package tablemaker

import (
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"golang.org/x/sync/errgroup"
)

func WriteTables(tables []*Table, pathPrefix string) error {
	g := &errgroup.Group{}
	for _, table := range tables {
		table := table
		g.Go(func() error {
			f, err := os.Create(path.Join(pathPrefix, table.Name+".sql"))
			if err != nil {
				return err
			}
			defer f.Close()
			return write(table, f)
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}

func write(table *Table, w io.Writer) error {
	var err error
	space := "    "
	_, err = fmt.Fprintf(w, "create table if not exists %q (\n%s", table.Name, space)
	if err != nil {
		return err
	}
	cols := make([]string, len(table.Columns))
	for i, col := range table.Columns {
		cols[i] = fmt.Sprintf("%s %s", col.Name, col.Type)
	}
	joined := strings.Join(cols, ",\n"+space)
	_, err = fmt.Fprintf(w, "%s\n);", joined)
	if err != nil {
		return err
	}
	return nil
}
