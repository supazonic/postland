package postland

import "fmt"

// Column describes one table column. Default and References are raw SQL.
type Column struct {
	Name       string
	Type       DataType
	NotNull    bool
	PrimaryKey bool // implies NOT NULL
	Unique     bool
	Default    string // e.g. "now()"; columns with a default are not read from records unless mapped
	References string // e.g. "shop.orders(id)"
}

// Table describes what is stored. It is independent of where the data comes
// from and of how it is written; the postgres package turns it into DDL.
type Table struct {
	Schema  string
	Name    string
	Columns []Column
	Indexes [][]string // each entry is the column list of one index
}

// NewTable is shorthand for &Table{Name: name, Columns: cols}.
func NewTable(name string, cols ...Column) *Table {
	return &Table{Name: name, Columns: cols}
}

// InSchema sets the PostgreSQL schema and returns t.
func (t *Table) InSchema(schema string) *Table { t.Schema = schema; return t }

// Index adds an index on the given columns and returns t.
func (t *Table) Index(cols ...string) *Table {
	t.Indexes = append(t.Indexes, cols)
	return t
}

// QualifiedName is "schema.name", or "name" without a schema.
func (t *Table) QualifiedName() string {
	if t.Schema != "" {
		return t.Schema + "." + t.Name
	}
	return t.Name
}

// PrimaryKey returns the primary key column names in definition order.
func (t *Table) PrimaryKey() []string {
	var pk []string
	for _, c := range t.Columns {
		if c.PrimaryKey {
			pk = append(pk, c.Name)
		}
	}
	return pk
}

// Column returns the named column.
func (t *Table) Column(name string) (*Column, bool) {
	for i := range t.Columns {
		if t.Columns[i].Name == name {
			return &t.Columns[i], true
		}
	}
	return nil, false
}

// Validate checks the definition for obvious mistakes.
func (t *Table) Validate() error {
	if t.Name == "" {
		return fmt.Errorf("table has no name")
	}
	if len(t.Columns) == 0 {
		return fmt.Errorf("table %s has no columns", t.QualifiedName())
	}
	seen := map[string]bool{}
	for _, c := range t.Columns {
		if c.Name == "" || c.Type.SQL == "" {
			return fmt.Errorf("table %s: column needs a name and a type", t.QualifiedName())
		}
		if seen[c.Name] {
			return fmt.Errorf("table %s: duplicate column %q", t.QualifiedName(), c.Name)
		}
		seen[c.Name] = true
	}
	for _, idx := range t.Indexes {
		for _, name := range idx {
			if !seen[name] {
				return fmt.Errorf("table %s: index on unknown column %q", t.QualifiedName(), name)
			}
		}
	}
	return nil
}

func (c Column) required() bool { return c.NotNull || c.PrimaryKey }
