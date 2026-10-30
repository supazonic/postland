package postland

import (
	"fmt"
	"reflect"
)

// Row is one row to write, keyed by column name.
type Row = map[string]any

// Field says where a column's value comes from.
//
// Exactly one of Path or Func is used; Func wins if both are set. When the
// path is missing or the value is nil, Default is used. Transform runs after
// that and before type coercion.
type Field struct {
	Path      string
	Func      func(record any) (any, error)
	Transform func(v any) (any, error)
	Default   any
}

// From reads a path such as "customer.email" or "items[0].sku".
func From(path string) Field { return Field{Path: path} }

// Const always yields v (e.g. a source-system tag).
func Const(v any) Field {
	return Field{Func: func(any) (any, error) { return v, nil }}
}

// Compute derives a value from the whole record.
func Compute(fn func(record any) (any, error)) Field { return Field{Func: fn} }

// Whole is the record itself (e.g. to keep the raw payload in a JSONB column,
// or for the element of an array of scalars).
var Whole = Field{Path: "$"}

// WithDefault returns a copy of f with a default value.
func (f Field) WithDefault(v any) Field { f.Default = v; return f }

// Then returns a copy of f that applies fn to the extracted value.
func (f Field) Then(fn func(v any) (any, error)) Field { f.Transform = fn; return f }

func (f Field) extract(record any) (any, error) {
	var v any
	if f.Func != nil {
		var err error
		if v, err = f.Func(record); err != nil {
			return nil, err
		}
	} else if found, ok := Lookup(record, f.Path); ok {
		v = found
	}
	if v == nil {
		v = f.Default
	}
	if f.Transform != nil {
		return f.Transform(v)
	}
	return v, nil
}

// Nested explodes an array inside each record into rows of a child table.
type Nested struct {
	// Path to the array within the parent record. A single object is treated
	// as a one-element array.
	Path string
	// Mapping applied to each array element.
	Mapping *TableMapping
	// ParentKeys copies values from the parent record onto each child row:
	// child column -> path in the parent record (usually the foreign key).
	ParentKeys map[string]string
	// IndexColumn, if set, receives the element's position (0-based).
	IndexColumn string
}

// TableMapping maps records onto Table.
//
// Fields maps column name -> Field. Columns not listed are read from the
// record key with the same name, except columns with a SQL Default (those are
// left to the database). Children explode nested arrays into other tables.
// Where, if set, skips records for which it returns false.
type TableMapping struct {
	Table    *Table
	Fields   map[string]Field
	Children []Nested
	Where    func(record any) bool

	compiled []compiledField
	ready    bool
}

type compiledField struct {
	col   *Column
	field Field
}

// MappingError reports which table and column a record failed on.
type MappingError struct {
	Table  string
	Column string
	Err    error
}

func (e *MappingError) Error() string {
	if e.Column == "" {
		return fmt.Sprintf("%s: %v", e.Table, e.Err)
	}
	return fmt.Sprintf("%s.%s: %v", e.Table, e.Column, e.Err)
}

func (e *MappingError) Unwrap() error { return e.Err }

// Compile validates the mapping (and its children). It is called by Map and
// by the pipeline, so calling it yourself is optional.
func (m *TableMapping) Compile() error {
	return m.compile(nil)
}

func (m *TableMapping) compile(inherited map[string]bool) error {
	if m.Table == nil {
		return fmt.Errorf("mapping has no table")
	}
	if err := m.Table.Validate(); err != nil {
		return err
	}
	for name := range m.Fields {
		if _, ok := m.Table.Column(name); !ok {
			return fmt.Errorf("mapping field %q is not a column of %s", name, m.Table.QualifiedName())
		}
	}
	m.compiled = nil
	for i := range m.Table.Columns {
		col := &m.Table.Columns[i]
		if inherited[col.Name] {
			continue // filled from the parent record
		}
		if f, ok := m.Fields[col.Name]; ok {
			m.compiled = append(m.compiled, compiledField{col, f})
		} else if col.Default == "" {
			m.compiled = append(m.compiled, compiledField{col, From(col.Name)})
		}
	}
	for _, child := range m.Children {
		if child.Mapping == nil {
			return fmt.Errorf("%s: nested %q has no mapping", m.Table.QualifiedName(), child.Path)
		}
		keys := map[string]bool{}
		for col := range child.ParentKeys {
			keys[col] = true
		}
		if child.IndexColumn != "" {
			keys[child.IndexColumn] = true
		}
		for col := range keys {
			if _, ok := child.Mapping.Table.Column(col); !ok {
				return fmt.Errorf("nested %q: %q is not a column of %s", child.Path, col, child.Mapping.Table.QualifiedName())
			}
		}
		if err := child.Mapping.compile(keys); err != nil {
			return err
		}
	}
	m.ready = true
	return nil
}

// Tables lists this table followed by all descendant tables, parents first.
func (m *TableMapping) Tables() []*Table {
	out := []*Table{m.Table}
	for _, c := range m.Children {
		for _, t := range c.Mapping.Tables() {
			if !containsTable(out, t) {
				out = append(out, t)
			}
		}
	}
	return out
}

// Map converts one record into rows for this table and its children.
// Either the whole record maps or nothing does.
func (m *TableMapping) Map(record any) (*Rows, error) {
	if !m.ready {
		if err := m.Compile(); err != nil {
			return nil, err
		}
	}
	out := NewRows(m.Tables()...)
	if err := m.mapInto(record, nil, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (m *TableMapping) mapInto(record any, extra Row, out *Rows) error {
	if m.Where != nil && !m.Where(record) {
		return nil
	}
	name := m.Table.QualifiedName()
	row := make(Row, len(m.compiled)+len(extra))
	set := func(col *Column, raw any) error {
		v, err := col.Type.Coerce(raw)
		if err != nil {
			return &MappingError{name, col.Name, err}
		}
		if v == nil && col.required() && col.Default == "" {
			return &MappingError{name, col.Name, fmt.Errorf("value is required but missing")}
		}
		row[col.Name] = v
		return nil
	}
	for _, cf := range m.compiled {
		raw, err := cf.field.extract(record)
		if err != nil {
			return &MappingError{name, cf.col.Name, fmt.Errorf("extraction failed: %w", err)}
		}
		if err := set(cf.col, raw); err != nil {
			return err
		}
	}
	for colName, raw := range extra {
		col, _ := m.Table.Column(colName)
		if err := set(col, raw); err != nil {
			return err
		}
	}
	out.Add(m.Table, row)

	for _, child := range m.Children {
		items, ok := Lookup(record, child.Path)
		if !ok || items == nil {
			continue
		}
		list := asSlice(items)
		keys := make(Row, len(child.ParentKeys))
		for col, path := range child.ParentKeys {
			keys[col], _ = Lookup(record, path)
		}
		for i, item := range list {
			childExtra := make(Row, len(keys)+1)
			for k, v := range keys {
				childExtra[k] = v
			}
			if child.IndexColumn != "" {
				childExtra[child.IndexColumn] = i
			}
			if err := child.Mapping.mapInto(item, childExtra, out); err != nil {
				return err
			}
		}
	}
	return nil
}

func asSlice(v any) []any {
	if s, ok := v.([]any); ok {
		return s
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		out := make([]any, rv.Len())
		for i := range out {
			out[i] = rv.Index(i).Interface()
		}
		return out
	}
	return []any{v}
}

// Rows holds mapped rows grouped by table, keeping parents before children
// so foreign keys are satisfied when written in order.
type Rows struct {
	tables []*Table
	rows   map[*Table][]Row
}

// NewRows creates an empty Rows with the given table order.
func NewRows(tables ...*Table) *Rows {
	r := &Rows{rows: map[*Table][]Row{}}
	for _, t := range tables {
		r.ensure(t)
	}
	return r
}

func (r *Rows) ensure(t *Table) {
	if _, ok := r.rows[t]; !ok {
		r.tables = append(r.tables, t)
		r.rows[t] = nil
	}
}

// Add appends a row for t.
func (r *Rows) Add(t *Table, row Row) {
	r.ensure(t)
	r.rows[t] = append(r.rows[t], row)
}

// Merge appends all rows of other.
func (r *Rows) Merge(other *Rows) {
	for _, t := range other.tables {
		r.ensure(t)
		r.rows[t] = append(r.rows[t], other.rows[t]...)
	}
}

// Tables returns the tables in write order.
func (r *Rows) Tables() []*Table { return r.tables }

// For returns the rows for t.
func (r *Rows) For(t *Table) []Row { return r.rows[t] }

// Len is the total number of rows.
func (r *Rows) Len() int {
	n := 0
	for _, rows := range r.rows {
		n += len(rows)
	}
	return n
}

func containsTable(list []*Table, t *Table) bool {
	for _, x := range list {
		if x == t {
			return true
		}
	}
	return false
}
