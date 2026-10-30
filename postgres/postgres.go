// Package postgres is a thin wrapper around a pgx connection pool that turns
// postland.Table definitions into DDL and writes rows with insert/upsert
// semantics. It also provides Sink, the postland.Sink used by pipelines.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/supazonic/postland"
)

// Mode is how rows are written.
type Mode string

const (
	// Upsert inserts, updating non-key columns on primary-key conflict.
	// Tables without a primary key get a plain insert.
	Upsert Mode = "upsert"
	// Insert is a plain insert; duplicates raise an error.
	Insert Mode = "insert"
	// Ignore inserts and silently skips conflicting rows.
	Ignore Mode = "ignore"
	// Truncate empties the table once at the start of a run, then inserts.
	Truncate Mode = "truncate"
)

func (m Mode) valid() bool {
	switch m {
	case Upsert, Insert, Ignore, Truncate:
		return true
	}
	return false
}

// querier is satisfied by both *pgxpool.Pool and pgx.Tx.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

// DB wraps a pool (or, inside InTx, a transaction).
type DB struct {
	pool     *pgxpool.Pool
	q        querier
	ownsPool bool
}

// Open connects using a DSN / connection URL.
func Open(ctx context.Context, dsn string) (*DB, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &DB{pool: pool, q: pool, ownsPool: true}, nil
}

// FromPool wraps an existing pool; Close will not close it.
func FromPool(pool *pgxpool.Pool) *DB { return &DB{pool: pool, q: pool} }

// Pool exposes the underlying pool for anything not covered here.
func (db *DB) Pool() *pgxpool.Pool { return db.pool }

// Close closes the pool if Open created it.
func (db *DB) Close() {
	if db.ownsPool && db.pool != nil {
		db.pool.Close()
	}
}

// InTx runs fn in a transaction: committed if fn returns nil, rolled back
// otherwise. The *DB passed to fn runs everything inside that transaction.
func (db *DB) InTx(ctx context.Context, fn func(tx *DB) error) (err error) {
	if _, nested := db.q.(pgx.Tx); nested {
		return fn(db)
	}
	tx, err := db.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()
	if err = fn(&DB{pool: db.pool, q: tx}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Exec runs a statement and returns the number of affected rows.
func (db *DB) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	tag, err := db.q.Exec(ctx, sql, args...)
	return tag.RowsAffected(), err
}

// Query returns all rows as maps keyed by column name.
func (db *DB) Query(ctx context.Context, sql string, args ...any) ([]map[string]any, error) {
	rows, err := db.q.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToMap)
}

// QueryRow returns the first row as a map, or nil if there is none.
func (db *DB) QueryRow(ctx context.Context, sql string, args ...any) (map[string]any, error) {
	rows, err := db.Query(ctx, sql, args...)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	return rows[0], nil
}

// ---------------------------------------------------------------------------
// schema management
// ---------------------------------------------------------------------------

// Ident quotes a table's (schema-qualified) name.
func Ident(t *postland.Table) string {
	if t.Schema != "" {
		return pgx.Identifier{t.Schema, t.Name}.Sanitize()
	}
	return pgx.Identifier{t.Name}.Sanitize()
}

func quote(name string) string { return pgx.Identifier{name}.Sanitize() }

// ColumnDDL renders one column definition.
func ColumnDDL(c postland.Column) string {
	var b strings.Builder
	b.WriteString(quote(c.Name) + " " + c.Type.SQL)
	if c.NotNull || c.PrimaryKey {
		b.WriteString(" NOT NULL")
	}
	if c.Unique {
		b.WriteString(" UNIQUE")
	}
	if c.Default != "" {
		b.WriteString(" DEFAULT " + c.Default)
	}
	if c.References != "" {
		b.WriteString(" REFERENCES " + c.References)
	}
	return b.String()
}

// CreateTableSQL renders CREATE TABLE IF NOT EXISTS for t.
func CreateTableSQL(t *postland.Table) string {
	items := make([]string, 0, len(t.Columns)+1)
	for _, c := range t.Columns {
		items = append(items, ColumnDDL(c))
	}
	if pk := t.PrimaryKey(); len(pk) > 0 {
		items = append(items, "PRIMARY KEY ("+quoteAll(pk)+")")
	}
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n  %s\n)", Ident(t), strings.Join(items, ",\n  "))
}

// TableExists reports whether t exists.
func (db *DB) TableExists(ctx context.Context, t *postland.Table) (bool, error) {
	var exists bool
	err := db.q.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", Ident(t)).Scan(&exists)
	return exists, err
}

// ExistingColumns lists the columns t currently has in the database.
func (db *DB) ExistingColumns(ctx context.Context, t *postland.Table) ([]string, error) {
	rows, err := db.q.Query(ctx,
		`SELECT column_name FROM information_schema.columns
		 WHERE table_schema = coalesce(nullif($1, ''), current_schema()) AND table_name = $2
		 ORDER BY ordinal_position`, t.Schema, t.Name)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// CreateTable creates the schema, table and indexes if missing. With
// addMissingColumns, columns added to the definition later are added to the
// existing table, so definitions can evolve.
func (db *DB) CreateTable(ctx context.Context, t *postland.Table, addMissingColumns bool) error {
	if err := t.Validate(); err != nil {
		return err
	}
	if t.Schema != "" {
		if _, err := db.q.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+quote(t.Schema)); err != nil {
			return err
		}
	}
	if _, err := db.q.Exec(ctx, CreateTableSQL(t)); err != nil {
		return fmt.Errorf("create %s: %w", t.QualifiedName(), err)
	}
	if addMissingColumns {
		existing, err := db.ExistingColumns(ctx, t)
		if err != nil {
			return err
		}
		have := map[string]bool{}
		for _, c := range existing {
			have[c] = true
		}
		for _, c := range t.Columns {
			if !have[c.Name] {
				sql := fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s", Ident(t), ColumnDDL(c))
				if _, err := db.q.Exec(ctx, sql); err != nil {
					return fmt.Errorf("add column %s.%s: %w", t.QualifiedName(), c.Name, err)
				}
			}
		}
	}
	for _, cols := range t.Indexes {
		name := fmt.Sprintf("ix_%s_%s", t.Name, strings.Join(cols, "_"))
		if len(name) > 63 {
			name = name[:63]
		}
		sql := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (%s)", quote(name), Ident(t), quoteAll(cols))
		if _, err := db.q.Exec(ctx, sql); err != nil {
			return fmt.Errorf("create index %s: %w", name, err)
		}
	}
	return nil
}

// DropTable drops t if it exists.
func (db *DB) DropTable(ctx context.Context, t *postland.Table, cascade bool) error {
	sql := "DROP TABLE IF EXISTS " + Ident(t)
	if cascade {
		sql += " CASCADE"
	}
	_, err := db.q.Exec(ctx, sql)
	return err
}

// Truncate empties the given tables in one statement.
func (db *DB) Truncate(ctx context.Context, tables ...*postland.Table) error {
	if len(tables) == 0 {
		return nil
	}
	names := make([]string, len(tables))
	for i, t := range tables {
		names[i] = Ident(t)
	}
	_, err := db.q.Exec(ctx, "TRUNCATE "+strings.Join(names, ", "))
	return err
}

// ---------------------------------------------------------------------------
// writing
// ---------------------------------------------------------------------------

// WriteSQL renders the INSERT statement for columns under mode.
func WriteSQL(t *postland.Table, columns []string, mode Mode) (string, error) {
	if !mode.valid() {
		return "", fmt.Errorf("unknown write mode %q", mode)
	}
	ph := make([]string, len(columns))
	for i := range ph {
		ph[i] = fmt.Sprintf("$%d", i+1)
	}
	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", Ident(t), quoteAll(columns), strings.Join(ph, ", "))
	pk := t.PrimaryKey()
	switch {
	case mode == Ignore:
		sql += " ON CONFLICT DO NOTHING"
	case mode == Upsert && len(pk) > 0:
		isPK := map[string]bool{}
		for _, c := range pk {
			isPK[c] = true
		}
		var sets []string
		for _, c := range columns {
			if !isPK[c] {
				sets = append(sets, fmt.Sprintf("%s = EXCLUDED.%s", quote(c), quote(c)))
			}
		}
		if len(sets) == 0 {
			sql += fmt.Sprintf(" ON CONFLICT (%s) DO NOTHING", quoteAll(pk))
		} else {
			sql += fmt.Sprintf(" ON CONFLICT (%s) DO UPDATE SET %s", quoteAll(pk), strings.Join(sets, ", "))
		}
	}
	return sql, nil
}

// Write stores rows (keyed by column name) in t. Rows are sent in a single
// pgx batch. In Upsert mode, rows repeating a primary key within the call are
// collapsed so the last one wins. It does not open a transaction itself; use
// InTx to make several writes atomic.
func (db *DB) Write(ctx context.Context, t *postland.Table, rows []postland.Row, mode Mode) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	cols := columnsOf(t, rows)
	sql, err := WriteSQL(t, cols, mode)
	if err != nil {
		return 0, err
	}
	if mode == Upsert {
		rows = dedupeLast(rows, t.PrimaryKey())
	}
	batch := &pgx.Batch{}
	for _, row := range rows {
		args := make([]any, len(cols))
		for i, name := range cols {
			v := row[name]
			if col, _ := t.Column(name); col.Type.IsJSON && v != nil {
				b, err := json.Marshal(v)
				if err != nil {
					return 0, fmt.Errorf("%s.%s: encode json: %w", t.QualifiedName(), name, err)
				}
				v = json.RawMessage(b)
			}
			args[i] = v
		}
		batch.Queue(sql, args...)
	}
	br := db.q.SendBatch(ctx, batch)
	for range rows {
		if _, err := br.Exec(); err != nil {
			br.Close()
			return 0, fmt.Errorf("write %s: %w", t.QualifiedName(), err)
		}
	}
	return len(rows), br.Close()
}

// columnsOf returns the table's columns present in any row, in table order.
func columnsOf(t *postland.Table, rows []postland.Row) []string {
	present := map[string]bool{}
	for _, r := range rows {
		for k := range r {
			present[k] = true
		}
	}
	var cols []string
	for _, c := range t.Columns {
		if present[c.Name] {
			cols = append(cols, c.Name)
		}
	}
	return cols
}

func dedupeLast(rows []postland.Row, pk []string) []postland.Row {
	if len(pk) == 0 {
		return rows
	}
	last := map[string]int{}
	keys := make([]string, len(rows))
	for i, r := range rows {
		parts := make([]string, len(pk))
		for j, c := range pk {
			parts[j] = fmt.Sprintf("%T:%v", r[c], r[c])
		}
		keys[i] = strings.Join(parts, "\x00")
		last[keys[i]] = i
	}
	if len(last) == len(rows) {
		return rows
	}
	out := make([]postland.Row, 0, len(last))
	for i, r := range rows {
		if last[keys[i]] == i {
			out = append(out, r)
		}
	}
	return out
}

func quoteAll(names []string) string {
	q := make([]string, len(names))
	for i, n := range names {
		q[i] = quote(n)
	}
	return strings.Join(q, ", ")
}

var errNoDB = errors.New("postgres: sink needs a DB")
