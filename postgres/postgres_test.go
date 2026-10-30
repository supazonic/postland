package postgres

// Integration tests: set POSTLAND_TEST_DSN to a PostgreSQL connection URL.

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/supazonic/postland"
	"github.com/supazonic/postland/apisource"
)

func testDB(t *testing.T) (*DB, string) {
	t.Helper()
	dsn := os.Getenv("POSTLAND_TEST_DSN")
	if dsn == "" {
		t.Skip("POSTLAND_TEST_DSN not set")
	}
	ctx := context.Background()
	db, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("t_%08x", rand.Uint32())
	t.Cleanup(func() {
		_, _ = db.Exec(ctx, "DROP SCHEMA IF EXISTS "+quote(schema)+" CASCADE")
		db.Close()
	})
	return db, schema
}

func orderTables(schema string) (*postland.Table, *postland.Table, *postland.TableMapping) {
	orders := postland.NewTable("orders",
		postland.Column{Name: "id", Type: postland.BigInt, PrimaryKey: true},
		postland.Column{Name: "email", Type: postland.Text},
		postland.Column{Name: "total", Type: postland.NumericPS(12, 2)},
		postland.Column{Name: "tags", Type: postland.ArrayOf(postland.Text)},
		postland.Column{Name: "placed_on", Type: postland.Date},
		postland.Column{Name: "ext_id", Type: postland.UUID},
		postland.Column{Name: "raw", Type: postland.JSONB},
		postland.Column{Name: "loaded_at", Type: postland.TimestampTZ, NotNull: true, Default: "now()"},
	).InSchema(schema).Index("email")
	items := postland.NewTable("order_items",
		postland.Column{Name: "order_id", Type: postland.BigInt, PrimaryKey: true,
			References: quote(schema) + ".orders(id)"},
		postland.Column{Name: "line_no", Type: postland.Integer, PrimaryKey: true},
		postland.Column{Name: "sku", Type: postland.Text},
		postland.Column{Name: "qty", Type: postland.Integer},
		postland.Column{Name: "at", Type: postland.Time},
	).InSchema(schema)
	m := &postland.TableMapping{
		Table:  orders,
		Fields: map[string]postland.Field{"email": postland.From("customer.email"), "raw": postland.Whole},
		Children: []postland.Nested{{Path: "items", Mapping: &postland.TableMapping{Table: items},
			ParentKeys: map[string]string{"order_id": "id"}, IndexColumn: "line_no"}},
	}
	return orders, items, m
}

func TestAPIToPostgresUpsert(t *testing.T) {
	db, schema := testDB(t)
	ctx := context.Background()
	_, _, mapping := orderTables(schema)

	pages := []string{
		`[{"id":1,"customer":{"email":"a@x"},"total":"9.99","tags":["n","sale"],"placed_on":"2024-05-01T10:00:00Z",
		   "ext_id":"6ba7b810-9dad-11d1-80b4-00c04fd430c8",
		   "items":[{"sku":"A","qty":1,"at":"10:30"},{"sku":"B","qty":"2"}]},
		  {"id":2,"customer":{"email":"b@x"},"total":1,"items":[]}]`,
		`[{"id":1,"customer":{"email":"a-updated@x"},"total":"10","items":[{"sku":"A","qty":5}]}]`,
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var page int
		fmt.Sscan(r.URL.Query().Get("page"), &page)
		if page > len(pages) {
			fmt.Fprint(w, `{"data":[]}`)
			return
		}
		fmt.Fprintf(w, `{"data":%s}`, pages[page-1])
	}))
	defer srv.Close()

	p := &postland.Pipeline{
		Source:   &apisource.Source{URL: srv.URL, RecordsPath: "data", Paginator: &apisource.PageNumber{}},
		Sink:     NewSink(db),
		Mappings: []*postland.TableMapping{mapping},
	}
	res, err := p.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Batches != 2 || res.Records != 3 {
		t.Fatalf("result = %s", res)
	}

	rows, err := db.Query(ctx, fmt.Sprintf(
		`SELECT id, email, total::text AS total, tags, placed_on::text AS placed_on, ext_id::text AS ext_id,
		        raw->'customer'->>'email' AS raw_email, loaded_at IS NOT NULL AS loaded
		 FROM %s.orders ORDER BY id`, quote(schema)))
	if err != nil {
		t.Fatal(err)
	}
	got := fmt.Sprint(rows)
	want := "[map[email:a-updated@x ext_id:<nil> id:1 loaded:true placed_on:<nil> raw_email:a-updated@x tags:<nil> total:10.00] " +
		"map[email:b@x ext_id:<nil> id:2 loaded:true placed_on:<nil> raw_email:b@x tags:<nil> total:1.00]]"
	if got != want {
		t.Errorf("orders:\n got %s\nwant %s", got, want)
	}

	items, err := db.Query(ctx, fmt.Sprintf(`SELECT sku, qty FROM %s.order_items ORDER BY line_no`, quote(schema)))
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(items) != "[map[qty:5 sku:A] map[qty:2 sku:B]]" {
		t.Errorf("items = %v", items)
	}
	idx, _ := db.QueryRow(ctx, "SELECT count(*) AS n FROM pg_indexes WHERE schemaname=$1 AND indexname='ix_orders_email'", schema)
	if idx["n"] != int64(1) {
		t.Errorf("index missing: %v", idx)
	}
}

func TestAllTypesRoundTrip(t *testing.T) {
	db, schema := testDB(t)
	ctx := context.Background()
	orders, items, mapping := orderTables(schema)
	rec := map[string]any{"id": 7, "customer": map[string]any{"email": "z@x"}, "total": "3.456",
		"tags": []any{"a", "b"}, "placed_on": "2024-05-01", "ext_id": "6BA7B810-9DAD-11D1-80B4-00C04FD430C8",
		"items": []any{map[string]any{"sku": "S", "qty": 1, "at": "08:15:30"}}}
	p := &postland.Pipeline{Source: postland.SliceSource{Records: []postland.Record{rec}},
		Sink: NewSink(db), Mappings: []*postland.TableMapping{mapping}}
	if _, err := p.Run(ctx); err != nil {
		t.Fatal(err)
	}
	row, err := db.QueryRow(ctx, fmt.Sprintf(`SELECT total::text, tags, placed_on::text, ext_id::text,
		raw->>'id' AS raw_id FROM %s`, Ident(orders)))
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(row) != "map[ext_id:6ba7b810-9dad-11d1-80b4-00c04fd430c8 placed_on:2024-05-01 raw_id:7 tags:[a b] total:3.46]" {
		t.Errorf("row = %v", row)
	}
	item, _ := db.QueryRow(ctx, fmt.Sprintf(`SELECT at::text FROM %s`, Ident(items)))
	if item["at"] != "08:15:30" {
		t.Errorf("item = %v", item)
	}
}

func TestBatchRolledBackOnError(t *testing.T) {
	db, schema := testDB(t)
	ctx := context.Background()
	orders, _, mapping := orderTables(schema)
	recs := []postland.Record{
		map[string]any{"id": 1}, map[string]any{"id": 2},
		map[string]any{"id": 3, "items": []any{map[string]any{"sku": "A"}, map[string]any{"sku": "B"}}},
		map[string]any{"id": "bad"},
	}
	p := &postland.Pipeline{Source: postland.SliceSource{Records: recs, BatchSize: 2},
		Sink: NewSink(db), Mappings: []*postland.TableMapping{mapping}}
	_, err := p.Run(ctx)
	var me *postland.MappingError
	if !errors.As(err, &me) {
		t.Fatalf("err = %v", err)
	}
	rows, _ := db.Query(ctx, "SELECT id FROM "+Ident(orders)+" ORDER BY id")
	if fmt.Sprint(rows) != "[map[id:1] map[id:2]]" {
		t.Errorf("first batch committed, failing batch not: %v", rows)
	}
}

func TestModesAndSchemaEvolution(t *testing.T) {
	db, schema := testDB(t)
	ctx := context.Background()
	run := func(sink *Sink, tb *postland.Table, recs ...postland.Record) {
		t.Helper()
		p := &postland.Pipeline{Source: postland.SliceSource{Records: recs}, Sink: sink,
			Mappings: []*postland.TableMapping{{Table: tb}}}
		if _, err := p.Run(ctx); err != nil {
			t.Fatal(err)
		}
	}
	names := func(tb *postland.Table) string {
		rows, err := db.Query(ctx, "SELECT name FROM "+Ident(tb)+" ORDER BY id")
		if err != nil {
			t.Fatal(err)
		}
		return fmt.Sprint(rows)
	}

	t1 := postland.NewTable("events",
		postland.Column{Name: "id", Type: postland.BigInt, PrimaryKey: true},
		postland.Column{Name: "name", Type: postland.Text}).InSchema(schema)
	run(NewSink(db), t1, map[string]any{"id": 1, "name": "a"})

	// a new column in the definition -> ALTER TABLE ADD COLUMN; truncate reloads
	t2 := postland.NewTable("events",
		postland.Column{Name: "id", Type: postland.BigInt, PrimaryKey: true},
		postland.Column{Name: "name", Type: postland.Text},
		postland.Column{Name: "score", Type: postland.Integer}).InSchema(schema)
	run(&Sink{DB: db, Mode: Truncate}, t2, map[string]any{"id": 5, "name": "b", "score": "7"})
	row, _ := db.QueryRow(ctx, "SELECT id, name, score FROM "+Ident(t2))
	if fmt.Sprint(row) != "map[id:5 name:b score:7]" {
		t.Errorf("after truncate: %v", row)
	}

	// ignore keeps the existing row
	run(&Sink{DB: db, Mode: Ignore}, t2, map[string]any{"id": 5, "name": "zzz"}, map[string]any{"id": 6, "name": "c"})
	if got := names(t2); got != "[map[name:b] map[name:c]]" {
		t.Errorf("after ignore: %s", got)
	}

	// upsert with a duplicate key in one batch: last one wins
	run(NewSink(db), t2, map[string]any{"id": 7, "name": "x"}, map[string]any{"id": 7, "name": "y"})
	if got := names(t2); got != "[map[name:b] map[name:c] map[name:y]]" {
		t.Errorf("after upsert: %s", got)
	}

	// insert raises on duplicates, and the batch is rolled back
	p := &postland.Pipeline{Source: postland.SliceSource{Records: []postland.Record{map[string]any{"id": 8, "name": "new"}, map[string]any{"id": 5}}},
		Sink: &Sink{DB: db, TableModes: map[string]Mode{"events": Insert}}, Mappings: []*postland.TableMapping{{Table: t2}}}
	if _, err := p.Run(ctx); err == nil {
		t.Error("expected duplicate key error")
	}
	if got := names(t2); got != "[map[name:b] map[name:c] map[name:y]]" {
		t.Errorf("after failed insert: %s", got)
	}
}

func TestWriteSQL(t *testing.T) {
	tb := postland.NewTable("t",
		postland.Column{Name: "id", Type: postland.BigInt, PrimaryKey: true},
		postland.Column{Name: "v", Type: postland.Text})
	sql, _ := WriteSQL(tb, []string{"id", "v"}, Upsert)
	want := `INSERT INTO "t" ("id", "v") VALUES ($1, $2) ON CONFLICT ("id") DO UPDATE SET "v" = EXCLUDED."v"`
	if sql != want {
		t.Errorf("got  %s\nwant %s", sql, want)
	}
	if _, err := WriteSQL(tb, []string{"id"}, Mode("nope")); err == nil {
		t.Error("expected invalid mode error")
	}
}
