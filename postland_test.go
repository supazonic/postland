package postland

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func decode(t *testing.T, s string) any {
	t.Helper()
	var v any
	d := json.NewDecoder(strings.NewReader(s))
	d.UseNumber()
	if err := d.Decode(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestLookup(t *testing.T) {
	rec := decode(t, `{"a":{"b":[{"c":1},{"c":2}]},"x":null}`)
	cases := map[string]any{"a.b[1].c": json.Number("2"), "a.b.0.c": json.Number("1")}
	for path, want := range cases {
		if got, ok := Lookup(rec, path); !ok || got != want {
			t.Errorf("Lookup(%q) = %v, %v; want %v", path, got, ok, want)
		}
	}
	if got, _ := Lookup(rec, "$"); !reflect.DeepEqual(got, rec) {
		t.Error("$ should return the record")
	}
	if _, ok := Lookup(rec, "a.zzz"); ok {
		t.Error("missing key should not be found")
	}
	if _, ok := Lookup(rec, "x.y"); ok {
		t.Error("path through null should not be found")
	}
	type item struct{ SKU string }
	if got, ok := Lookup(map[string]any{"items": []item{{"A"}}}, "items[0].SKU"); !ok || got != "A" {
		t.Errorf("struct lookup = %v", got)
	}
}

func TestCoerce(t *testing.T) {
	utc := time.Date(2024, 2, 3, 10, 0, 0, 0, time.UTC)
	cases := []struct {
		typ  DataType
		in   any
		want any
	}{
		{Integer, "42", int32(42)},
		{Integer, json.Number("42"), int32(42)},
		{Integer, 42.0, int32(42)},
		{BigInt, "1e3", int64(1000)},
		{BigInt, json.Number("9007199254740993"), int64(9007199254740993)},
		{Numeric, "12.50", "12.5"},
		{Numeric, json.Number("7"), "7"},
		{NumericPS(10, 2), 3.14159, "3.14"},
		{NumericPS(10, 2), "10.005", "10.01"},
		{Boolean, "yes", true},
		{Boolean, 0.0, false},
		{Date, "2024-02-03T10:00:00Z", time.Date(2024, 2, 3, 0, 0, 0, 0, time.UTC)},
		{TimestampTZ, "2024-02-03T10:00:00Z", utc},
		{TimestampTZ, "2024-02-03 12:00:00+02:00", utc},
		{TimestampTZ, json.Number("1706954400000"), utc},
		{TimestampTZ, json.Number("1706954400"), utc},
		{Timestamp, "2024-02-03T10:00:00", utc},
		{Time, "10:30", "10:30:00"},
		{Text, map[string]any{"a": 1}, `{"a":1}`},
		{Text, "", ""},
		{Integer, "", nil},
		{JSONB, `{"a":1}`, map[string]any{"a": json.Number("1")}},
		{ArrayOf(Integer), []any{"1", 2.0}, []any{int32(1), int32(2)}},
		{ArrayOf(Text), "solo", []any{"solo"}},
		{UUID, "{6BA7B810-9DAD-11D1-80B4-00C04FD430C8}", "6ba7b810-9dad-11d1-80b4-00c04fd430c8"},
	}
	for _, c := range cases {
		got, err := c.typ.Coerce(c.in)
		if err != nil {
			t.Errorf("%s(%#v): %v", c.typ, c.in, err)
			continue
		}
		if tm, ok := got.(time.Time); ok {
			if !tm.Equal(c.want.(time.Time)) {
				t.Errorf("%s(%#v) = %v; want %v", c.typ, c.in, got, c.want)
			}
			continue
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s(%#v) = %#v; want %#v", c.typ, c.in, got, c.want)
		}
	}
}

func TestCoerceErrors(t *testing.T) {
	cases := []struct {
		typ DataType
		in  any
	}{
		{Integer, "abc"}, {Integer, 1.5}, {Integer, int64(1) << 40}, {Boolean, "maybe"},
		{Varchar(3), "abcd"}, {UUID, "nope"}, {TimestampTZ, "yesterday"},
	}
	for _, c := range cases {
		if _, err := c.typ.Coerce(c.in); err == nil {
			t.Errorf("%s(%#v): expected error", c.typ, c.in)
		}
	}
}

var (
	orders = NewTable("orders",
		Column{Name: "id", Type: BigInt, PrimaryKey: true},
		Column{Name: "customer_email", Type: Text},
		Column{Name: "total", Type: NumericPS(12, 2)},
		Column{Name: "source", Type: Text},
		Column{Name: "raw", Type: JSONB},
		Column{Name: "loaded_at", Type: TimestampTZ, Default: "now()"},
	)
	items = NewTable("order_items",
		Column{Name: "order_id", Type: BigInt, PrimaryKey: true},
		Column{Name: "line_no", Type: Integer, PrimaryKey: true},
		Column{Name: "sku", Type: Text, NotNull: true},
		Column{Name: "qty", Type: Integer},
	)
	itemTags = NewTable("order_item_tags",
		Column{Name: "sku", Type: Text},
		Column{Name: "tag", Type: Text},
	)
)

const ordersJSON = `[
 {"id":"1","customer":{"email":"a@x.io"},"total":"10.005",
  "items":[{"sku":"A","qty":"2","tags":["red","big"]},{"sku":"B","qty":1}]},
 {"id":2,"customer":{"email":"b@x.io"},"total":5,"items":[]}
]`

func orderMapping() *TableMapping {
	return &TableMapping{
		Table: orders,
		Fields: map[string]Field{
			"customer_email": From("customer.email"),
			"source":         Const("shop-api"),
			"raw":            Whole,
		},
		Children: []Nested{{
			Path: "items",
			Mapping: &TableMapping{
				Table: items,
				Children: []Nested{{
					Path:       "tags",
					Mapping:    &TableMapping{Table: itemTags, Fields: map[string]Field{"tag": Whole}},
					ParentKeys: map[string]string{"sku": "sku"},
				}},
			},
			ParentKeys:  map[string]string{"order_id": "id"},
			IndexColumn: "line_no",
		}},
	}
}

func TestMappingNested(t *testing.T) {
	m := orderMapping()
	all := NewRows()
	for _, rec := range decode(t, ordersJSON).([]any) {
		rows, err := m.Map(rec)
		if err != nil {
			t.Fatal(err)
		}
		all.Merge(rows)
	}
	var names []string
	for _, tb := range all.Tables() {
		names = append(names, tb.Name)
	}
	if strings.Join(names, ",") != "orders,order_items,order_item_tags" {
		t.Fatalf("table order = %v", names)
	}
	o := all.For(orders)
	if o[0]["id"] != int64(1) || o[0]["customer_email"] != "a@x.io" || o[0]["total"] != "10.01" ||
		o[0]["source"] != "shop-api" {
		t.Errorf("order row = %#v", o[0])
	}
	if _, ok := o[0]["loaded_at"]; ok {
		t.Error("columns with a SQL default should be left to the database")
	}
	if raw := o[0]["raw"].(map[string]any); raw["id"] != "1" {
		t.Errorf("raw = %#v", raw)
	}
	wantItems := []Row{
		{"order_id": int64(1), "line_no": int32(0), "sku": "A", "qty": int32(2)},
		{"order_id": int64(1), "line_no": int32(1), "sku": "B", "qty": int32(1)},
	}
	if !reflect.DeepEqual(all.For(items), wantItems) {
		t.Errorf("items = %#v", all.For(items))
	}
	tags := all.For(itemTags)
	if len(tags) != 2 || tags[0]["tag"] != "red" || tags[1]["sku"] != "A" {
		t.Errorf("tags = %#v", tags)
	}
}

func TestMappingValidation(t *testing.T) {
	m := &TableMapping{Table: orders, Fields: map[string]Field{"nope": From("x")}}
	if err := m.Compile(); err == nil {
		t.Error("expected unknown-field error")
	}
	bad := &TableMapping{Table: orders, Children: []Nested{{Path: "items",
		Mapping: &TableMapping{Table: items}, ParentKeys: map[string]string{"nope": "id"}}}}
	if err := bad.Compile(); err == nil {
		t.Error("expected unknown parent-key column error")
	}
}

func TestPipelineSkip(t *testing.T) {
	recs := decode(t, ordersJSON).([]any)
	recs = append(recs,
		decode(t, `{"id":"not-a-number","items":[]}`),
		decode(t, `{"id":3,"items":[{"qty":1}]}`), // missing required sku
	)
	sink := &MemorySink{}
	p := &Pipeline{Source: SliceSource{Records: recs, BatchSize: 2}, Sink: sink,
		Mappings: []*TableMapping{orderMapping()}, OnError: Skip}
	res, err := p.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Records != 4 || res.Batches != 2 || res.RejectedCount != 2 {
		t.Fatalf("result = %s", res)
	}
	if !strings.Contains(res.Rejected[0].Err.Error(), "orders.id") ||
		!strings.Contains(res.Rejected[1].Err.Error(), "order_items.sku") {
		t.Errorf("rejected = %v", res.Rejected)
	}
	// the rejected order's parent row must not be written either
	if got := len(sink.Data["orders"]); got != 2 {
		t.Errorf("orders written = %d", got)
	}
	var me *MappingError
	if !errors.As(res.Rejected[0].Err, &me) || me.Column != "id" {
		t.Errorf("want *MappingError, got %v", res.Rejected[0].Err)
	}
}

func TestPipelineAbortAndLimits(t *testing.T) {
	p := &Pipeline{Source: SliceSource{Records: []Record{map[string]any{"id": "x"}}}, Sink: &MemorySink{},
		Mappings: []*TableMapping{{Table: orders}}}
	if _, err := p.Run(context.Background()); err == nil {
		t.Error("expected mapping error")
	}

	recs := make([]Record, 10)
	for i := range recs {
		recs[i] = map[string]any{"id": i}
	}
	p = &Pipeline{Source: SliceSource{Records: recs, BatchSize: 3}, Sink: &MemorySink{},
		Mappings: []*TableMapping{{Table: orders}}, MaxBatches: 2}
	res, err := p.Run(context.Background())
	if err != nil || res.Batches != 2 || res.Records != 6 {
		t.Errorf("res=%v err=%v", res, err)
	}
}

func TestFieldHelpers(t *testing.T) {
	tb := NewTable("t", Column{Name: "name", Type: Text})
	m := &TableMapping{
		Table: tb,
		Fields: map[string]Field{"name": From("n").WithDefault("anon").Then(func(v any) (any, error) {
			return strings.ToUpper(v.(string)), nil
		})},
		Where: func(r any) bool { keep, ok := r.(map[string]any)["keep"]; return !ok || keep == true },
	}
	all := NewRows()
	for _, rec := range []any{map[string]any{"n": "bob"}, map[string]any{}, map[string]any{"n": "x", "keep": false}} {
		r, err := m.Map(rec)
		if err != nil {
			t.Fatal(err)
		}
		all.Merge(r)
	}
	want := []Row{{"name": "BOB"}, {"name": "ANON"}}
	if !reflect.DeepEqual(all.For(tb), want) {
		t.Errorf("rows = %#v", all.For(tb))
	}
}
