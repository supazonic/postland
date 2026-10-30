# postland

A Go package for loading records from any source into typed PostgreSQL tables.

Today it ships an **HTTP API source**. The parts that know where data comes from are kept apart
from the parts that know where it goes. That means a database table, files from SFTP, or
anything else can be plugged in later without touching your table definitions, mappings or the
Postgres writer.

```
  Source  ──records──▶  TableMapping  ──rows──▶  Sink
 (apisource.Source,    (paths, types,           (postgres.Sink,
  SliceSource,          nested arrays →          MemorySink)
  your own…)            child tables)
```

| Package / file | Knows about |
|---|---|
| `postland` – `types.go` | Turning raw values (`"12.5"`, `"2024-01-01T00:00:00Z"`, `"yes"`) into Go values for a column type |
| `postland` – `schema.go` | `Table` / `Column` definitions: names, types, PKs, defaults, indexes |
| `postland` – `mapping.go` | Turning one record into rows: field paths, transforms, nested arrays |
| `postland` – `source.go`, `sink.go` | The `Source` and `Sink` interfaces, plus `SliceSource` and `MemorySink` |
| `postland` – `pipeline.go` | Connecting the pieces, error policy, run stats |
| `postland/apisource` | HTTP/JSON source: pagination, retries, auth |
| `postland/postgres` | A `pgx` v5 wrapper (DDL, insert/upsert) and `postgres.Sink` |

## Install

```bash
go get github.com/supazonic/postland      # Go 1.22+, uses github.com/jackc/pgx/v5
```

## Quick start

```go
import (
    "github.com/supazonic/postland"
    "github.com/supazonic/postland/apisource"
    "github.com/supazonic/postland/postgres"
)

// 1) Define tables: what you store (source-independent)
var orders = postland.NewTable("orders",
    postland.Column{Name: "id", Type: postland.BigInt, PrimaryKey: true},
    postland.Column{Name: "customer_email", Type: postland.Text},
    postland.Column{Name: "total", Type: postland.NumericPS(12, 2)},
    postland.Column{Name: "placed_at", Type: postland.TimestampTZ},
    postland.Column{Name: "tags", Type: postland.ArrayOf(postland.Text)},
    postland.Column{Name: "raw", Type: postland.JSONB},
    postland.Column{Name: "loaded_at", Type: postland.TimestampTZ, Default: "now()"}, // filled by Postgres
).InSchema("shop").Index("customer_email")

var orderItems = postland.NewTable("order_items",
    postland.Column{Name: "order_id", Type: postland.BigInt, PrimaryKey: true},
    postland.Column{Name: "line_no", Type: postland.Integer, PrimaryKey: true},
    postland.Column{Name: "sku", Type: postland.Text, NotNull: true},
    postland.Column{Name: "qty", Type: postland.Integer},
).InSchema("shop")

// 2) Map records to rows: how a record's fields land in columns
var mapping = &postland.TableMapping{
    Table: orders,
    Fields: map[string]postland.Field{
        "customer_email": postland.From("customer.email"), // nested path
        "raw":            postland.Whole,                  // the whole record as JSONB
    }, // unlisted columns read the same-named key
    Children: []postland.Nested{{
        Path:        "line_items",                             // array inside each order...
        Mapping:     &postland.TableMapping{Table: orderItems}, // ...becomes rows in order_items
        ParentKeys:  map[string]string{"order_id": "id"},       // copy the parent's id onto each child
        IndexColumn: "line_no",                                 // array position
    }},
}

func main() {
    ctx := context.Background()

    // 3) Choose a source (where the data comes from)
    source := &apisource.Source{
        URL:         "https://api.example.com/v1/orders",
        Query:       url.Values{"status": {"paid"}},
        Auth:        apisource.Bearer(os.Getenv("API_TOKEN")),
        RecordsPath: "data.items", // where the array sits in the response
        Paginator:   &apisource.Cursor{CursorPath: "meta.next_cursor", Param: "cursor"},
    }

    // 4) Choose a sink and run
    db, err := postgres.Open(ctx, "postgres://user:pw@host:5432/db")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    res, err := (&postland.Pipeline{
        Source:   source,
        Sink:     postgres.NewSink(db),
        Mappings: []*postland.TableMapping{mapping},
        OnError:  postland.Skip,
    }).Run(ctx)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(res) // 250 records in 3 batches -> rows: shop.order_items=731, shop.orders=250; rejected: 2; 1.2s
    for _, r := range res.Rejected {
        log.Printf("rejected: %v", r.Err) // e.g. shop.orders.total: cannot convert "abc" to NUMERIC(12,2): not a number
    }
}
```

Tables and schemas are created when they don't exist. Columns you add to a `Table` definition
later are added with `ALTER TABLE ... ADD COLUMN`.

For a runnable program, see `examples/api_to_postgres/main.go`:

```bash
DATABASE_URL=postgres://user:pw@localhost:5432/db go run ./examples/api_to_postgres
```

## Types

| postland | PostgreSQL | Go value written |
|---|---|---|
| `Text`, `Varchar(n)` | `TEXT`, `VARCHAR(n)` | `string` |
| `SmallInt`, `Integer`, `BigInt` | `SMALLINT`, `INTEGER`, `BIGINT` | `int16`, `int32`, `int64` |
| `Real`, `Double` | `REAL`, `DOUBLE PRECISION` | `float32`, `float64` |
| `Numeric`, `NumericPS(p, s)` | `NUMERIC`, `NUMERIC(p,s)` | exact decimal string |
| `Boolean` | `BOOLEAN` | `bool` |
| `Date`, `Time`, `Timestamp`, `TimestampTZ` | same | `time.Time` / `"15:04:05"` |
| `JSON`, `JSONB` | same | any value, JSON-encoded |
| `UUID`, `Bytea` | same | canonical string / `[]byte` |
| `ArrayOf(T)` | `T[]` | `[]any` |
| `Custom("INET", fn)` | anything else | whatever `fn` returns |

Coercion is lenient where it's safe and strict where it isn't:

- `"42"`, `42.0`, `json.Number("42")` → `42`; `1.5` → error; out-of-range for the type → error.
- `"yes"`, `"t"`, `1` → `true`.
- RFC 3339 / ISO strings (with or without a zone), epoch seconds and epoch milliseconds → timestamps. Values without a zone are treated as UTC.
- `""` → `NULL` for non-text types.
- JSON strings → structured JSONB.
- The API source decodes JSON with `UseNumber`, so large IDs are never rounded through `float64`.

A value that can't be converted returns a `*postland.MappingError` naming `table.column`:

```go
var me *postland.MappingError
if errors.As(err, &me) {
    fmt.Println(me.Table, me.Column, me.Err)
}
```

## Mapping

```go
Fields: map[string]postland.Field{
    "email":   postland.From("customer.email"),               // path into the record
    "sku":     postland.From("items[0].sku"),                 // indexes: [0] or .0
    "raw":     postland.Whole,                                // the record itself ("$")
    "source":  postland.Const("shop-api"),                    // same value on every row
    "country": postland.From("addr.country").WithDefault("US"),
    "name":    postland.From("name").Then(func(v any) (any, error) {   // transform before coercion
        return strings.ToUpper(fmt.Sprint(v)), nil
    }),
    "full":    postland.Compute(func(r any) (any, error) {            // anything else
        m := r.(map[string]any)
        return fmt.Sprintf("%v %v", m["first"], m["last"]), nil
    }),
},
Where: func(r any) bool { return r.(map[string]any)["deleted"] != true }, // skip records
```

- **Nested arrays**: list them in `Children` (`[]postland.Nested`). Nesting can go to any depth. For an array of scalars such as `"tags": ["a","b"]`, map the element with `Fields: map[string]postland.Field{"tag": postland.Whole}`.
- **Multiple mappings**: pass several to `Pipeline.Mappings` to fan one record out to independent tables.
- **Atomic records**: each record is mapped in full before anything is kept. With `OnError: postland.Skip`, a bad record is dropped with all its child rows. With `postland.Abort` (the default), the current batch is not written and the run stops; earlier batches stay committed.

## API source

```go
&apisource.Source{
    URL:         "https://api.example.com/items",
    Method:      http.MethodGet,          // default GET
    Query:       url.Values{"q": {"x"}},
    Header:      http.Header{"X-Tenant": {"acme"}},
    Body:        map[string]any{...},     // JSON body (POST search / GraphQL)
    Auth:        apisource.Bearer(token), // also Basic(u, p), HeaderAuth(name, v), BearerFunc(refresh)
    RecordsPath: "data",
    Paginator:   &apisource.PageNumber{PageSize: 100},
    Client:      &http.Client{Timeout: time.Minute},
    MaxRetries:  3,                       // on 429 / 5xx / network errors; honours Retry-After
    Backoff:     500 * time.Millisecond,  // doubled each retry
    MaxPages:    0,                       // safety limit
}
```

| Paginator | For APIs like |
|---|---|
| none (default `SinglePage`) | One response holds everything |
| `&apisource.PageNumber{Param: "page", PageSize: 100, SizeParam: "per_page", TotalPagesPath: "meta.pages"}` | `?page=2&per_page=100` |
| `&apisource.Offset{Limit: 100, OffsetParam: "offset", LimitParam: "limit", TotalPath: "total"}` | `?offset=200&limit=100` |
| `&apisource.Cursor{CursorPath: "next_cursor", Param: "cursor", HasMorePath: "has_more"}` | Token/cursor APIs |
| `&apisource.NextURL{Path: "links.next"}` / `&apisource.NextURL{}` | A next-page URL in the body / the `Link: rel="next"` header |

Every paginator takes `InBody: true` to put its parameters in the JSON body. For any other style,
implement the interface:

```go
type Paginator interface {
    First(req *apisource.Request) error
    Next(page *apisource.Page) (*apisource.Request, error) // nil = done
}
```

Each page is one batch, and each batch is one transaction.

## Write modes (`postgres.Sink`)

```go
sink := &postgres.Sink{
    DB:         db,
    Mode:       postgres.Upsert,
    TableModes: map[string]postgres.Mode{"order_items": postgres.Insert},
}
```

- **`Upsert`** (default): `INSERT ... ON CONFLICT (pk) DO UPDATE`. Duplicate keys in one batch resolve to the last one. A table without a primary key gets a plain insert.
- **`Insert`**: a plain insert; duplicates raise.
- **`Ignore`**: `ON CONFLICT DO NOTHING`.
- **`Truncate`**: empty the table once at the start of the run, then insert (a full reload).

Rows are sent with a `pgx.Batch`, parents before children, inside one transaction per batch.

## Using the Postgres wrapper directly

```go
db, _ := postgres.Open(ctx, dsn)
defer db.Close()

_ = db.CreateTable(ctx, orders, true) // true = add new columns to an existing table

err := db.InTx(ctx, func(tx *postgres.DB) error {
    _, err := tx.Write(ctx, orders, []postland.Row{{"id": int64(1), "total": "10.00"}}, postgres.Upsert)
    return err
})

rows, _ := db.Query(ctx, "SELECT * FROM shop.orders WHERE id = $1", 1) // []map[string]any
```

## Adding a new source later (database, SFTP, files…)

A source implements one method:

```go
type Source interface {
    Batches(ctx context.Context, fn func(postland.Batch) error) error
}
```

Tables, mappings, sink and pipeline stay exactly as they are. Two sketches:

```go
// Another database table
type SQLSource struct {
    Pool      *pgxpool.Pool
    Query     string
    BatchSize int
}

func (s *SQLSource) Batches(ctx context.Context, fn func(postland.Batch) error) error {
    rows, err := s.Pool.Query(ctx, s.Query)
    if err != nil {
        return err
    }
    defer rows.Close()
    batch := postland.Batch{}
    for rows.Next() {
        rec, err := pgx.RowToMap(rows)
        if err != nil {
            return err
        }
        if batch = append(batch, rec); len(batch) == s.BatchSize {
            if err := fn(batch); err != nil {
                return err
            }
            batch = postland.Batch{}
        }
    }
    if len(batch) > 0 {
        if err := fn(batch); err != nil {
            return err
        }
    }
    return rows.Err()
}

// CSV files over SFTP (github.com/pkg/sftp): one file = one batch
func (s *SFTPCSVSource) Batches(ctx context.Context, fn func(postland.Batch) error) error {
    files, _ := s.Client.Glob(s.Pattern)
    for _, path := range files {
        f, err := s.Client.Open(path)
        if err != nil {
            return err
        }
        records, err := readCSVAsMaps(f) // []map[string]any keyed by header
        f.Close()
        if err != nil {
            return err
        }
        if err := fn(records); err != nil {
            return err
        }
    }
    return nil
}

// ...and the same mapping that was used for the API:
(&postland.Pipeline{Source: sftpSource, Sink: postgres.NewSink(db), Mappings: []*postland.TableMapping{mapping}}).Run(ctx)
```

For quick jobs, `postland.SliceSource{Records: recs}` serves records you already have in memory,
and `postland.SourceFunc` turns any function into a source.

You can swap the sink the same way: implement `Prepare` and `Write` from `postland.Sink`.
`postland.MemorySink` is included for tests and dry runs.

## Tests

```bash
go test ./...                                                            # unit tests
POSTLAND_TEST_DSN=postgres://postgres@localhost:5432/postgres go test ./...  # + Postgres integration tests
```
