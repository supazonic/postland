# postland

Land records from any source into typed PostgreSQL tables.

Today it ships an **HTTP API source**. The design keeps the parts that know where data comes
from apart from the parts that know where it goes. That means a database table, files from
SFTP, or anything else can be plugged in later without touching your table definitions,
mappings or the Postgres writer.

```
  Source  ──records──▶  TableMapping  ──rows──▶  Sink
 (ApiSource,           (paths, types,           (PostgresSink,
  IterableSource,       nested arrays →          MemorySink)
  your own…)            child tables)
```

| Layer | Module | Knows about |
|---|---|---|
| **Types** | `postland.types` | Turning raw values (`"12.5"`, `"2024-01-01T00:00:00Z"`, `"yes"`) into Python values for a column type |
| **Schema** | `postland.schema` | `Table` / `Column` definitions: names, types, PKs, defaults, indexes |
| **Mapping** | `postland.mapping` | Turning one record (a dict) into rows: field paths, transforms, nested arrays |
| **Sources** | `postland.sources` | Producing batches of records: `ApiSource` (pagination, retries, auth) |
| **Sinks** | `postland.sinks` | Writing rows: `PostgresSink` (DDL, upsert, one transaction per batch) |
| **Postgres** | `postland.postgres` | A small `psycopg` 3 wrapper you can also use directly |
| **Pipeline** | `postland.pipeline` | Connecting the pieces, error policy, run stats |

## Install

```bash
pip install -e .            # requires Python 3.9+, psycopg 3, requests
pip install -e ".[dev]"     # + pytest
```

## Quick start

```python
from postland import *

# 1) Define tables: what you store (source-independent)
orders = table(
    "orders",
    Column("id", BIGINT, primary_key=True),
    Column("customer_email", TEXT),
    Column("total", numeric(12, 2)),
    Column("placed_at", TIMESTAMPTZ),
    Column("tags", array_of(TEXT)),
    Column("raw", JSONB),
    Column("loaded_at", TIMESTAMPTZ, default="now()"),   # filled by Postgres
    schema="shop",
    indexes=["customer_email"],
)
order_items = table(
    "order_items",
    Column("order_id", BIGINT, primary_key=True),
    Column("line_no", INTEGER, primary_key=True),
    Column("sku", TEXT, nullable=False),
    Column("qty", INTEGER),
    schema="shop",
)

# 2) Map records to rows: how a record's fields land in columns
mapping = TableMapping(
    orders,
    fields={
        "customer_email": "customer.email",      # nested path
        "raw": "$",                              # the whole record as JSONB
    },                                           # unlisted columns read the same-named key
    children=[
        Nested("line_items",                     # array inside each order...
               TableMapping(order_items),        # ...becomes rows in order_items
               parent_keys={"order_id": "id"},   # copy the parent's id onto each child
               index_column="line_no"),          # array position
    ],
)

# 3) Choose a source (where the data comes from)
source = ApiSource(
    "https://api.example.com/v1/orders",
    params={"status": "paid"},
    auth=BearerAuth("token"),
    records_path="data.items",                   # where the array sits in the response
    paginator=CursorPaginator(cursor_path="meta.next_cursor", cursor_param="cursor"),
)

# 4) Run it
result = Pipeline(source, PostgresSink("postgresql://user:pw@host/db"), mapping,
                  on_error="skip").run()
print(result)        # 250 records in 3 batches -> rows: shop.orders=250, shop.order_items=731; ...
print(result.rejected[:5])
```

Tables and schemas are created when they don't exist. Columns you add to a `Table`
definition later are added with `ALTER TABLE ... ADD COLUMN`.

See `examples/api_to_postgres.py` for a runnable example.

## Types

`TEXT`, `varchar(n)`, `SMALLINT`, `INTEGER`, `BIGINT`, `REAL`, `DOUBLE`, `NUMERIC`,
`numeric(p, s)`, `BOOLEAN`, `DATE`, `TIME`, `TIMESTAMP`, `TIMESTAMPTZ`, `JSON`, `JSONB`, `UUID`,
`BYTEA`, `array_of(T)`, and `custom("INET")` for anything else.

Coercion is lenient where it's safe and strict where it isn't:

- `"42"`, `42.0` → `42`; `1.5` → error.
- `"yes"`, `"t"`, `1` → `True`.
- ISO strings (including `Z`), epoch seconds and epoch milliseconds → timestamps. Naive values are treated as UTC for `TIMESTAMPTZ`.
- `""` → `NULL` for non-text types.
- JSON strings → structured JSONB.

A value that can't be converted raises `MappingError` naming `table.column`.

## Mapping

- **Paths**: `"a.b"`, `"items[0].sku"`, `"items.0.sku"`, `"$"` (the record itself), or any `callable(record)`.
- **`Field(path, transform=fn, default=x)`**: post-process a value before type coercion.
- **`const(value)`**: the same value on every row (for example a source-system tag).
- **`where=fn`**: on a `TableMapping`, skip records.
- **`Nested(...)`**: explode arrays into child tables. Nests to any depth. For an array of scalars (`"tags": ["a","b"]`), map the value with `fields={"tag": "$"}`.
- **Multiple mappings**: pass a list to `Pipeline` to fan one record out to several independent tables.

Each record is mapped in full before anything is written. With `on_error="skip"`, a bad record is
dropped together with all its child rows. With `on_error="raise"` (the default), the current batch
is rolled back and the run stops; earlier batches stay committed.

## API source

```python
ApiSource(url, method="GET", params=..., headers=..., json=..., auth=...,
          records_path="data", paginator=..., timeout=30, max_retries=3, max_pages=None,
          session=None, parse=None)
```

| Paginator | For APIs like |
|---|---|
| `SinglePage()` (default) | One response holds everything |
| `PageNumberPaginator(page_param="page", page_size=100, size_param="per_page", total_pages_path=None)` | `?page=2&per_page=100` |
| `OffsetPaginator(limit=100, offset_param="offset", limit_param="limit", total_path=None)` | `?offset=200&limit=100` |
| `CursorPaginator(cursor_path="next_cursor", cursor_param="cursor", has_more_path=None)` | Token/cursor APIs |
| `NextUrlPaginator("links.next")` / `NextUrlPaginator(None)` | A next-page URL in the body / the `Link: rel="next"` header |

Every paginator takes `in_body=True` to put its params in the JSON body, for POST search or
GraphQL-style APIs. To support another style, subclass `Paginator` and implement
`next(page) -> ApiRequest | None`.

Requests retry with backoff on 429 and 5xx responses and honour `Retry-After`. Each page is one batch, and each batch is one transaction.

## Write modes (`PostgresSink`)

```python
PostgresSink(dsn, mode="upsert", table_modes={"order_items": "insert"})
```

- **`upsert`** (default): `INSERT ... ON CONFLICT (pk) DO UPDATE`. Duplicate keys in one batch resolve to the last one. A table without a primary key gets a plain insert.
- **`insert`**: a plain insert; duplicates raise.
- **`ignore`**: `ON CONFLICT DO NOTHING`.
- **`truncate`**: empty the table once at the start of the run, then insert (a full reload).

## Using the Postgres wrapper directly

```python
from postland import Postgres

with Postgres("postgresql://...") as db:        # commits on success, rolls back on error
    db.create_table(orders)
    db.write(orders, [{"id": 1, "total": 10}], mode="upsert")
    rows = db.fetch_all("SELECT * FROM shop.orders WHERE id = %s", (1,))
```

## Adding a new source later (database, SFTP, files…)

Implement one method, `batches()`, which yields lists of dicts. Tables, mappings, sink and
pipeline stay exactly as they are.

```python
from postland import Source

class SqlSource(Source):
    def __init__(self, dsn, query, batch_size=1000): ...
    def batches(self):
        with psycopg.connect(self.dsn) as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(self.query)
            while rows := cur.fetchmany(self.batch_size):
                yield rows

class SftpCsvSource(Source):
    def __init__(self, host, user, key, pattern): ...
    def batches(self):
        for path in self.list_files():              # e.g. paramiko
            with self.open(path) as fh:
                yield list(csv.DictReader(io.TextIOWrapper(fh)))
    def close(self): self.sftp.close()

Pipeline(SftpCsvSource(...), PostgresSink(dsn), mapping).run()   # same mapping as the API
```

For quick jobs, `IterableSource(any_iterable_or_generator_fn, batch_size=1000)` wraps
whatever you already have.

You can swap the sink the same way: subclass `Sink` and implement `write(rows_by_table)`.
`MemorySink` is included for tests and dry runs.

## Tests

```bash
pytest                                                   # unit tests
POSTLAND_TEST_DSN=postgresql://postgres@localhost/postgres pytest   # + Postgres integration tests
```
