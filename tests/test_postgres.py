"""Integration tests against a real PostgreSQL (set POSTLAND_TEST_DSN)."""

from decimal import Decimal

import pytest

from postland import (
    BIGINT, INTEGER, JSONB, TEXT, TIMESTAMPTZ, ApiSource, Column, IterableSource, Nested,
    PageNumberPaginator, Pipeline, PostgresSink, TableMapping, array_of, numeric, table,
)
from postland.mapping import MappingError

from conftest import FakeSession


def make_tables(schema):
    orders = table(
        "orders",
        Column("id", BIGINT, primary_key=True),
        Column("email", TEXT),
        Column("total", numeric(12, 2)),
        Column("tags", array_of(TEXT)),
        Column("raw", JSONB),
        Column("loaded_at", TIMESTAMPTZ, nullable=False, default="now()"),
        schema=schema,
        indexes=["email"],
    )
    items = table(
        "order_items",
        Column("order_id", BIGINT, primary_key=True, references=f'"{schema}".orders(id)'),
        Column("line_no", INTEGER, primary_key=True),
        Column("sku", TEXT),
        Column("qty", INTEGER),
        schema=schema,
    )
    mapping = TableMapping(
        orders,
        fields={"email": "customer.email", "raw": "$"},
        children=[Nested("items", TableMapping(items), parent_keys={"order_id": "id"},
                         index_column="line_no")],
    )
    return orders, items, mapping


def api_pages(pages):
    def handler(method, url, params, body):
        page = params["page"]
        return {"data": pages[page - 1] if page <= len(pages) else []}
    return FakeSession(handler)


def test_api_to_postgres_upsert(pg):
    orders, items, mapping = make_tables(pg.test_schema)
    pages = [
        [{"id": 1, "customer": {"email": "a@x"}, "total": "9.99", "tags": ["n"],
          "items": [{"sku": "A", "qty": 1}, {"sku": "B", "qty": "2"}]},
         {"id": 2, "customer": {"email": "b@x"}, "total": 1, "items": []}],
        [{"id": 1, "customer": {"email": "a-updated@x"}, "total": "10", "items": [{"sku": "A", "qty": 5}]}],
    ]
    source = ApiSource("https://api.test/orders", records_path="data", session=api_pages(pages),
                       paginator=PageNumberPaginator(size_param=None))
    result = Pipeline(source, PostgresSink(db=pg), mapping).run()

    assert result.batches == 2 and result.records == 3
    rows = pg.fetch_all(f'SELECT * FROM "{pg.test_schema}".orders ORDER BY id')
    assert [(r["id"], r["email"], r["total"]) for r in rows] == [
        (1, "a-updated@x", Decimal("10.00")), (2, "b@x", Decimal("1.00"))]
    assert rows[1]["raw"]["customer"]["email"] == "b@x"
    assert rows[0]["loaded_at"] is not None
    item_rows = pg.fetch_all(f'SELECT * FROM "{pg.test_schema}".order_items ORDER BY line_no')
    assert [(r["sku"], r["qty"]) for r in item_rows] == [("A", 5), ("B", 2)]
    assert pg.fetch_one("SELECT count(*) AS n FROM pg_indexes WHERE schemaname = %s AND indexname = %s",
                        (pg.test_schema, "ix_orders_email"))["n"] == 1


def test_batch_rolled_back_on_error(pg):
    orders, items, mapping = make_tables(pg.test_schema)
    records = [{"id": 1, "items": []}, {"id": 2, "items": []}, {"id": "bad", "items": []}]
    with pytest.raises(MappingError):
        Pipeline(IterableSource(records, batch_size=2), PostgresSink(db=pg), mapping).run()
    ids = [r["id"] for r in pg.fetch_all(f'SELECT id FROM "{pg.test_schema}".orders ORDER BY id')]
    assert ids == [1, 2]  # first batch committed, failing batch not


def test_modes_and_schema_evolution(pg):
    s = pg.test_schema
    t1 = table("events", Column("id", BIGINT, primary_key=True), Column("name", TEXT), schema=s)
    Pipeline(IterableSource([{"id": 1, "name": "a"}]), PostgresSink(db=pg), TableMapping(t1)).run()

    # new column added to the definition -> ALTER TABLE ADD COLUMN; truncate mode reloads
    t2 = table("events", Column("id", BIGINT, primary_key=True), Column("name", TEXT),
               Column("score", INTEGER), schema=s)
    Pipeline(IterableSource([{"id": 5, "name": "b", "score": "7"}]),
             PostgresSink(db=pg, mode="truncate"), TableMapping(t2)).run()
    assert pg.fetch_all(f'SELECT id, name, score FROM "{s}".events') == [{"id": 5, "name": "b", "score": 7}]

    # ignore mode keeps the existing row
    Pipeline(IterableSource([{"id": 5, "name": "zzz"}, {"id": 6, "name": "c"}]),
             PostgresSink(db=pg, mode="ignore"), TableMapping(t2)).run()
    assert [r["name"] for r in pg.fetch_all(f'SELECT name FROM "{s}".events ORDER BY id')] == ["b", "c"]

    # upsert with duplicate keys inside one batch: last one wins
    Pipeline(IterableSource([{"id": 7, "name": "x"}, {"id": 7, "name": "y"}]),
             PostgresSink(db=pg), TableMapping(t2)).run()
    assert pg.fetch_one(f'SELECT name FROM "{s}".events WHERE id = 7')["name"] == "y"
