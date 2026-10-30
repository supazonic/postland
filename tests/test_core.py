from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from postland import (
    BIGINT, BOOLEAN, DATE, INTEGER, JSONB, NUMERIC, TEXT, TIMESTAMPTZ, CoercionError, Column,
    Field, IterableSource, MappingError, MemorySink, Nested, Pipeline, TableMapping, array_of,
    const, get_path, numeric, table, varchar,
)
from postland.paths import MISSING


def test_get_path():
    rec = {"a": {"b": [{"c": 1}, {"c": 2}]}, "x": None}
    assert get_path(rec, "a.b[1].c") == 2
    assert get_path(rec, "a.b.0.c") == 1
    assert get_path(rec, "$") is rec
    assert get_path(rec, "a.zzz") is MISSING
    assert get_path(rec, "x.y", "dflt") == "dflt"
    assert get_path(rec, lambda r: r["a"]["b"][0]) == {"c": 1}


@pytest.mark.parametrize("dtype,raw,expected", [
    (INTEGER, "42", 42),
    (INTEGER, 42.0, 42),
    (BIGINT, "1e3", 1000),
    (NUMERIC, "12.50", Decimal("12.50")),
    (numeric(10, 2), 3.14159, Decimal("3.14")),
    (BOOLEAN, "yes", True),
    (BOOLEAN, 0, False),
    (DATE, "2024-02-03T10:00:00Z", date(2024, 2, 3)),
    (TIMESTAMPTZ, "2024-02-03T10:00:00Z", datetime(2024, 2, 3, 10, tzinfo=timezone.utc)),
    (TIMESTAMPTZ, 1706954400000, datetime(2024, 2, 3, 10, tzinfo=timezone.utc)),
    (TEXT, {"a": 1}, '{"a": 1}'),
    (TEXT, "", ""),
    (INTEGER, "", None),
    (JSONB, '{"a": 1}', {"a": 1}),
    (array_of(INTEGER), ["1", 2], [1, 2]),
])
def test_coercion(dtype, raw, expected):
    assert dtype.coerce(raw) == expected


@pytest.mark.parametrize("dtype,raw", [
    (INTEGER, "abc"), (INTEGER, 1.5), (INTEGER, 2 ** 40), (BOOLEAN, "maybe"), (varchar(3), "abcd"),
])
def test_coercion_errors(dtype, raw):
    with pytest.raises(CoercionError):
        dtype.coerce(raw)


orders = table(
    "orders",
    Column("id", BIGINT, primary_key=True),
    Column("customer_email", TEXT),
    Column("total", numeric(12, 2)),
    Column("placed_at", TIMESTAMPTZ),
    Column("source", TEXT),
    Column("raw", JSONB),
    Column("loaded_at", TIMESTAMPTZ, default="now()"),
)
items = table(
    "order_items",
    Column("order_id", BIGINT, primary_key=True),
    Column("line_no", INTEGER, primary_key=True),
    Column("sku", TEXT, nullable=False),
    Column("qty", INTEGER),
)
item_tags = table(
    "order_item_tags",
    Column("order_id", BIGINT),
    Column("sku", TEXT),
    Column("tag", TEXT),
)

RECORDS = [
    {"id": "1", "customer": {"email": "a@x.io"}, "total": "10.005", "placed_at": "2024-01-01T00:00:00Z",
     "items": [{"sku": "A", "qty": "2", "tags": ["red", "big"]}, {"sku": "B", "qty": 1}]},
    {"id": 2, "customer": {"email": "b@x.io"}, "total": 5, "placed_at": None, "items": []},
]


def order_mapping():
    return TableMapping(
        orders,
        fields={
            "customer_email": "customer.email",
            "source": const("shop-api"),
            "raw": "$",
        },
        children=[Nested(
            "items",
            TableMapping(items, children=[Nested(
                "tags", TableMapping(item_tags, fields={"tag": "$"}),
                parent_keys={"sku": "sku"},
            )]),
            parent_keys={"order_id": "id"},
            index_column="line_no",
        )],
    )


def test_mapping_nested():
    rows = order_mapping().map_many(RECORDS)
    assert [t.name for t in rows] == ["orders", "order_items", "order_item_tags"]
    o = rows[orders]
    assert o[0]["id"] == 1 and o[0]["customer_email"] == "a@x.io"
    assert o[0]["total"] == Decimal("10.01") or o[0]["total"] == Decimal("10.00")
    assert o[0]["source"] == "shop-api" and "loaded_at" not in o[0]
    assert o[0]["raw"]["id"] == "1"
    assert rows[items] == [
        {"order_id": 1, "line_no": 0, "sku": "A", "qty": 2},
        {"order_id": 1, "line_no": 1, "sku": "B", "qty": 1},
    ]
    assert [r["tag"] for r in rows[item_tags]] == ["red", "big"]
    assert rows[item_tags][0]["sku"] == "A"


def test_mapping_validates_fields():
    with pytest.raises(ValueError):
        TableMapping(orders, fields={"nope": "x"})


def test_pipeline_skip_bad_records():
    bad = {"id": "not-a-number", "items": []}
    missing_sku = {"id": 3, "items": [{"qty": 1}]}
    sink = MemorySink()
    result = Pipeline(IterableSource(RECORDS + [bad, missing_sku], batch_size=2), sink,
                      order_mapping(), on_error="skip").run()
    assert result.records == 4 and result.batches == 2
    assert result.rejected_count == 2
    assert "orders.id" in result.rejected[0].error
    assert "order_items.sku" in result.rejected[1].error
    # the rejected order's parent row must not have been written either
    assert [r["id"] for r in sink.tables["orders"]] == [1, 2]


def test_pipeline_raise():
    with pytest.raises(MappingError):
        Pipeline(IterableSource([{"id": "x"}]), MemorySink(), TableMapping(orders)).run()


def test_field_transform_and_where():
    t = table("t", Column("name", TEXT))
    m = TableMapping(t, fields={"name": Field("n", transform=str.upper, default="anon")},
                     where=lambda r: r.get("keep", True))
    rows = m.map_many([{"n": "bob"}, {}, {"n": "x", "keep": False}])
    assert rows[t] == [{"name": "BOB"}, {"name": "ANON"}]
