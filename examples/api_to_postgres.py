"""Load users (with nested address/company) and their todos from
jsonplaceholder.typicode.com into PostgreSQL.

    export DATABASE_URL=postgresql://user:pass@localhost:5432/mydb
    python examples/api_to_postgres.py
"""

import logging
import os

from postland import (
    BIGINT, BOOLEAN, DOUBLE, JSONB, TEXT, TIMESTAMPTZ, ApiSource, Column, Field, Nested,
    PageNumberPaginator, Pipeline, PostgresSink, TableMapping, table,
)

logging.basicConfig(level=logging.INFO)
DSN = os.environ.get("DATABASE_URL", "postgresql://postgres@localhost:5432/postgres")
API = os.environ.get("API_BASE", "https://jsonplaceholder.typicode.com")

# 1. Define the tables (independent of where data comes from) -----------------
users = table(
    "users",
    Column("id", BIGINT, primary_key=True),
    Column("name", TEXT, nullable=False),
    Column("email", TEXT),
    Column("city", TEXT),
    Column("lat", DOUBLE),
    Column("company", TEXT),
    Column("raw", JSONB),
    Column("loaded_at", TIMESTAMPTZ, default="now()"),
    schema="demo",
    indexes=["email"],
)
todos = table(
    "todos",
    Column("id", BIGINT, primary_key=True),
    Column("user_id", BIGINT, nullable=False),
    Column("title", TEXT),
    Column("completed", BOOLEAN),
    schema="demo",
    indexes=["user_id"],
)

# 2. Describe how a record becomes rows (paths into the JSON) ----------------
user_mapping = TableMapping(
    users,
    fields={
        "city": "address.city",
        "lat": "address.geo.lat",  # "-37.3159" (string) -> DOUBLE
        "company": Field("company.name", transform=lambda v: v and v.strip()),
        "raw": "$",  # keep the whole original record
    },
)
todo_mapping = TableMapping(todos, fields={"user_id": "userId"})

# 3. Plug a source into a sink ------------------------------------------------
if __name__ == "__main__":
    sink = PostgresSink(DSN)  # upsert by primary key
    print(Pipeline(ApiSource(f"{API}/users"), sink, user_mapping).run())

    todo_source = ApiSource(
        f"{API}/todos",
        paginator=PageNumberPaginator(page_param="_page", size_param="_limit", page_size=50),
    )
    print(Pipeline(todo_source, PostgresSink(DSN), todo_mapping).run())


# A nested-array example (an API returning orders with line items) looks like:
#
#   orders_mapping = TableMapping(
#       orders,
#       fields={"customer_email": "customer.email"},
#       children=[Nested("line_items", TableMapping(order_items),
#                        parent_keys={"order_id": "id"}, index_column="line_no")],
#   )
_ = Nested  # referenced in the comment above
