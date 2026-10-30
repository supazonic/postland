"""A thin, safe wrapper around a PostgreSQL connection (psycopg 3).

Usable on its own (create tables from :class:`~postland.schema.Table`
definitions, insert/upsert rows, run queries) and used by
:class:`~postland.sinks.PostgresSink` inside pipelines.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List, Optional, Sequence

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Json, Jsonb

from .schema import Column, Table

log = logging.getLogger(__name__)

WRITE_MODES = ("insert", "upsert", "ignore", "truncate")


def _ident(table: Table) -> sql.Composable:
    if table.schema:
        return sql.Identifier(table.schema, table.name)
    return sql.Identifier(table.name)


def column_ddl(col: Column) -> sql.Composable:
    parts: List[sql.Composable] = [sql.Identifier(col.name), sql.SQL(col.type.sql)]
    if not col.nullable:
        parts.append(sql.SQL("NOT NULL"))
    if col.unique:
        parts.append(sql.SQL("UNIQUE"))
    if col.default is not None:
        parts.append(sql.SQL("DEFAULT {}").format(sql.SQL(col.default)))
    if col.references:
        parts.append(sql.SQL("REFERENCES {}").format(sql.SQL(col.references)))
    return sql.SQL(" ").join(parts)


def create_table_sql(table: Table) -> sql.Composable:
    items = [column_ddl(c) for c in table.columns]
    if table.primary_key:
        items.append(sql.SQL("PRIMARY KEY ({})").format(
            sql.SQL(", ").join(map(sql.Identifier, table.primary_key))))
    return sql.SQL("CREATE TABLE IF NOT EXISTS {} (\n  {}\n)").format(
        _ident(table), sql.SQL(",\n  ").join(items))


def write_sql(table: Table, columns: Sequence[str], mode: str) -> sql.Composable:
    """INSERT statement for ``columns`` honouring ``mode``."""
    if mode not in WRITE_MODES:
        raise ValueError(f"unknown write mode {mode!r}; expected one of {WRITE_MODES}")
    query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        _ident(table),
        sql.SQL(", ").join(map(sql.Identifier, columns)),
        sql.SQL(", ").join(sql.Placeholder() * len(columns)),
    )
    pk = table.primary_key
    if mode == "ignore":
        query += sql.SQL(" ON CONFLICT DO NOTHING")
    elif mode == "upsert" and pk:
        updates = [c for c in columns if c not in pk]
        conflict = sql.SQL(", ").join(map(sql.Identifier, pk))
        if updates:
            query += sql.SQL(" ON CONFLICT ({}) DO UPDATE SET {}").format(
                conflict,
                sql.SQL(", ").join(
                    sql.SQL("{0} = EXCLUDED.{0}").format(sql.Identifier(c)) for c in updates),
            )
        else:
            query += sql.SQL(" ON CONFLICT ({}) DO NOTHING").format(conflict)
    return query


class Postgres:
    """Connection wrapper.

    >>> db = Postgres("postgresql://user:pass@localhost/db")
    >>> db.create_table(users)
    >>> db.write(users, [{"id": 1, "email": "a@b.c"}], mode="upsert")
    >>> db.fetch_all("SELECT * FROM users")
    """

    def __init__(self, dsn: Optional[str] = None, *, connection: Optional[psycopg.Connection] = None,
                 **connect_kwargs: Any) -> None:
        if connection is None and dsn is None and not connect_kwargs:
            raise ValueError("pass a DSN, connection keyword arguments, or an open connection")
        self._owns_connection = connection is None
        self.conn = connection or psycopg.connect(dsn or "", **connect_kwargs)

    # -- context management -------------------------------------------------
    def close(self) -> None:
        if self._owns_connection and not self.conn.closed:
            self.conn.close()

    def __enter__(self) -> "Postgres":
        return self

    def __exit__(self, exc_type: Any, *exc: Any) -> None:
        if exc_type is None:
            self.conn.commit()
        else:
            self.conn.rollback()
        self.close()

    def commit(self) -> None:
        self.conn.commit()

    def rollback(self) -> None:
        self.conn.rollback()

    def transaction(self):
        """``with db.transaction(): ...`` commits on success, rolls back on error."""
        return self.conn.transaction()

    # -- queries ------------------------------------------------------------
    def execute(self, query: Any, params: Any = None) -> int:
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            return cur.rowcount

    def fetch_all(self, query: Any, params: Any = None) -> List[Dict[str, Any]]:
        with self.conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, params)
            return cur.fetchall()

    def fetch_one(self, query: Any, params: Any = None) -> Optional[Dict[str, Any]]:
        with self.conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, params)
            return cur.fetchone()

    # -- schema management --------------------------------------------------
    def table_exists(self, table: Table) -> bool:
        row = self.fetch_one("SELECT to_regclass(%s) AS oid", (_regclass_name(table),))
        return bool(row and row["oid"])

    def existing_columns(self, table: Table) -> List[str]:
        rows = self.fetch_all(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = coalesce(%s, current_schema()) AND table_name = %s "
            "ORDER BY ordinal_position",
            (table.schema, table.name),
        )
        return [r["column_name"] for r in rows]

    def create_table(self, table: Table, add_missing_columns: bool = True) -> None:
        """Create the table (and schema) if needed; optionally add new columns
        to an existing table so definitions can evolve over time."""
        if table.schema:
            self.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(table.schema)))
        self.execute(create_table_sql(table))
        if add_missing_columns:
            existing = set(self.existing_columns(table))
            for col in table.columns:
                if col.name not in existing:
                    log.info("adding column %s.%s", table.qualified_name, col.name)
                    self.execute(sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {}").format(
                        _ident(table), column_ddl(col)))
        for index in table.indexes:
            cols = (index,) if isinstance(index, str) else tuple(index)
            name = f"ix_{table.name}_{'_'.join(cols)}"[:63]
            self.execute(sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({})").format(
                sql.Identifier(name), _ident(table), sql.SQL(", ").join(map(sql.Identifier, cols))))

    def drop_table(self, table: Table, cascade: bool = False) -> None:
        self.execute(sql.SQL("DROP TABLE IF EXISTS {}{}").format(
            _ident(table), sql.SQL(" CASCADE" if cascade else "")))

    def truncate(self, tables: Iterable[Table]) -> None:
        tables = list(tables)
        if tables:
            self.execute(sql.SQL("TRUNCATE {}").format(sql.SQL(", ").join(map(_ident, tables))))

    # -- writing ------------------------------------------------------------
    def write(self, table: Table, rows: Sequence[Dict[str, Any]], mode: str = "upsert") -> int:
        """Insert ``rows`` (dicts keyed by column name). Does not commit.

        Modes: ``insert`` (plain), ``upsert`` (update on primary-key conflict;
        plain insert if the table has no PK), ``ignore`` (skip conflicting rows),
        ``truncate`` (plain insert; truncation is done once by the sink).
        """
        if not rows:
            return 0
        written = 0
        # Group by column set so rows with different keys still work.
        groups: Dict[tuple, List[Dict[str, Any]]] = {}
        for row in rows:
            groups.setdefault(tuple(row.keys()), []).append(row)
        for columns, group in groups.items():
            json_cols = {c for c in columns if table.column(c).type.is_json}
            json_wrap = {c: (Jsonb if table.column(c).type.sql == "JSONB" else Json) for c in json_cols}
            params = [
                [json_wrap[c](row[c]) if c in json_cols and row[c] is not None else row[c] for c in columns]
                for row in group
            ]
            if mode == "upsert" and table.primary_key:
                params = _dedupe_last(params, [columns.index(c) for c in table.primary_key])
            with self.conn.cursor() as cur:
                cur.executemany(write_sql(table, columns, mode), params)
            written += len(params)
        return written


def _regclass_name(table: Table) -> str:
    quoted = '"' + table.name.replace('"', '""') + '"'
    if table.schema:
        return '"' + table.schema.replace('"', '""') + '".' + quoted
    return quoted


def _dedupe_last(params: List[List[Any]], key_idx: List[int]) -> List[List[Any]]:
    """Keep only the last row per primary key (last write wins)."""
    seen: Dict[tuple, int] = {}
    for i, row in enumerate(params):
        seen[tuple(row[k] for k in key_idx)] = i
    if len(seen) == len(params):
        return params
    keep = sorted(seen.values())
    return [params[i] for i in keep]
