from __future__ import annotations

from typing import Any, Callable, Dict, Mapping, Optional, Sequence

from ..postgres import WRITE_MODES, Postgres
from ..schema import Table
from .base import Sink, TableRows


class PostgresSink(Sink):
    """Write batches into PostgreSQL, one transaction per batch.

    ``mode`` is the default write mode for every table and ``table_modes``
    overrides it per table (keyed by table name or qualified name):

    * ``upsert``   insert or update on primary-key conflict (default)
    * ``insert``   plain insert; duplicates raise
    * ``ignore``   insert, silently skipping conflicting rows
    * ``truncate`` empty the table once at the start of the run, then insert
    """

    def __init__(
        self,
        dsn: Optional[str] = None,
        *,
        db: Optional[Postgres] = None,
        mode: str = "upsert",
        table_modes: Optional[Mapping[str, str]] = None,
        create_tables: bool = True,
        add_missing_columns: bool = True,
        **connect_kwargs: Any,
    ) -> None:
        for m in [mode, *(table_modes or {}).values()]:
            if m not in WRITE_MODES:
                raise ValueError(f"unknown write mode {m!r}; expected one of {WRITE_MODES}")
        self._owns_db = db is None
        self.db = db or Postgres(dsn, **connect_kwargs)
        self.mode = mode
        self.table_modes = dict(table_modes or {})
        self.create_tables = create_tables
        self.add_missing_columns = add_missing_columns

    def mode_for(self, table: Table) -> str:
        return self.table_modes.get(table.qualified_name, self.table_modes.get(table.name, self.mode))

    def prepare(self, tables: Sequence[Table]) -> None:
        def run() -> None:
            if self.create_tables:
                for t in tables:
                    self.db.create_table(t, add_missing_columns=self.add_missing_columns)
            self.db.truncate(t for t in tables if self.mode_for(t) == "truncate")

        self._atomically(run)

    def write(self, rows: TableRows) -> Dict[str, int]:
        counts: Dict[str, int] = {}

        def run() -> None:
            for table, table_rows in rows.items():
                counts[table.qualified_name] = self.db.write(table, table_rows, self.mode_for(table))

        self._atomically(run)
        return counts

    def _atomically(self, fn: Callable[[], None]) -> None:
        try:
            fn()
        except BaseException:
            self.db.rollback()
            raise
        self.db.commit()

    def close(self) -> None:
        if self._owns_db:
            self.db.close()
