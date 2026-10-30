"""The sink contract: where mapped rows end up."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Sequence

from ..schema import Table

TableRows = Dict[Table, List[Dict[str, Any]]]


class Sink(ABC):
    def prepare(self, tables: Sequence[Table]) -> None:
        """Called once before the first batch (create tables, truncate...)."""

    @abstractmethod
    def write(self, rows: TableRows) -> Dict[str, int]:
        """Write one batch atomically. Returns rows written per table name."""

    def close(self) -> None:
        pass


class MemorySink(Sink):
    """Collects rows in memory. Useful for tests and dry runs."""

    def __init__(self) -> None:
        self.tables: Dict[str, List[Dict[str, Any]]] = {}

    def prepare(self, tables: Sequence[Table]) -> None:
        for t in tables:
            self.tables.setdefault(t.qualified_name, [])

    def write(self, rows: TableRows) -> Dict[str, int]:
        counts = {}
        for table, table_rows in rows.items():
            self.tables.setdefault(table.qualified_name, []).extend(table_rows)
            counts[table.qualified_name] = len(table_rows)
        return counts
