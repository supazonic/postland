"""Table and column definitions.

These describe *what* is stored and are independent of where the data comes
from and of how it is written. The PostgreSQL sink turns them into DDL.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, List, Optional, Sequence, Tuple, Union

from .types import DataType


@dataclass
class Column:
    name: str
    type: DataType
    nullable: bool = True
    primary_key: bool = False
    unique: bool = False
    default: Optional[str] = None  # raw SQL expression, e.g. "now()"
    references: Optional[str] = None  # e.g. "public.orders(id)"

    def __post_init__(self) -> None:
        if self.primary_key:
            self.nullable = False


@dataclass(eq=False)  # identity hash so tables can key dicts
class Table:
    name: str
    columns: List[Column]
    schema: Optional[str] = None
    indexes: Sequence[Union[str, Tuple[str, ...]]] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        seen = set()
        for col in self.columns:
            if col.name in seen:
                raise ValueError(f"duplicate column {col.name!r} in table {self.name!r}")
            seen.add(col.name)

    @property
    def qualified_name(self) -> str:
        return f"{self.schema}.{self.name}" if self.schema else self.name

    @property
    def primary_key(self) -> List[str]:
        return [c.name for c in self.columns if c.primary_key]

    @property
    def column_names(self) -> List[str]:
        return [c.name for c in self.columns]

    def column(self, name: str) -> Column:
        for col in self.columns:
            if col.name == name:
                return col
        raise KeyError(f"table {self.qualified_name!r} has no column {name!r}")

    def __repr__(self) -> str:
        return f"Table({self.qualified_name!r}, columns={self.column_names})"


def table(name: str, *columns: Column, schema: Optional[str] = None,
          indexes: Iterable[Union[str, Tuple[str, ...]]] = ()) -> Table:
    """Shorthand: ``table("users", Column("id", BIGINT, primary_key=True), ...)``."""
    return Table(name=name, columns=list(columns), schema=schema, indexes=tuple(indexes))
