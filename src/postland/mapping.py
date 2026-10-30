"""Turn source records into table rows.

A :class:`TableMapping` says how one record becomes one row of a
:class:`~postland.schema.Table`. Nested arrays inside a record (e.g. an order's
``line_items``) are exploded into child tables with :class:`Nested`, which can
copy keys from the parent record so child rows can be joined back.

Mappings only deal with plain Python dicts, so the same mapping works whether
the records came from an API, a database query, or a parsed file.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Union

from .paths import MISSING, Path, get_path
from .schema import Table
from .types import CoercionError


class MappingError(ValueError):
    """A record could not be mapped to a row."""

    def __init__(self, table: str, column: Optional[str], message: str) -> None:
        self.table = table
        self.column = column
        where = f"{table}.{column}" if column else table
        super().__init__(f"{where}: {message}")


@dataclass
class Field:
    """Where a column's value comes from.

    ``path``      path into the record (see :mod:`postland.paths`) or a callable.
    ``transform`` optional function applied to the extracted value before type coercion.
    ``default``   value used when the path is missing or resolves to ``None``.
    """

    path: Path = None
    transform: Optional[Callable[[Any], Any]] = None
    default: Any = None

    def extract(self, record: Any) -> Any:
        value = get_path(record, self.path)
        if value is MISSING or value is None:
            value = self.default
        if self.transform is not None:
            value = self.transform(value)
        return value


def const(value: Any) -> Field:
    """A field that always yields ``value`` (e.g. a source system tag)."""
    return Field(path=lambda _record: value)


FieldSpec = Union[str, Callable[[Any], Any], Field]


@dataclass
class Nested:
    """Explode an array inside each record into rows of a child table.

    ``path``         path to the array within the parent record.
    ``mapping``      mapping applied to each array element.
    ``parent_keys``  child column -> path in the *parent* record whose value is
                     copied onto every child row (foreign key).
    ``index_column`` optional child column that receives the element position.
    """

    path: Path
    mapping: "TableMapping"
    parent_keys: Mapping[str, Path] = field(default_factory=dict)
    index_column: Optional[str] = None


Rows = Dict[Table, List[Dict[str, Any]]]


class TableMapping:
    """Map records onto ``table``.

    ``fields`` maps column name -> path string, callable, or :class:`Field`.
    Columns not listed are read from the record key of the same name, except
    columns that have a SQL ``default`` (those are left for the database to fill).

    ``children`` is a list of :class:`Nested` definitions for arrays that should
    land in their own tables. ``where`` can drop records (return False to skip).
    """

    def __init__(
        self,
        table: Table,
        fields: Optional[Mapping[str, FieldSpec]] = None,
        children: Sequence[Nested] = (),
        where: Optional[Callable[[Any], bool]] = None,
    ) -> None:
        self.table = table
        self.children = list(children)
        self.where = where
        fields = dict(fields or {})
        unknown = set(fields) - set(table.column_names)
        if unknown:
            raise ValueError(f"fields {sorted(unknown)} are not columns of {table.qualified_name}")
        self.fields: Dict[str, Field] = {}
        for col in table.columns:
            if col.name in fields:
                self.fields[col.name] = _as_field(fields[col.name])
            elif col.default is None:
                self.fields[col.name] = Field(path=col.name)
        for child in self.children:
            for name in list(child.parent_keys) + ([child.index_column] if child.index_column else []):
                child.mapping.table.column(name)  # validates the column exists
                child.mapping.fields.pop(name, None)  # filled from the parent instead

    @property
    def tables(self) -> List[Table]:
        """This table followed by all descendant tables (parents before children)."""
        result = [self.table]
        for child in self.children:
            for t in child.mapping.tables:
                if t not in result:
                    result.append(t)
        return result

    def map(self, record: Any, extra: Optional[Dict[str, Any]] = None) -> Rows:
        """Map one record. Returns rows grouped by table, parents first."""
        out: Rows = {t: [] for t in self.tables}
        self._map_into(record, extra or {}, out)
        return out

    def map_many(self, records: Sequence[Any]) -> Rows:
        out: Rows = {t: [] for t in self.tables}
        for record in records:
            self._map_into(record, {}, out)
        return out

    def _map_into(self, record: Any, extra: Dict[str, Any], out: Rows) -> None:
        if self.where is not None and not self.where(record):
            return
        row: Dict[str, Any] = {}
        name = self.table.qualified_name
        for col_name, fld in self.fields.items():
            try:
                raw = fld.extract(record)
            except Exception as exc:  # user transforms may raise anything
                raise MappingError(name, col_name, f"extraction failed: {exc}") from exc
            row[col_name] = raw
        row.update(extra)
        for col_name, raw in row.items():
            col = self.table.column(col_name)
            try:
                value = col.type.coerce(raw)
            except CoercionError as exc:
                raise MappingError(name, col_name, str(exc)) from exc
            if value is None and not col.nullable and col.default is None:
                raise MappingError(name, col_name, "value is required but missing")
            row[col_name] = value
        out[self.table].append(row)

        for child in self.children:
            items = get_path(record, child.path)
            if items is MISSING or items is None:
                continue
            if not isinstance(items, (list, tuple)):
                items = [items]
            keys = {col: get_path(record, path, None) for col, path in child.parent_keys.items()}
            for position, item in enumerate(items):
                child_extra = dict(keys)
                if child.index_column:
                    child_extra[child.index_column] = position
                child.mapping._map_into(item, child_extra, out)


def _as_field(spec: FieldSpec) -> Field:
    if isinstance(spec, Field):
        return spec
    return Field(path=spec)
