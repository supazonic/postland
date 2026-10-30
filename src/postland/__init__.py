"""postland: land records from any source into typed PostgreSQL tables.

    Source  --records-->  TableMapping  --rows-->  Sink
   (ApiSource, ...)     (paths, types,          (PostgresSink)
                         nested arrays)
"""

from .mapping import Field, MappingError, Nested, TableMapping, const
from .paths import MISSING, get_path
from .pipeline import Pipeline, Rejected, RunResult
from .postgres import Postgres
from .schema import Column, Table, table
from .sinks import MemorySink, PostgresSink, Sink
from .sources import (
    ApiSource,
    BearerAuth,
    CursorPaginator,
    IterableSource,
    NextUrlPaginator,
    OffsetPaginator,
    PageNumberPaginator,
    Paginator,
    SinglePage,
    Source,
)
from .types import (
    BIGINT,
    BOOLEAN,
    BYTEA,
    DATE,
    DOUBLE,
    INTEGER,
    JSON,
    JSONB,
    NUMERIC,
    REAL,
    SMALLINT,
    TEXT,
    TIME,
    TIMESTAMP,
    TIMESTAMPTZ,
    UUID,
    CoercionError,
    DataType,
    array_of,
    custom,
    numeric,
    varchar,
)

__version__ = "0.1.0"

__all__ = [
    "Field", "MappingError", "Nested", "TableMapping", "const", "MISSING", "get_path",
    "Pipeline", "Rejected", "RunResult", "Postgres", "Column", "Table", "table",
    "MemorySink", "PostgresSink", "Sink", "ApiSource", "BearerAuth", "CursorPaginator",
    "IterableSource", "NextUrlPaginator", "OffsetPaginator", "PageNumberPaginator",
    "Paginator", "SinglePage", "Source", "BIGINT", "BOOLEAN", "BYTEA", "DATE", "DOUBLE",
    "INTEGER", "JSON", "JSONB", "NUMERIC", "REAL", "SMALLINT", "TEXT", "TIME", "TIMESTAMP",
    "TIMESTAMPTZ", "UUID", "CoercionError", "DataType", "array_of", "custom", "numeric",
    "varchar",
]
