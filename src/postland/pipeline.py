"""Glue: source -> mappings -> sink."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Union

from .mapping import MappingError, TableMapping
from .schema import Table
from .sinks.base import Sink, TableRows
from .sources.base import Source

log = logging.getLogger(__name__)


@dataclass
class Rejected:
    record: Any
    error: str


@dataclass
class RunResult:
    batches: int = 0
    records: int = 0
    rows: Dict[str, int] = field(default_factory=dict)
    rejected: List[Rejected] = field(default_factory=list)
    rejected_count: int = 0
    seconds: float = 0.0

    def __str__(self) -> str:
        rows = ", ".join(f"{k}={v}" for k, v in self.rows.items()) or "none"
        return (f"{self.records} records in {self.batches} batches -> rows: {rows}; "
                f"rejected: {self.rejected_count}; {self.seconds:.2f}s")


class Pipeline:
    """Move records from a :class:`Source` to a :class:`Sink` through one or more
    :class:`TableMapping`s.

    ``on_error``: ``"raise"`` aborts on the first bad record (the current batch is
    rolled back, earlier batches stay committed); ``"skip"`` drops the record (and
    all its child rows) and records it in ``RunResult.rejected``.
    """

    def __init__(self, source: Source, sink: Sink,
                 mappings: Union[TableMapping, Sequence[TableMapping]],
                 on_error: str = "raise", keep_rejected: int = 100) -> None:
        if on_error not in ("raise", "skip"):
            raise ValueError("on_error must be 'raise' or 'skip'")
        self.source = source
        self.sink = sink
        self.mappings = [mappings] if isinstance(mappings, TableMapping) else list(mappings)
        if not self.mappings:
            raise ValueError("at least one mapping is required")
        self.on_error = on_error
        self.keep_rejected = keep_rejected

    @property
    def tables(self) -> List[Table]:
        result: List[Table] = []
        for m in self.mappings:
            for t in m.tables:
                if t not in result:
                    result.append(t)
        return result

    def transform(self, batch: Sequence[Any], result: RunResult) -> TableRows:
        rows: TableRows = {t: [] for t in self.tables}
        for record in batch:
            try:
                # map the whole record first so a failure never leaves partial rows
                mapped = [m.map(record) for m in self.mappings]
            except MappingError as exc:
                if self.on_error == "raise":
                    raise
                result.rejected_count += 1
                if len(result.rejected) < self.keep_rejected:
                    result.rejected.append(Rejected(record, str(exc)))
                log.warning("rejected record: %s", exc)
                continue
            for table_rows in mapped:
                for table, r in table_rows.items():
                    rows[table].extend(r)
        return rows

    def run(self, max_batches: Optional[int] = None, close: bool = True) -> RunResult:
        result = RunResult()
        started = time.monotonic()
        try:
            self.sink.prepare(self.tables)
            for batch in self.source.batches():
                rows = self.transform(batch, result)
                for name, count in self.sink.write(rows).items():
                    result.rows[name] = result.rows.get(name, 0) + count
                result.batches += 1
                result.records += len(batch)
                log.info("batch %d: %d records", result.batches, len(batch))
                if max_batches is not None and result.batches >= max_batches:
                    break
        finally:
            result.seconds = time.monotonic() - started
            if close:
                self.source.close()
                self.sink.close()
        return result
