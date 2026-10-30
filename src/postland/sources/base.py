"""The source contract.

A source is anything that can yield batches of records, where a record is a
plain ``dict`` (or any object the mapping paths can navigate). That is the only
thing the rest of postland relies on, so adding a new kind of source (another
database, files on SFTP, a message queue...) means implementing ``batches()``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from itertools import islice
from typing import Any, Callable, Dict, Iterable, Iterator, List, Union

Record = Dict[str, Any]
Batch = List[Record]


class Source(ABC):
    """Base class for all record sources."""

    name: str = "source"

    @abstractmethod
    def batches(self) -> Iterator[Batch]:
        """Yield lists of records. Each batch is written in one transaction."""

    def records(self) -> Iterator[Record]:
        for batch in self.batches():
            yield from batch

    def close(self) -> None:
        """Release connections/handles. Called by the pipeline when done."""

    def __enter__(self) -> "Source":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


class IterableSource(Source):
    """Wrap any iterable (or a zero-arg callable returning one) as a source.

    Handy for tests, generators, or quickly plugging in a loader you already have.
    """

    name = "iterable"

    def __init__(self, records: Union[Iterable[Record], Callable[[], Iterable[Record]]],
                 batch_size: int = 1000) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        self._records = records
        self.batch_size = batch_size

    def batches(self) -> Iterator[Batch]:
        records = self._records() if callable(self._records) else self._records
        iterator = iter(records)
        while True:
            batch = list(islice(iterator, self.batch_size))
            if not batch:
                return
            yield batch
