"""Column data types and the coercion rules that turn raw source values
(usually JSON scalars) into Python values PostgreSQL can store.

Every type is a :class:`DataType` with a SQL name and a ``coerce`` function.
Coercion is source-agnostic: it does not care whether ``"2024-01-01"`` came
from an API, a CSV file or another database.
"""

from __future__ import annotations

import json
import uuid as _uuid
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Optional


class CoercionError(ValueError):
    """Raised when a value cannot be converted to a column's type."""


@dataclass(frozen=True)
class DataType:
    sql: str
    coerce_fn: Callable[[Any], Any]
    is_json: bool = False
    item: Optional["DataType"] = None  # set for array types
    empty_is_null: bool = True  # treat "" as NULL (off for text types)

    def coerce(self, value: Any) -> Any:
        if value is None:
            return None
        if self.empty_is_null and isinstance(value, str) and value.strip() == "":
            return None
        try:
            return self.coerce_fn(value)
        except CoercionError:
            raise
        except (TypeError, ValueError, InvalidOperation, OverflowError) as exc:
            raise CoercionError(f"cannot convert {value!r} to {self.sql}: {exc}") from exc

    def __str__(self) -> str:
        return self.sql


# --------------------------------------------------------------------------- #
# coercion helpers
# --------------------------------------------------------------------------- #

_TRUE = {"true", "t", "yes", "y", "1", "on"}
_FALSE = {"false", "f", "no", "n", "0", "off"}


def _to_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str)
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _to_int(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not value.is_integer():
            raise CoercionError(f"{value!r} is not a whole number")
        return int(value)
    if isinstance(value, Decimal):
        if value != value.to_integral_value():
            raise CoercionError(f"{value!r} is not a whole number")
        return int(value)
    text = str(value).strip()
    try:
        return int(text)
    except ValueError:
        return _to_int(Decimal(text))


def _bounded_int(bits: int) -> Callable[[Any], int]:
    low, high = -(2 ** (bits - 1)), 2 ** (bits - 1) - 1

    def coerce(value: Any) -> int:
        result = _to_int(value)
        if not low <= result <= high:
            raise CoercionError(f"{result} out of range for {bits}-bit integer")
        return result

    return coerce


def _to_float(value: Any) -> float:
    if isinstance(value, bool):
        raise CoercionError("refusing to convert a boolean to a float")
    return float(value)


def _to_decimal(value: Any) -> Decimal:
    if isinstance(value, bool):
        raise CoercionError("refusing to convert a boolean to a numeric")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value).strip())


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value in (0, 1):
        return bool(value)
    text = str(value).strip().lower()
    if text in _TRUE:
        return True
    if text in _FALSE:
        return False
    raise CoercionError(f"{value!r} is not a boolean")


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        seconds = float(value)
        if abs(seconds) > 1e11:  # looks like epoch milliseconds
            seconds /= 1000.0
        return datetime.fromtimestamp(seconds, tz=timezone.utc)
    text = str(value).strip()
    if text.isdigit():
        return _parse_datetime(int(text))
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    if " " in text and "T" not in text and len(text) > 10:
        text = text.replace(" ", "T", 1)
    return datetime.fromisoformat(text)


def _to_timestamptz(value: Any) -> datetime:
    result = _parse_datetime(value)
    if result.tzinfo is None:
        result = result.replace(tzinfo=timezone.utc)
    return result


def _to_timestamp(value: Any) -> datetime:
    result = _parse_datetime(value)
    if result.tzinfo is not None:
        result = result.astimezone(timezone.utc).replace(tzinfo=None)
    return result


def _to_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if len(text) == 10:
        return date.fromisoformat(text)
    return _parse_datetime(value).date()


def _to_time(value: Any) -> time:
    if isinstance(value, time):
        return value
    if isinstance(value, datetime):
        return value.time()
    return time.fromisoformat(str(value).strip())


def _to_json(value: Any) -> Any:
    # Strings holding serialised JSON are decoded so they land as structured
    # JSON rather than as a JSON string literal.
    if isinstance(value, str):
        stripped = value.strip()
        if stripped[:1] in ("{", "[") or stripped in ("null", "true", "false"):
            try:
                return json.loads(stripped)
            except ValueError:
                pass
    return value


def _to_uuid(value: Any) -> _uuid.UUID:
    if isinstance(value, _uuid.UUID):
        return value
    return _uuid.UUID(str(value).strip())


def _to_bytes(value: Any) -> bytes:
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    return str(value).encode("utf-8")


# --------------------------------------------------------------------------- #
# public types
# --------------------------------------------------------------------------- #

TEXT = DataType("TEXT", _to_text, empty_is_null=False)
SMALLINT = DataType("SMALLINT", _bounded_int(16))
INTEGER = DataType("INTEGER", _bounded_int(32))
BIGINT = DataType("BIGINT", _bounded_int(64))
REAL = DataType("REAL", _to_float)
DOUBLE = DataType("DOUBLE PRECISION", _to_float)
NUMERIC = DataType("NUMERIC", _to_decimal)
BOOLEAN = DataType("BOOLEAN", _to_bool)
DATE = DataType("DATE", _to_date)
TIME = DataType("TIME", _to_time)
TIMESTAMP = DataType("TIMESTAMP", _to_timestamp)
TIMESTAMPTZ = DataType("TIMESTAMPTZ", _to_timestamptz)
JSON = DataType("JSON", _to_json, is_json=True)
JSONB = DataType("JSONB", _to_json, is_json=True)
UUID = DataType("UUID", _to_uuid)
BYTEA = DataType("BYTEA", _to_bytes)


def varchar(length: int) -> DataType:
    def coerce(value: Any) -> str:
        text = _to_text(value)
        if len(text) > length:
            raise CoercionError(f"value longer than {length} characters")
        return text

    return DataType(f"VARCHAR({int(length)})", coerce, empty_is_null=False)


def numeric(precision: int, scale: int = 0) -> DataType:
    quantum = Decimal(1).scaleb(-int(scale))

    def coerce(value: Any) -> Decimal:
        return _to_decimal(value).quantize(quantum)

    return DataType(f"NUMERIC({int(precision)},{int(scale)})", coerce)


def array_of(item: DataType) -> DataType:
    """A PostgreSQL array column, e.g. ``array_of(TEXT)`` -> ``TEXT[]``."""
    if item.is_json:
        raise ValueError("use a JSON/JSONB column instead of an array of JSON")

    def coerce(value: Any) -> list:
        if isinstance(value, str):
            value = _to_json(value)
        if not isinstance(value, (list, tuple, set)):
            value = [value]
        return [item.coerce(v) for v in value]

    return DataType(f"{item.sql}[]", coerce, item=item)


def custom(sql: str, coerce: Callable[[Any], Any] = lambda v: v) -> DataType:
    """Escape hatch for any other PostgreSQL type (``INET``, ``CITEXT``...)."""
    return DataType(sql, coerce)
