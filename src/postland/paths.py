"""Tiny path language for pulling values out of nested records.

Supported syntax::

    "id"                  -> record["id"]
    "customer.email"      -> record["customer"]["email"]
    "items[0].sku"        -> record["items"][0]["sku"]
    "items.0.sku"         -> same as above
    "$" or ""             -> the record itself (useful for arrays of scalars)

A path may also be any callable ``f(record) -> value`` for cases the syntax
cannot express.
"""

from __future__ import annotations

import re
from typing import Any, Callable, List, Union

Path = Union[str, Callable[[Any], Any], None]


class _Missing:
    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return "MISSING"

    def __bool__(self) -> bool:
        return False


MISSING: Any = _Missing()

_TOKEN = re.compile(r"[^.\[\]]+|\[(\d+)\]")


def parse_path(path: str) -> List[Union[str, int]]:
    """Split ``"a.b[0].c"`` into ``["a", "b", 0, "c"]``."""
    if path in ("", "$"):
        return []
    if path.startswith("$."):
        path = path[2:]
    tokens: List[Union[str, int]] = []
    for match in _TOKEN.finditer(path):
        if match.group(1) is not None:
            tokens.append(int(match.group(1)))
        else:
            tokens.append(match.group(0))
    return tokens


def get_path(obj: Any, path: Path, default: Any = MISSING) -> Any:
    """Resolve ``path`` against ``obj``; return ``default`` if any step is missing."""
    if path is None:
        return obj
    if callable(path):
        return path(obj)
    current = obj
    for token in parse_path(path):
        if current is None:
            return default
        if isinstance(current, dict):
            if token in current:
                current = current[token]
            elif isinstance(token, int) and str(token) in current:
                current = current[str(token)]
            else:
                return default
        elif isinstance(current, (list, tuple)):
            try:
                current = current[int(token)]
            except (ValueError, IndexError):
                return default
        else:
            current = getattr(current, str(token), default)
            if current is default:
                return default
    return current
