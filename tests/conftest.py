import os
import sys
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from postland import Postgres  # noqa: E402


class FakeResponse:
    def __init__(self, payload, url, status_code=200, links=None):
        self._payload = payload
        self.url = url
        self.status_code = status_code
        self.links = links or {}
        self.text = str(payload)

    def json(self):
        return self._payload


class FakeSession:
    """Stands in for requests.Session: ``handler(method, url, params, json)``
    returns either a payload or a FakeResponse."""

    def __init__(self, handler):
        self.handler = handler
        self.calls = []

    def request(self, method, url, params=None, headers=None, json=None, data=None, auth=None, timeout=None):
        self.calls.append({"method": method, "url": url, "params": dict(params or {}), "json": json})
        result = self.handler(method, url, dict(params or {}), json)
        return result if isinstance(result, FakeResponse) else FakeResponse(result, url)

    def close(self):
        pass


@pytest.fixture
def fake_session():
    return FakeSession


@pytest.fixture
def pg():
    """A Postgres wrapper inside a throwaway schema. Set POSTLAND_TEST_DSN to run."""
    dsn = os.environ.get("POSTLAND_TEST_DSN")
    if not dsn:
        pytest.skip("POSTLAND_TEST_DSN not set")
    db = Postgres(dsn)
    schema = f"t_{uuid.uuid4().hex[:10]}"
    db.execute(f'CREATE SCHEMA "{schema}"')
    db.commit()
    db.test_schema = schema
    try:
        yield db
    finally:
        db.rollback()
        db.execute(f'DROP SCHEMA "{schema}" CASCADE')
        db.commit()
        db.close()
