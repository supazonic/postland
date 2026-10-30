import pytest

from postland import (
    ApiSource, CursorPaginator, NextUrlPaginator, OffsetPaginator, PageNumberPaginator,
)
from postland.sources.api import ApiError

from conftest import FakeResponse, FakeSession

DATA = [{"id": i} for i in range(1, 8)]


def ids(source):
    return [r["id"] for r in source.records()]


def test_single_page_records_path():
    s = FakeSession(lambda *a: {"data": {"items": DATA[:2]}})
    src = ApiSource("https://api.test/x", records_path="data.items", session=s)
    assert ids(src) == [1, 2]


def test_object_body_is_one_record():
    src = ApiSource("https://api.test/x", session=FakeSession(lambda *a: {"id": 9}))
    assert ids(src) == [9]


def test_page_number():
    def handler(method, url, params, body):
        page, size = params["page"], params["per_page"]
        return {"results": DATA[(page - 1) * size: page * size]}

    s = FakeSession(handler)
    src = ApiSource("https://api.test/x", records_path="results", session=s,
                    paginator=PageNumberPaginator(page_size=3))
    assert ids(src) == list(range(1, 8))
    assert [c["params"]["page"] for c in s.calls] == [1, 2, 3]


def test_page_number_total_pages():
    def handler(method, url, params, body):
        return {"results": DATA[params["page"] - 1: params["page"]], "meta": {"pages": 2}}

    s = FakeSession(handler)
    src = ApiSource("https://api.test/x", records_path="results", session=s,
                    paginator=PageNumberPaginator(size_param=None, total_pages_path="meta.pages"))
    assert ids(src) == [1, 2]


def test_offset_in_body():
    def handler(method, url, params, body):
        return {"rows": DATA[body["offset"]: body["offset"] + body["limit"]], "total": len(DATA)}

    s = FakeSession(handler)
    src = ApiSource("https://api.test/search", method="POST", json={"q": "x"}, records_path="rows",
                    session=s, paginator=OffsetPaginator(limit=4, total_path="total", in_body=True))
    assert ids(src) == list(range(1, 8))
    assert s.calls[0]["json"] == {"q": "x", "offset": 0, "limit": 4}


def test_cursor():
    def handler(method, url, params, body):
        start = int(params.get("cursor", 0))
        nxt = start + 3 if start + 3 < len(DATA) else None
        return {"data": DATA[start:start + 3], "next_cursor": nxt and str(nxt)}

    src = ApiSource("https://api.test/x", records_path="data", session=FakeSession(handler),
                    paginator=CursorPaginator())
    assert ids(src) == list(range(1, 8))


def test_next_url_and_link_header():
    def body_handler(method, url, params, body):
        n = int(url.rsplit("=", 1)[1]) if "=" in url else 0
        return {"items": DATA[n:n + 4], "next": f"/x?from={n + 4}" if n + 4 < len(DATA) else None}

    src = ApiSource("https://api.test/x", records_path="items", session=FakeSession(body_handler),
                    paginator=NextUrlPaginator("next"))
    assert ids(src) == list(range(1, 8))

    def header_handler(method, url, params, body):
        n = int(url.rsplit("=", 1)[1]) if "=" in url else 0
        links = {"next": {"url": f"https://api.test/x?from={n + 4}"}} if n + 4 < len(DATA) else {}
        return FakeResponse(DATA[n:n + 4], url, links=links)

    src = ApiSource("https://api.test/x", session=FakeSession(header_handler),
                    paginator=NextUrlPaginator(None))
    assert ids(src) == list(range(1, 8))


def test_max_pages_and_errors():
    s = FakeSession(lambda *a: {"next_cursor": "again", "data": [{"id": 1}]})
    src = ApiSource("https://api.test/x", records_path="data", session=s,
                    paginator=CursorPaginator(), max_pages=3)
    assert ids(src) == [1, 1, 1]

    src = ApiSource("https://api.test/x", session=FakeSession(
        lambda m, url, *a: FakeResponse({"err": 1}, url, status_code=404)))
    with pytest.raises(ApiError):
        list(src.records())
