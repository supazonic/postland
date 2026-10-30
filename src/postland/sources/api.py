"""HTTP/REST API source with pluggable pagination."""

from __future__ import annotations

import copy
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterator, List, Optional
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ..paths import MISSING, Path, get_path
from .base import Batch, Source

log = logging.getLogger(__name__)


@dataclass
class ApiRequest:
    """A mutable description of the next HTTP call; paginators edit it."""

    method: str
    url: str
    params: Dict[str, Any] = field(default_factory=dict)
    headers: Dict[str, str] = field(default_factory=dict)
    json: Any = None
    data: Any = None

    def copy(self) -> "ApiRequest":
        return copy.deepcopy(self)


@dataclass
class Page:
    """What a paginator sees after each call."""

    request: ApiRequest
    response: requests.Response
    payload: Any
    records: List[Any]
    number: int  # 1-based count of pages fetched so far


class Paginator(ABC):
    """Decides the next request (or ``None`` to stop) from the current page."""

    def first(self, request: ApiRequest) -> ApiRequest:
        return request

    @abstractmethod
    def next(self, page: Page) -> Optional[ApiRequest]:
        ...


class SinglePage(Paginator):
    """No pagination: one request, done."""

    def next(self, page: Page) -> Optional[ApiRequest]:
        return None


def _set_param(request: ApiRequest, name: str, value: Any, in_body: bool) -> None:
    if in_body:
        body = dict(request.json or {})
        body[name] = value
        request.json = body
    else:
        request.params[name] = value


class PageNumberPaginator(Paginator):
    """``?page=1&per_page=100``, ``?page=2...`` until a page comes back short/empty
    or ``total_pages_path`` says we are done."""

    def __init__(self, page_param: str = "page", start: int = 1,
                 page_size: Optional[int] = None, size_param: Optional[str] = "per_page",
                 total_pages_path: Optional[Path] = None, in_body: bool = False) -> None:
        self.page_param = page_param
        self.start = start
        self.page_size = page_size
        self.size_param = size_param
        self.total_pages_path = total_pages_path
        self.in_body = in_body

    def first(self, request: ApiRequest) -> ApiRequest:
        _set_param(request, self.page_param, self.start, self.in_body)
        if self.page_size and self.size_param:
            _set_param(request, self.size_param, self.page_size, self.in_body)
        return request

    def next(self, page: Page) -> Optional[ApiRequest]:
        if not page.records:
            return None
        if self.page_size and len(page.records) < self.page_size:
            return None
        current = self.start + page.number - 1
        if self.total_pages_path is not None:
            total = get_path(page.payload, self.total_pages_path)
            if total is not MISSING and total is not None and current >= int(total) + self.start - 1:
                return None
        request = page.request.copy()
        _set_param(request, self.page_param, current + 1, self.in_body)
        return request


class OffsetPaginator(Paginator):
    """``?offset=0&limit=100``, ``?offset=100...`` until short page or ``total_path``."""

    def __init__(self, limit: int = 100, offset_param: str = "offset",
                 limit_param: str = "limit", total_path: Optional[Path] = None,
                 start: int = 0, in_body: bool = False) -> None:
        self.limit = limit
        self.offset_param = offset_param
        self.limit_param = limit_param
        self.total_path = total_path
        self.start = start
        self.in_body = in_body
        self._offset = start

    def first(self, request: ApiRequest) -> ApiRequest:
        self._offset = self.start
        _set_param(request, self.offset_param, self._offset, self.in_body)
        _set_param(request, self.limit_param, self.limit, self.in_body)
        return request

    def next(self, page: Page) -> Optional[ApiRequest]:
        if len(page.records) < self.limit:
            return None
        self._offset += len(page.records)
        if self.total_path is not None:
            total = get_path(page.payload, self.total_path)
            if total is not MISSING and total is not None and self._offset >= int(total):
                return None
        request = page.request.copy()
        _set_param(request, self.offset_param, self._offset, self.in_body)
        return request


class CursorPaginator(Paginator):
    """Read a cursor/token from the response body and send it back as a param."""

    def __init__(self, cursor_path: Path = "next_cursor", cursor_param: str = "cursor",
                 in_body: bool = False, has_more_path: Optional[Path] = None) -> None:
        self.cursor_path = cursor_path
        self.cursor_param = cursor_param
        self.in_body = in_body
        self.has_more_path = has_more_path

    def next(self, page: Page) -> Optional[ApiRequest]:
        if self.has_more_path is not None and not get_path(page.payload, self.has_more_path, False):
            return None
        cursor = get_path(page.payload, self.cursor_path, None)
        if cursor in (None, "", MISSING):
            return None
        request = page.request.copy()
        _set_param(request, self.cursor_param, cursor, self.in_body)
        return request


class NextUrlPaginator(Paginator):
    """Follow a "next page" URL found in the body (``next_path``) or, when
    ``next_path`` is None, in the HTTP ``Link: <...>; rel="next"`` header."""

    def __init__(self, next_path: Optional[Path] = "next") -> None:
        self.next_path = next_path

    def next(self, page: Page) -> Optional[ApiRequest]:
        if self.next_path is None:
            url = page.response.links.get("next", {}).get("url")
        else:
            url = get_path(page.payload, self.next_path, None)
        if not url or url is MISSING:
            return None
        request = page.request.copy()
        request.url = urljoin(page.response.url or page.request.url, url)
        request.params = {}  # the next URL already carries its query string
        return request


class BearerAuth(requests.auth.AuthBase):
    """``Authorization: Bearer <token>``. ``token`` may be a callable for refreshable tokens."""

    def __init__(self, token: Any) -> None:
        self.token = token

    def __call__(self, r: requests.PreparedRequest) -> requests.PreparedRequest:
        token = self.token() if callable(self.token) else self.token
        r.headers["Authorization"] = f"Bearer {token}"
        return r


class ApiError(RuntimeError):
    pass


class ApiSource(Source):
    """Fetch records from an HTTP API, page by page.

    ``records_path`` points at the array of records inside each response body
    (``"data"``, ``"result.items"``...). If omitted, a list body is used as-is
    and an object body is treated as a single record.

    Each page becomes one batch, so each page is committed independently.
    """

    name = "api"

    def __init__(
        self,
        url: str,
        *,
        method: str = "GET",
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        json: Any = None,
        data: Any = None,
        auth: Any = None,
        records_path: Optional[Path] = None,
        paginator: Optional[Paginator] = None,
        session: Optional[requests.Session] = None,
        timeout: float = 30.0,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
        max_pages: Optional[int] = None,
        parse: Optional[Callable[[requests.Response], Any]] = None,
    ) -> None:
        self.request = ApiRequest(method=method.upper(), url=url, params=dict(params or {}),
                                  headers=dict(headers or {}), json=json, data=data)
        self.auth = auth
        self.records_path = records_path
        self.paginator = paginator or SinglePage()
        self.timeout = timeout
        self.max_pages = max_pages
        self.parse = parse or (lambda response: response.json())
        self._owns_session = session is None
        self.session = session or self._build_session(max_retries, backoff_factor)

    @staticmethod
    def _build_session(max_retries: int, backoff_factor: float) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=None,  # also retry POST: many read APIs (search, GraphQL) use it
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def extract_records(self, payload: Any) -> List[Any]:
        data = payload if self.records_path is None else get_path(payload, self.records_path)
        if data is MISSING or data is None:
            return []
        if isinstance(data, list):
            return data
        return [data]

    def fetch(self, request: ApiRequest) -> requests.Response:
        log.debug("%s %s params=%s", request.method, request.url, request.params)
        response = self.session.request(
            request.method, request.url, params=request.params or None,
            headers=request.headers or None, json=request.json, data=request.data,
            auth=self.auth, timeout=self.timeout,
        )
        if response.status_code >= 400:
            raise ApiError(f"{request.method} {response.url} -> HTTP {response.status_code}: "
                           f"{response.text[:500]}")
        return response

    def pages(self) -> Iterator[Page]:
        request: Optional[ApiRequest] = self.paginator.first(self.request.copy())
        number = 0
        while request is not None:
            response = self.fetch(request)
            payload = self.parse(response)
            number += 1
            page = Page(request=request, response=response, payload=payload,
                        records=self.extract_records(payload), number=number)
            yield page
            if self.max_pages is not None and number >= self.max_pages:
                return
            request = self.paginator.next(page)

    def batches(self) -> Iterator[Batch]:
        for page in self.pages():
            if page.records:
                yield page.records

    def close(self) -> None:
        if self._owns_session:
            self.session.close()
