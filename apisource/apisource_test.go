package apisource

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

var data = func() []map[string]any {
	out := make([]map[string]any, 7)
	for i := range out {
		out[i] = map[string]any{"id": i + 1}
	}
	return out
}()

func slice(from, to int) []map[string]any {
	from, to = min(from, len(data)), min(to, len(data))
	return data[from:to]
}

func serve(t *testing.T, h func(w http.ResponseWriter, r *http.Request) any) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if v := h(w, r); v != nil {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(v)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func ids(t *testing.T, s *Source) []int {
	t.Helper()
	var out []int
	err := s.Batches(context.Background(), func(b []any) error {
		for _, r := range b {
			n, _ := r.(map[string]any)["id"].(json.Number).Int64()
			out = append(out, int(n))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func seq(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i + 1
	}
	return out
}

func eq(t *testing.T, got, want []int) {
	t.Helper()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("got %v want %v", got, want)
	}
}

func qi(r *http.Request, name string) int {
	n, _ := strconv.Atoi(r.URL.Query().Get(name))
	return n
}

func TestSinglePageAndRecordsPath(t *testing.T) {
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) any {
		return map[string]any{"data": map[string]any{"items": slice(0, 2)}}
	})
	eq(t, ids(t, &Source{URL: srv.URL, RecordsPath: "data.items"}), []int{1, 2})

	obj := serve(t, func(w http.ResponseWriter, r *http.Request) any { return map[string]any{"id": 9} })
	eq(t, ids(t, New(obj.URL)), []int{9})
}

func TestPageNumber(t *testing.T) {
	var calls []int
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) any {
		p, n := qi(r, "page"), qi(r, "per_page")
		calls = append(calls, p)
		return map[string]any{"results": slice((p-1)*n, p*n)}
	})
	src := &Source{URL: srv.URL, RecordsPath: "results", Paginator: &PageNumber{PageSize: 3}}
	eq(t, ids(t, src), seq(7))
	eq(t, calls, []int{1, 2, 3})

	total := serve(t, func(w http.ResponseWriter, r *http.Request) any {
		p := qi(r, "p")
		return map[string]any{"results": slice(p, p+1), "meta": map[string]any{"pages": 2}}
	})
	src = &Source{URL: total.URL, RecordsPath: "results",
		Paginator: &PageNumber{Param: "p", StartAtZero: true, TotalPagesPath: "meta.pages"}}
	eq(t, ids(t, src), []int{1, 2})
}

func TestOffsetInBody(t *testing.T) {
	var first map[string]any
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) any {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if first == nil {
			first = body
		}
		off, lim := int(body["offset"].(float64)), int(body["limit"].(float64))
		return map[string]any{"rows": slice(off, off+lim), "total": len(data)}
	})
	src := &Source{URL: srv.URL, Method: http.MethodPost, Body: map[string]any{"q": "x"},
		RecordsPath: "rows", Paginator: &Offset{Limit: 4, TotalPath: "total", InBody: true}}
	eq(t, ids(t, src), seq(7))
	if fmt.Sprint(first) != "map[limit:4 offset:0 q:x]" {
		t.Errorf("first body = %v", first)
	}
}

func TestCursor(t *testing.T) {
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) any {
		start := qi(r, "cursor")
		var next any
		if start+3 < len(data) {
			next = strconv.Itoa(start + 3)
		}
		return map[string]any{"data": slice(start, start+3), "next_cursor": next}
	})
	eq(t, ids(t, &Source{URL: srv.URL, RecordsPath: "data", Paginator: &Cursor{}}), seq(7))
}

func TestNextURLBodyAndLinkHeader(t *testing.T) {
	body := serve(t, func(w http.ResponseWriter, r *http.Request) any {
		n := qi(r, "from")
		var next any
		if n+4 < len(data) {
			next = fmt.Sprintf("/x?from=%d", n+4)
		}
		return map[string]any{"items": slice(n, n+4), "links": map[string]any{"next": next}}
	})
	eq(t, ids(t, &Source{URL: body.URL + "/x", RecordsPath: "items", Paginator: &NextURL{Path: "links.next"}}), seq(7))

	var srvURL string
	header := serve(t, func(w http.ResponseWriter, r *http.Request) any {
		n := qi(r, "from")
		if n+4 < len(data) {
			w.Header().Set("Link", fmt.Sprintf(`<%s/x?from=%d>; rel="next", <%s/x>; rel="first"`, srvURL, n+4, srvURL))
		}
		return slice(n, n+4)
	})
	srvURL = header.URL
	eq(t, ids(t, &Source{URL: header.URL + "/x", Paginator: &NextURL{}}), seq(7))
}

func TestRetriesAuthAndErrors(t *testing.T) {
	var attempts atomic.Int32
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) any {
		if r.Header.Get("Authorization") != "Bearer tok" {
			w.WriteHeader(http.StatusUnauthorized)
			return nil
		}
		if attempts.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return nil
		}
		return []any{map[string]any{"id": 1}}
	})
	src := &Source{URL: srv.URL, Auth: Bearer("tok"), Backoff: time.Millisecond}
	eq(t, ids(t, src), []int{1})
	if attempts.Load() != 3 {
		t.Errorf("attempts = %d", attempts.Load())
	}

	err := (&Source{URL: srv.URL, MaxRetries: -1}).Batches(context.Background(), func([]any) error { return nil })
	var apiErr *Error
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusUnauthorized {
		t.Errorf("err = %v", err)
	}
}

func TestMaxPagesAndStop(t *testing.T) {
	srv := serve(t, func(w http.ResponseWriter, r *http.Request) any {
		_, _ = io.Copy(io.Discard, r.Body)
		return map[string]any{"data": []any{map[string]any{"id": 1}}, "next_cursor": "again"}
	})
	src := &Source{URL: srv.URL, RecordsPath: "data", Paginator: &Cursor{}, MaxPages: 3}
	eq(t, ids(t, src), []int{1, 1, 1})

	stop := errors.New("stop")
	err := (&Source{URL: srv.URL, RecordsPath: "data", Paginator: &Cursor{}}).
		Batches(context.Background(), func([]any) error { return stop })
	if !errors.Is(err, stop) {
		t.Errorf("err = %v", err)
	}
}
