// Package apisource is a postland.Source that reads records from HTTP/JSON
// APIs, with pluggable pagination, retries and authentication.
package apisource

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/supazonic/postland"
)

// Request describes the next HTTP call. Paginators receive a copy and edit it.
type Request struct {
	Method string
	URL    string
	Query  url.Values
	Header http.Header
	// Body is sent as JSON when non-nil. Paginators with InBody set their
	// parameters on it, so it must be a map[string]any in that case.
	Body any
}

// Clone returns a deep-enough copy for a paginator to modify.
func (r *Request) Clone() *Request {
	c := *r
	c.Query = url.Values{}
	for k, v := range r.Query {
		c.Query[k] = append([]string(nil), v...)
	}
	c.Header = r.Header.Clone()
	if m, ok := r.Body.(map[string]any); ok {
		c.Body = maps.Clone(m)
	}
	return &c
}

// Page is what a paginator sees after each call.
type Page struct {
	Request  *Request
	Response *http.Response // body already consumed
	Payload  any            // decoded body
	Records  []any
	Number   int // 1-based count of pages fetched so far
}

// Paginator decides the first request and, from each page, the next one.
// Next returns nil when there are no more pages.
type Paginator interface {
	First(req *Request) error
	Next(page *Page) (*Request, error)
}

// Error is returned for HTTP responses with status >= 400 (after retries).
type Error struct {
	Method     string
	URL        string
	StatusCode int
	Body       string
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s %s -> HTTP %d: %s", e.Method, e.URL, e.StatusCode, e.Body)
}

// Source fetches records from an HTTP API page by page. Each page becomes one
// batch, so each page is committed independently.
type Source struct {
	URL    string
	Method string // default GET
	Query  url.Values
	Header http.Header
	Body   any // JSON request body

	// Auth is applied to every outgoing request, e.g. Bearer("token").
	Auth func(*http.Request) error

	// RecordsPath points at the array of records in each response body
	// ("data", "result.items"...). Empty: a list body is used as-is and an
	// object body is a single record.
	RecordsPath string

	Paginator Paginator    // default SinglePage
	Client    *http.Client // default: client with 30s timeout

	MaxRetries int           // retries for 429/5xx/network errors (default 3; -1 disables)
	Backoff    time.Duration // first retry delay, doubled each time (default 500ms)
	MaxPages   int           // safety limit (0 = unlimited)

	// Decode parses the response body. Default: JSON with numbers kept
	// exact (json.Number), so large IDs are not rounded through float64.
	Decode func(*http.Response, []byte) (any, error)
}

// New is shorthand for &Source{URL: url}.
func New(url string) *Source { return &Source{URL: url} }

var _ postland.Source = (*Source)(nil)

// Batches implements postland.Source.
func (s *Source) Batches(ctx context.Context, fn func(postland.Batch) error) error {
	return s.Pages(ctx, func(p *Page) error {
		if len(p.Records) == 0 {
			return nil
		}
		return fn(p.Records)
	})
}

// Pages calls fn for every fetched page, including empty ones.
func (s *Source) Pages(ctx context.Context, fn func(*Page) error) error {
	pag := s.Paginator
	if pag == nil {
		pag = SinglePage{}
	}
	method := s.Method
	if method == "" {
		method = http.MethodGet
	}
	req := (&Request{Method: method, URL: s.URL, Query: s.Query, Header: s.Header, Body: s.Body}).Clone()
	if err := pag.First(req); err != nil {
		return err
	}
	for number := 1; req != nil; number++ {
		resp, body, err := s.Do(ctx, req)
		if err != nil {
			return err
		}
		payload, err := s.decode(resp, body)
		if err != nil {
			return fmt.Errorf("decode %s: %w", resp.Request.URL, err)
		}
		records, err := s.ExtractRecords(payload)
		if err != nil {
			return err
		}
		page := &Page{Request: req, Response: resp, Payload: payload, Records: records, Number: number}
		if err := fn(page); err != nil {
			return err
		}
		if s.MaxPages > 0 && number >= s.MaxPages {
			return nil
		}
		if req, err = pag.Next(page); err != nil {
			return err
		}
	}
	return nil
}

// ExtractRecords pulls the record list out of a decoded payload.
func (s *Source) ExtractRecords(payload any) ([]any, error) {
	data := payload
	if s.RecordsPath != "" {
		v, ok := postland.Lookup(payload, s.RecordsPath)
		if !ok {
			return nil, nil
		}
		data = v
	}
	switch d := data.(type) {
	case nil:
		return nil, nil
	case []any:
		return d, nil
	}
	return []any{data}, nil
}

func (s *Source) decode(resp *http.Response, body []byte) (any, error) {
	if s.Decode != nil {
		return s.Decode(resp, body)
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return nil, nil
	}
	var out any
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

// Do sends one request with retries and returns the response and its body.
func (s *Source) Do(ctx context.Context, r *Request) (*http.Response, []byte, error) {
	client := s.Client
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	retries := s.MaxRetries
	if retries == 0 {
		retries = 3
	}
	if retries < 0 {
		retries = 0
	}
	delay := s.Backoff
	if delay <= 0 {
		delay = 500 * time.Millisecond
	}

	var payload []byte
	if r.Body != nil {
		var err error
		if payload, err = json.Marshal(r.Body); err != nil {
			return nil, nil, fmt.Errorf("encode request body: %w", err)
		}
	}
	target, err := url.Parse(r.URL)
	if err != nil {
		return nil, nil, err
	}
	if len(r.Query) > 0 {
		q := target.Query()
		for k, v := range r.Query {
			q[k] = v
		}
		target.RawQuery = q.Encode()
	}

	for attempt := 0; ; attempt++ {
		httpReq, err := http.NewRequestWithContext(ctx, r.Method, target.String(), bytes.NewReader(payload))
		if err != nil {
			return nil, nil, err
		}
		for k, v := range r.Header {
			httpReq.Header[k] = v
		}
		if payload != nil && httpReq.Header.Get("Content-Type") == "" {
			httpReq.Header.Set("Content-Type", "application/json")
		}
		if httpReq.Header.Get("Accept") == "" {
			httpReq.Header.Set("Accept", "application/json")
		}
		if s.Auth != nil {
			if err := s.Auth(httpReq); err != nil {
				return nil, nil, fmt.Errorf("auth: %w", err)
			}
		}

		resp, err := client.Do(httpReq)
		var body []byte
		if err == nil {
			body, err = io.ReadAll(resp.Body)
			resp.Body.Close()
		}
		retryable := err != nil || resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500
		if retryable && attempt < retries && ctx.Err() == nil {
			wait := delay << attempt
			if err == nil {
				if ra := retryAfter(resp.Header.Get("Retry-After")); ra > 0 {
					wait = ra
				}
			}
			select {
			case <-time.After(wait):
				continue
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			}
		}
		if err != nil {
			return nil, nil, err
		}
		if resp.StatusCode >= 400 {
			snippet := string(body)
			if len(snippet) > 500 {
				snippet = snippet[:500]
			}
			return nil, nil, &Error{r.Method, target.String(), resp.StatusCode, snippet}
		}
		return resp, body, nil
	}
}

func retryAfter(h string) time.Duration {
	if h == "" {
		return 0
	}
	if secs, err := strconv.Atoi(h); err == nil {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(h); err == nil {
		return time.Until(t)
	}
	return 0
}

// Bearer sets "Authorization: Bearer <token>".
func Bearer(token string) func(*http.Request) error {
	return func(r *http.Request) error {
		r.Header.Set("Authorization", "Bearer "+token)
		return nil
	}
}

// BearerFunc fetches the token on each request (for refreshable tokens).
func BearerFunc(token func(context.Context) (string, error)) func(*http.Request) error {
	return func(r *http.Request) error {
		t, err := token(r.Context())
		if err != nil {
			return err
		}
		r.Header.Set("Authorization", "Bearer "+t)
		return nil
	}
}

// Basic sets HTTP basic auth.
func Basic(user, password string) func(*http.Request) error {
	return func(r *http.Request) error {
		r.SetBasicAuth(user, password)
		return nil
	}
}

// HeaderAuth sets an arbitrary header, e.g. HeaderAuth("X-API-Key", key).
func HeaderAuth(name, value string) func(*http.Request) error {
	return func(r *http.Request) error {
		r.Header.Set(name, value)
		return nil
	}
}

var errBodyNotMap = errors.New("apisource: InBody pagination needs Body to be a map[string]any (or nil)")
