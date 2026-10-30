package apisource

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/supazonic/postland"
)

func setParam(r *Request, name string, value any, inBody bool) error {
	if inBody {
		if r.Body == nil {
			r.Body = map[string]any{}
		}
		m, ok := r.Body.(map[string]any)
		if !ok {
			return errBodyNotMap
		}
		m[name] = value
		return nil
	}
	if r.Query == nil {
		r.Query = url.Values{}
	}
	r.Query.Set(name, fmt.Sprint(value))
	return nil
}

func lookupInt(payload any, path string) (int, bool) {
	v, ok := postland.Lookup(payload, path)
	if !ok || v == nil {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimSpace(fmt.Sprint(v)))
	return n, err == nil
}

// SinglePage makes one request.
type SinglePage struct{}

func (SinglePage) First(*Request) error         { return nil }
func (SinglePage) Next(*Page) (*Request, error) { return nil, nil }

// PageNumber handles ?page=1&per_page=100, ?page=2... It stops on an empty
// page, a page shorter than PageSize, or when TotalPagesPath says so.
type PageNumber struct {
	Param          string // default "page"
	Start          int    // first page number (default 1; set StartAtZero for 0)
	StartAtZero    bool
	PageSize       int    // optional; sent as SizeParam
	SizeParam      string // default "per_page"
	TotalPagesPath string // optional path to the total page count in the body
	InBody         bool   // put params in the JSON body instead of the query
}

func (p *PageNumber) param() string { return or(p.Param, "page") }

func (p *PageNumber) start() int {
	if p.StartAtZero {
		return 0
	}
	if p.Start == 0 {
		return 1
	}
	return p.Start
}

func (p *PageNumber) First(r *Request) error {
	if err := setParam(r, p.param(), p.start(), p.InBody); err != nil {
		return err
	}
	if p.PageSize > 0 {
		return setParam(r, or(p.SizeParam, "per_page"), p.PageSize, p.InBody)
	}
	return nil
}

func (p *PageNumber) Next(pg *Page) (*Request, error) {
	if len(pg.Records) == 0 || (p.PageSize > 0 && len(pg.Records) < p.PageSize) {
		return nil, nil
	}
	current := p.start() + pg.Number - 1
	if p.TotalPagesPath != "" {
		if total, ok := lookupInt(pg.Payload, p.TotalPagesPath); ok && current >= total+p.start()-1 {
			return nil, nil
		}
	}
	next := pg.Request.Clone()
	return next, setParam(next, p.param(), current+1, p.InBody)
}

// Offset handles ?offset=0&limit=100, ?offset=100... It stops on a short page
// or when TotalPath says every record has been read.
type Offset struct {
	Limit       int    // page size (default 100)
	OffsetParam string // default "offset"
	LimitParam  string // default "limit"
	TotalPath   string // optional path to the total record count
	Start       int
	InBody      bool
}

func (p *Offset) limit() int {
	if p.Limit <= 0 {
		return 100
	}
	return p.Limit
}

func (p *Offset) First(r *Request) error {
	if err := setParam(r, or(p.OffsetParam, "offset"), p.Start, p.InBody); err != nil {
		return err
	}
	return setParam(r, or(p.LimitParam, "limit"), p.limit(), p.InBody)
}

func (p *Offset) Next(pg *Page) (*Request, error) {
	if len(pg.Records) < p.limit() {
		return nil, nil
	}
	// offsets advance by whole pages; every page so far was full
	offset := p.Start + pg.Number*p.limit()
	if p.TotalPath != "" {
		if total, ok := lookupInt(pg.Payload, p.TotalPath); ok && offset >= total {
			return nil, nil
		}
	}
	next := pg.Request.Clone()
	return next, setParam(next, or(p.OffsetParam, "offset"), offset, p.InBody)
}

// Cursor reads a token from the body and sends it back with the next request.
type Cursor struct {
	CursorPath  string // default "next_cursor"
	Param       string // default "cursor"
	HasMorePath string // optional boolean path, e.g. "has_more"
	InBody      bool
}

func (p *Cursor) First(*Request) error { return nil }

func (p *Cursor) Next(pg *Page) (*Request, error) {
	if p.HasMorePath != "" {
		v, _ := postland.Lookup(pg.Payload, p.HasMorePath)
		if b, ok := v.(bool); !ok || !b {
			return nil, nil
		}
	}
	cur, ok := postland.Lookup(pg.Payload, or(p.CursorPath, "next_cursor"))
	if !ok || cur == nil || fmt.Sprint(cur) == "" {
		return nil, nil
	}
	next := pg.Request.Clone()
	return next, setParam(next, or(p.Param, "cursor"), cur, p.InBody)
}

// NextURL follows a next-page URL found at Path in the body or, when Path is
// empty, in the HTTP `Link: <...>; rel="next"` header. Relative URLs are
// resolved against the current one.
type NextURL struct {
	Path string
}

func (p *NextURL) First(*Request) error { return nil }

func (p *NextURL) Next(pg *Page) (*Request, error) {
	var raw string
	if p.Path == "" {
		raw = linkNext(pg.Response.Header.Values("Link"))
	} else if v, ok := postland.Lookup(pg.Payload, p.Path); ok && v != nil {
		raw = fmt.Sprint(v)
	}
	if raw == "" {
		return nil, nil
	}
	ref, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("next url %q: %w", raw, err)
	}
	next := pg.Request.Clone()
	next.URL = pg.Response.Request.URL.ResolveReference(ref).String()
	next.Query = nil // the next URL already carries its query string
	return next, nil
}

// linkNext extracts the rel="next" target from Link headers.
func linkNext(headers []string) string {
	for _, h := range headers {
		for _, part := range strings.Split(h, ",") {
			segs := strings.Split(part, ";")
			target := strings.Trim(strings.TrimSpace(segs[0]), "<>")
			for _, attr := range segs[1:] {
				attr = strings.ReplaceAll(strings.TrimSpace(attr), " ", "")
				if attr == `rel="next"` || attr == "rel=next" {
					return target
				}
			}
		}
	}
	return ""
}

func or(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}
