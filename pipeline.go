package postland

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"
)

// ErrorPolicy decides what happens when a record cannot be mapped.
type ErrorPolicy int

const (
	// Abort stops the run; the current batch is not written, earlier
	// batches stay committed.
	Abort ErrorPolicy = iota
	// Skip drops the record (and all its child rows) and records it in
	// Result.Rejected.
	Skip
)

// Pipeline moves records from a Source to a Sink through one or more mappings.
type Pipeline struct {
	Source   Source
	Sink     Sink
	Mappings []*TableMapping

	OnError      ErrorPolicy
	KeepRejected int          // how many rejected records to keep in Result (default 100)
	MaxBatches   int          // stop after this many batches (0 = no limit)
	Logger       *slog.Logger // defaults to slog.Default()
}

// Rejected is a record that failed mapping under the Skip policy.
type Rejected struct {
	Record Record
	Err    error
}

// Result summarises a run.
type Result struct {
	Batches       int
	Records       int
	Rows          map[string]int
	Rejected      []Rejected
	RejectedCount int
	Duration      time.Duration
}

func (r *Result) String() string {
	names := make([]string, 0, len(r.Rows))
	for k := range r.Rows {
		names = append(names, k)
	}
	sort.Strings(names)
	parts := make([]string, len(names))
	for i, k := range names {
		parts[i] = fmt.Sprintf("%s=%d", k, r.Rows[k])
	}
	rows := strings.Join(parts, ", ")
	if rows == "" {
		rows = "none"
	}
	return fmt.Sprintf("%d records in %d batches -> rows: %s; rejected: %d; %s",
		r.Records, r.Batches, rows, r.RejectedCount, r.Duration.Round(time.Millisecond))
}

// Tables returns every table the mappings write to, parents first.
func (p *Pipeline) Tables() []*Table {
	var out []*Table
	for _, m := range p.Mappings {
		for _, t := range m.Tables() {
			if !containsTable(out, t) {
				out = append(out, t)
			}
		}
	}
	return out
}

// Transform maps one batch. Each record is mapped completely before any of
// its rows are kept, so a failing record never leaves partial rows behind.
func (p *Pipeline) Transform(batch Batch, res *Result) (*Rows, error) {
	out := NewRows(p.Tables()...)
	keep := p.KeepRejected
	if keep == 0 {
		keep = 100
	}
	for _, rec := range batch {
		recRows := NewRows()
		var err error
		for _, m := range p.Mappings {
			var r *Rows
			if r, err = m.Map(rec); err != nil {
				break
			}
			recRows.Merge(r)
		}
		if err != nil {
			if p.OnError == Abort {
				return nil, err
			}
			res.RejectedCount++
			if len(res.Rejected) < keep {
				res.Rejected = append(res.Rejected, Rejected{rec, err})
			}
			p.logger().Warn("rejected record", "error", err)
			continue
		}
		out.Merge(recRows)
	}
	return out, nil
}

// Run executes the pipeline until the source is exhausted, MaxBatches is
// reached, an error occurs, or ctx is cancelled. The returned Result is
// valid (and reflects committed work) even when err is non-nil.
func (p *Pipeline) Run(ctx context.Context) (*Result, error) {
	res := &Result{Rows: map[string]int{}}
	start := time.Now()
	defer func() { res.Duration = time.Since(start) }()

	if p.Source == nil || p.Sink == nil || len(p.Mappings) == 0 {
		return res, errors.New("postland: pipeline needs a Source, a Sink and at least one mapping")
	}
	for _, m := range p.Mappings {
		if err := m.Compile(); err != nil {
			return res, err
		}
	}
	if err := p.Sink.Prepare(ctx, p.Tables()); err != nil {
		return res, fmt.Errorf("prepare sink: %w", err)
	}

	err := p.Source.Batches(ctx, func(batch Batch) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		rows, err := p.Transform(batch, res)
		if err != nil {
			return err
		}
		counts, err := p.Sink.Write(ctx, rows)
		if err != nil {
			return fmt.Errorf("write batch %d: %w", res.Batches+1, err)
		}
		for name, n := range counts {
			res.Rows[name] += n
		}
		res.Batches++
		res.Records += len(batch)
		p.logger().Info("batch written", "batch", res.Batches, "records", len(batch))
		if p.MaxBatches > 0 && res.Batches >= p.MaxBatches {
			return errStop
		}
		return nil
	})
	if errors.Is(err, errStop) {
		err = nil
	}
	return res, err
}

func (p *Pipeline) logger() *slog.Logger {
	if p.Logger != nil {
		return p.Logger
	}
	return slog.Default()
}
