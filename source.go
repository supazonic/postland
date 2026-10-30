package postland

import (
	"context"
	"errors"
)

// Record is one item from a source. Sources usually produce map[string]any
// (decoded JSON, a scanned SQL row, a CSV line) but anything Lookup can
// navigate works.
type Record = any

// Batch is a group of records written in one transaction.
type Batch = []Record

// Source produces batches of records. It is the only thing the rest of
// postland needs from where the data comes from, so a new kind of source (a
// database table, files on SFTP, a queue...) only has to implement this.
//
// Batches calls fn once per batch, in order, and stops as soon as fn returns
// an error, returning that error. Release any connections before returning.
type Source interface {
	Batches(ctx context.Context, fn func(Batch) error) error
}

// SourceFunc adapts a function to the Source interface.
type SourceFunc func(ctx context.Context, fn func(Batch) error) error

func (f SourceFunc) Batches(ctx context.Context, fn func(Batch) error) error { return f(ctx, fn) }

// SliceSource serves in-memory records in batches of BatchSize (default 1000).
// Handy for tests and for plugging in data you already loaded.
type SliceSource struct {
	Records   []Record
	BatchSize int
}

func (s SliceSource) Batches(ctx context.Context, fn func(Batch) error) error {
	size := s.BatchSize
	if size <= 0 {
		size = 1000
	}
	for start := 0; start < len(s.Records); start += size {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := min(start+size, len(s.Records))
		if err := fn(s.Records[start:end]); err != nil {
			return err
		}
	}
	return nil
}

// errStop is used internally to end a source early without an error.
var errStop = errors.New("postland: stop")
