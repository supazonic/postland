package postland

import (
	"context"
	"sync"
)

// Sink is where mapped rows end up.
type Sink interface {
	// Prepare runs once before the first batch (create tables, truncate...).
	Prepare(ctx context.Context, tables []*Table) error
	// Write stores one batch atomically and returns rows written per table
	// (keyed by qualified name).
	Write(ctx context.Context, rows *Rows) (map[string]int, error)
}

// MemorySink collects rows in memory; useful for tests and dry runs.
type MemorySink struct {
	mu   sync.Mutex
	Data map[string][]Row
}

func (s *MemorySink) Prepare(_ context.Context, tables []*Table) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Data == nil {
		s.Data = map[string][]Row{}
	}
	for _, t := range tables {
		if _, ok := s.Data[t.QualifiedName()]; !ok {
			s.Data[t.QualifiedName()] = nil
		}
	}
	return nil
}

func (s *MemorySink) Write(_ context.Context, rows *Rows) (map[string]int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Data == nil {
		s.Data = map[string][]Row{}
	}
	counts := map[string]int{}
	for _, t := range rows.Tables() {
		s.Data[t.QualifiedName()] = append(s.Data[t.QualifiedName()], rows.For(t)...)
		counts[t.QualifiedName()] = len(rows.For(t))
	}
	return counts, nil
}
