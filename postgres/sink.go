package postgres

import (
	"context"
	"fmt"

	"github.com/supazonic/postland"
)

// Sink writes pipeline batches into PostgreSQL, one transaction per batch.
type Sink struct {
	DB *DB

	// Mode is the default write mode (Upsert if empty). TableModes overrides
	// it per table, keyed by table name or qualified name.
	Mode       Mode
	TableModes map[string]Mode

	// SkipCreate disables CREATE TABLE/SCHEMA/INDEX in Prepare.
	SkipCreate bool
	// NoAddColumns disables adding columns missing from existing tables.
	NoAddColumns bool
}

var _ postland.Sink = (*Sink)(nil)

// NewSink returns an upserting sink on db.
func NewSink(db *DB) *Sink { return &Sink{DB: db, Mode: Upsert} }

// ModeFor returns the write mode for t.
func (s *Sink) ModeFor(t *postland.Table) Mode {
	if m, ok := s.TableModes[t.QualifiedName()]; ok {
		return m
	}
	if m, ok := s.TableModes[t.Name]; ok {
		return m
	}
	if s.Mode == "" {
		return Upsert
	}
	return s.Mode
}

// Prepare creates/evolves tables and truncates Truncate-mode tables, atomically.
func (s *Sink) Prepare(ctx context.Context, tables []*postland.Table) error {
	if s.DB == nil {
		return errNoDB
	}
	for _, t := range tables {
		if m := s.ModeFor(t); !m.valid() {
			return fmt.Errorf("table %s: unknown write mode %q", t.QualifiedName(), m)
		}
	}
	return s.DB.InTx(ctx, func(tx *DB) error {
		if !s.SkipCreate {
			for _, t := range tables {
				if err := tx.CreateTable(ctx, t, !s.NoAddColumns); err != nil {
					return err
				}
			}
		}
		var trunc []*postland.Table
		for _, t := range tables {
			if s.ModeFor(t) == Truncate {
				trunc = append(trunc, t)
			}
		}
		return tx.Truncate(ctx, trunc...)
	})
}

// Write stores one batch (parents before children) in a single transaction.
func (s *Sink) Write(ctx context.Context, rows *postland.Rows) (map[string]int, error) {
	if s.DB == nil {
		return nil, errNoDB
	}
	counts := map[string]int{}
	err := s.DB.InTx(ctx, func(tx *DB) error {
		for _, t := range rows.Tables() {
			n, err := tx.Write(ctx, t, rows.For(t), s.ModeFor(t))
			if err != nil {
				return err
			}
			counts[t.QualifiedName()] = n
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return counts, nil
}
