package repository

import (
	"time"

	"github.com/jmoiron/sqlx"
)

type Repositort[T any] struct {
	db    *sqlx.DB
	query string
}

func NewRepository[T any](db *sqlx.DB, query string) *Repositort[T] {
	return &Repositort[T]{db: db, query: query}
}

func (r *Repositort[T]) Stream(since time.Time) (<-chan T, <-chan error) {
	out := make(chan T)
	errs := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errs)

		rows, err := r.db.Queryx(r.query, since)
		if err != nil {
			errs <- err
			return
		}
		defer rows.Close()

		for rows.Next() {
			var item T
			if err := rows.StructScan(&item); err != nil {
				errs <- err
				return
			}
			out <- item
		}

		if err := rows.Err(); err != nil {
			errs <- err
		}
	}()

	return out, errs
}
