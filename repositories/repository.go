package repositories

import (
	"time"

	"github.com/jmoiron/sqlx"
)

type Repository[T any] struct {
	db    *sqlx.DB
	query string
}

func NewRepository[T any](db *sqlx.DB, query string) *Repository[T] {
	return &Repository[T]{db: db, query: query}
}

func (r *Repository[T]) Stream(since time.Time) (<-chan T, <-chan error) {
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
