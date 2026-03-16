package db

import (
	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
)

func Scan[T any](rows pgx.Rows) ([]T, error) {
	result, err := pgx.CollectRows(rows, pgx.RowTo[T])
	if err != nil {
		return nil, errors.Wrap(err, "failed to collect rows")
	}
	return result, nil
}

func ScanOne[T any](rows pgx.Rows) (T, bool, error) {
	result, err := pgx.CollectOneRow(rows, pgx.RowTo[T])
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			var zero T
			return zero, false, nil
		}
		return result, false, errors.Wrap(err, "failed to collect single row")
	}
	return result, true, nil
}
