package db

import (
	"reflect"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
)

func Scan[T any](rows pgx.Rows) ([]T, error) {
	defer rows.Close()

	if isTime[T]() { //时间
		goto Base
	}

	//结构体
	if isStruct[T]() {
		result, err := pgx.CollectRows[T](rows, pgx.RowToStructByPos[T])
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return []T{}, nil
			}
			return result, errors.Wrap(err, "")
		}
		return result, nil
	}

Base:
	//基本类型
	result, err := pgx.CollectRows(rows, pgx.RowTo[T])
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return []T{}, nil
		}
		return nil, errors.Wrap(err, "failed to collect rows")
	}
	return result, nil
}

func ScanOne[T any](rows pgx.Rows) (T, bool, error) {
	defer rows.Close()

	if isTime[T]() { //时间
		goto Base
	}

	//结构体
	if isStruct[T]() {
		result, err := pgx.CollectOneRow[T](rows, pgx.RowToStructByPos[T])
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return result, false, nil
			}
			return result, false, errors.Wrap(err, "")
		}
		return result, true, nil
	}

Base:

	result, err := pgx.CollectOneRow(rows, pgx.RowTo[T])
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return result, false, nil
		}
		return result, false, errors.Wrap(err, "failed to collect single row")
	}
	return result, true, nil
}

func isStruct[T any]() bool {
	return reflect.TypeOf((*T)(nil)).Elem().Kind() == reflect.Struct
}

func isTime[T any]() bool {
	_, ok := any(*new(T)).(time.Time)
	return ok
}
