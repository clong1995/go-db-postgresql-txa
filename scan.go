package db

import (
	"reflect"

	pcolor "github.com/clong1995/go-ansi-color"
	"github.com/jackc/pgx/v5"
)

func Scan[T any](rows pgx.Rows) (result []T, err error) {
	var obj T
	typ := reflect.TypeOf(obj)
	if typ.Kind() == reflect.Struct {
		if result, err = pgx.CollectRows[T](rows, pgx.RowToStructByPos[T]); err != nil {
			pcolor.PrintError(err)
			return
		}
	} else {
		for rows.Next() {
			if err = rows.Scan(&obj); err != nil {
				pcolor.PrintError(err)
				return
			}
			result = append(result, obj)
		}
	}
	return
}

func ScanOne[T any](rows pgx.Rows) (result T, err error) {
	var obj T
	typ := reflect.TypeOf(obj)
	if typ.Kind() == reflect.Struct {
		if result, err = pgx.CollectOneRow[T](rows, pgx.RowToStructByPos[T]); err != nil {
			pcolor.PrintError(err)
			return
		}
	} else {
		for rows.Next() {
			if err = rows.Scan(&result); err != nil {
				pcolor.PrintError(err)
				return
			}
		}
	}
	return
}
