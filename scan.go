package db

import (
	"log"
	"reflect"

	pcolor "github.com/clong1995/go-ansi-color"
	"github.com/jackc/pgx/v5"
)

func Scan[T any](rows pgx.Rows) (result []T, err error) {
	var obj T
	typ := reflect.TypeOf(obj)
	if typ.Kind() == reflect.Struct {
		if result, err = pgx.CollectRows[T](rows, pgx.RowToStructByPos[T]); err != nil {
			log.Println(pcolor.Err("CollectRows error: %v", err))
			return
		}
	} else {
		for rows.Next() {
			if err = rows.Scan(&obj); err != nil {
				log.Println(pcolor.Error(err))
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
			log.Println(pcolor.Err("CollectRows error: %v", err))
			return
		}
	} else {
		for rows.Next() {
			if err = rows.Scan(&result); err != nil {
				log.Println(pcolor.Error(err))
				return
			}
		}
	}
	return
}
