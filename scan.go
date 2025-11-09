package db

import (
	"log"
	"reflect"
	"time"

	"github.com/jackc/pgx/v5"
)

func Scan[T any](rows pgx.Rows) (result []T, err error) {
	defer rows.Close()
	var obj T
	//特例时间类型
	if _, isTime := any(obj).(time.Time); isTime {
		for rows.Next() {
			if err = rows.Scan(&obj); err != nil {
				log.Println(err)
				return
			}
			result = append(result, obj)
		}
		return
	}

	//结构体
	if typ := reflect.TypeOf(obj); typ.Kind() == reflect.Struct {
		if result, err = pgx.CollectRows[T](rows, pgx.RowToStructByPos[T]); err != nil {
			log.Println(err)
			return
		}
		return
	}

	//基本类型
	for rows.Next() {
		if err = rows.Scan(&obj); err != nil {
			log.Println(err)
			return
		}
		result = append(result, obj)
	}
	return
}

func ScanOne[T any](rows pgx.Rows) (result T, exists bool, err error) {
	/*var obj T

	//特例时间类型
	if _, isTime := any(obj).(time.Time); isTime {
		for rows.Next() {
			if err = rows.Scan(&result); err != nil {
				log.Println(err)
				return
			}
		}
		return
	}

	// 结构体
	if typ := reflect.TypeOf(obj); typ.Kind() == reflect.Struct {
		if result, err = pgx.CollectOneRow[T](rows, pgx.RowToStructByPos[T]); err != nil {
			log.Println(err)
			return
		}
		return
	}

	// 基本类型
	for rows.Next() {
		if err = rows.Scan(&result); err != nil {
			log.Println(err)
			return
		}
	}*/

	scan, err := Scan[T](rows)
	if err != nil {
		log.Println(err)
		return
	}
	if len(scan) == 0 {
		return
	}
	result, exists = scan[0], true
	return
}
