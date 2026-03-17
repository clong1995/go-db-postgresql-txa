package db

import (
	"reflect"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
)

// Scan 是一个通用的扫描函数，用于将 pgx.Rows 的所有行收集到一个指定类型 T 的切片中。
// 它能够处理基本类型、结构体以及 time.Time 类型。
func Scan[T any](rows pgx.Rows) ([]T, error) {
	defer rows.Close()

	// 对 time.Time 类型进行特殊处理，因为它不是一个普通的结构体，
	// pgx 需要使用特定的方式来扫描它。
	if isTime[T]() {
		goto Base
	}

	// 如果目标类型 T 是一个结构体
	if isStruct[T]() {
		// 使用 pgx 的 RowToStructByPos 函数将每一行按位置映射到结构体字段
		result, err := pgx.CollectRows[T](rows, pgx.RowToStructByPos[T])
		if err != nil {
			// 如果没有行，返回一个空切片而不是错误
			if errors.Is(err, pgx.ErrNoRows) {
				return []T{}, nil
			}
			return result, errors.Wrap(err, "扫描到结构体切片失败")
		}
		return result, nil
	}

Base:
	// 如果目标类型 T 是基本类型（如 int, string, bool 等）或 time.Time
	result, err := pgx.CollectRows(rows, pgx.RowTo[T])
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return []T{}, nil
		}
		return nil, errors.Wrap(err, "扫描到基本类型切片失败")
	}
	return result, nil
}

// ScanOne 与 Scan 类似，但只扫描查询结果的第一行。
// 它返回扫描到的值、一个布尔值（指示是否成功找到并扫描了一行）以及可能的错误。
func ScanOne[T any](rows pgx.Rows) (T, bool, error) {
	defer rows.Close()

	// 对 time.Time 进行特殊处理
	if isTime[T]() {
		goto Base
	}

	// 如果目标类型是结构体
	if isStruct[T]() {
		result, err := pgx.CollectOneRow[T](rows, pgx.RowToStructByPos[T])
		if err != nil {
			// 如果没有行，返回零值和 false
			if errors.Is(err, pgx.ErrNoRows) {
				return result, false, nil
			}
			return result, false, errors.Wrap(err, "扫描单行到结构体失败")
		}
		return result, true, nil
	}

Base:
	// 如果目标类型是基本类型或 time.Time
	result, err := pgx.CollectOneRow(rows, pgx.RowTo[T])
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return result, false, nil
		}
		return result, false, errors.Wrap(err, "扫描单行到基本类型失败")
	}
	return result, true, nil
}

// isStruct 检查泛型类型 T 是否是一个结构体。
func isStruct[T any]() bool {
	// 使用反射获取 T 的类型，并判断其种类是否为 reflect.Struct
	return reflect.TypeOf((*T)(nil)).Elem().Kind() == reflect.Struct
}

// isTime 检查泛型类型 T 是否是 time.Time。
func isTime[T any]() bool {
	// 尝试将 T 的零值断言为 time.Time 类型
	_, ok := any(*new(T)).(time.Time)
	return ok
}
