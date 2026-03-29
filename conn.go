package db

import (
	"context"

	"github.com/pkg/errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// NewConn 根据提供的数据库名称从全局数据库连接池中创建一个新的 Conn 实例。
func MultiConn(dbNames ...string) ([]Conn, error) {
	conns := make([]Conn, len(dbNames))
	for i, v := range dbNames {
		p := databasePool[v]
		if p == nil {
			return nil, errors.Errorf("数据库[%s]不存在", v)
		}
		conns[i] = Conn{pool: databasePool[v]}
	}

	return conns, nil
}

// Conn 包装了 pgxpool.Pool，提供了一个非事务性的数据库连接。
// 它用于执行单个的、不需要事务保证的数据库操作。
type Conn struct {
	pool *pgxpool.Pool
}

// Query 在连接上执行一个查询，并返回 pgx.Rows。
// 这是对 pgxpool.Pool.Query 的一个简单包装。
func (p Conn) Query(query string, args ...any) (pgx.Rows, error) {
	if p.pool == nil {
		return nil, errors.New("连接池为 nil")
	}
	rows, err := p.pool.Query(context.Background(), query, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return rows, nil
}

// Exec 在连接上执行一个 SQL 命令（如 INSERT, UPDATE, DELETE），并返回命令的结果标签。
// 这是对 pgxpool.Pool.Exec 的一个简单包装。
func (p Conn) Exec(query string, args ...any) (pgconn.CommandTag, error) {
	result := pgconn.CommandTag{}
	if p.pool == nil {
		return result, errors.New("连接池为 nil")
	}

	result, err := p.pool.Exec(context.Background(), query, args...)
	if err != nil {
		return result, errors.WithStack(err)
	}
	return result, nil
}

// QueryScan 是一个便捷函数，它执行查询，并将结果自动扫描到指定的类型切片中。
// 它负责处理 rows 的关闭。
func QueryScan[T any](conn Conn, query string, args ...any) ([]T, error) {
	rows, err := conn.Query(query, args...)
	if err != nil {
		return nil, err
	}

	// Scan 内部会执行 defer rows.Close()，故去掉多余关闭
	//defer rows.Close()
	result, err := Scan[T](rows)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// QueryScanOne 与 QueryScan 类似，但只返回查询结果的第一行。
// 它返回结果、一个布尔值（指示是否找到记录）和可能的错误。
func QueryScanOne[T any](conn Conn, query string, args ...any) (T, bool, error) {

	var zero T
	rows, err := conn.Query(query, args...)
	if err != nil {
		return zero, false, err
	}
	// ScanOne 内部会执行 defer rows.Close()，故去掉多余关闭
	// defer rows.Close()

	result, exists, err := ScanOne[T](rows)
	if err != nil {
		return zero, false, err
	}
	return result, exists, nil
}
