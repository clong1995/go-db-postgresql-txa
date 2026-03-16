package db

import (
	"context"

	"github.com/pkg/errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Conn struct {
	pool *pgxpool.Pool
}

func NewConn(name DBName) Conn {
	return Conn{pool: databasePool[name]}
}

func (p Conn) Query(query string, args ...any) (pgx.Rows, error) {
	if p.pool == nil {
		return nil, errors.New("pool is nil")
	}
	rows, err := p.pool.Query(context.Background(), query, args...)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	return rows, nil
}

func (p Conn) Exec(query string, args ...any) (pgconn.CommandTag, error) {
	result := pgconn.CommandTag{}
	if p.pool == nil {
		return result, errors.New("pool is nil")
	}

	result, err := p.pool.Exec(context.Background(), query, args...)
	if err != nil {
		return result, errors.Wrap(err, "")
	}
	return result, nil
}

// ConnQueryScan 自动扫描结果并关闭rows，对 Conn.Query 的包装
func ConnQueryScan[T any](conn Conn, query string, args ...any) ([]T, error) {
	rows, err := conn.Query(query, args...)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	defer rows.Close()
	result, err := Scan[T](rows)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	return result, nil
}

// ConnQueryScanOne 自动扫描结果并关闭rows，对 Conn.Query 的包装
func ConnQueryScanOne[T any](conn Conn, query string, args ...any) (T, bool, error) {
	var result T
	scan, err := ConnQueryScan[T](conn, query, args...)
	if err != nil {
		return result, false, errors.Wrap(err, "")
	}
	if len(scan) == 0 {
		return result, false, nil
	}
	return scan[0], true, nil
}
