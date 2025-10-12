package db

import (
	"context"
	"errors"
	"log"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Conn struct {
	tx   pgx.Tx
	pool *pgxpool.Pool
}

func NewConn(name DBName) *Conn {
	return &Conn{pool: dataPool[name]}
}

func (p Conn) Query(query string, args ...any) (rows pgx.Rows, err error) {
	if p.tx != nil {
		if rows, err = p.tx.Query(context.Background(), query, args...); err != nil {
			log.Println(err)
			return
		}
		return
	}
	if p.pool == nil {
		err = errors.New("pool is nil")
		log.Println(err)
		return
	}
	if rows, err = p.pool.Query(context.Background(), query, args...); err != nil {
		log.Println(err)
		return
	}
	return
}

func (p Conn) Exec(query string, args ...any) (result pgconn.CommandTag, err error) {
	if p.tx != nil {
		if result, err = p.tx.Exec(context.Background(), query, args...); err != nil {
			log.Println(err)
			return
		}
		return
	}

	if p.pool == nil {
		err = errors.New("pool is nil")
		log.Println(err)
		return
	}

	if result, err = p.pool.Exec(context.Background(), query, args...); err != nil {
		log.Println(err)
		return
	}
	return
}

func (p Conn) Batch(query string, data [][]any) (err error) {
	if p.tx == nil {
		err = errors.New("tx is nil")
		log.Println(err)
		return
	}
	batch := &pgx.Batch{}
	for _, v := range data {
		_ = batch.Queue(query, v...)
	}
	br := p.tx.SendBatch(context.Background(), batch)
	if err = br.Close(); err != nil {
		log.Println(err)
		return
	}
	return
}

func (p Conn) Copy(tableName string, columnNames []string, data [][]any) (rowsAffected int64, err error) {
	if p.tx == nil {
		err = errors.New("tx is nil")
		log.Println(err)
		return
	}
	table := pgx.Identifier{tableName}
	if rowsAffected, err = p.tx.CopyFrom(
		context.Background(),
		table,
		columnNames,
		pgx.CopyFromRows(data),
	); err != nil {
		log.Println(err)
		return
	}
	return
}

// QueryScan 自动扫描结果并关闭rows，对 Conn.Query 的包装
func QueryScan[T any](conn *Conn, query string, args ...any) (result []T, err error) {
	rows, err := conn.Query(query, args...)
	if err != nil {
		log.Println(err)
		return
	}
	defer rows.Close()
	if result, err = Scan[T](rows); err != nil {
		log.Println(err)
		return
	}
	return
}

// QueryScanOne 自动扫描结果并关闭rows，对 Conn.Query 的包装
func QueryScanOne[T any](conn *Conn, query string, args ...any) (result T, err error) {
	rows, err := conn.Query(query, args...)
	if err != nil {
		log.Println(err)
		return
	}
	defer rows.Close()
	if result, err = ScanOne[T](rows); err != nil {
		log.Println(err)
		return
	}
	return
}
