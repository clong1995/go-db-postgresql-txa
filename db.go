package db

import (
	"context"
	"errors"
	"log"

	pcolor "github.com/clong1995/go-ansi-color"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DB struct {
	tx   pgx.Tx
	pool *pgxpool.Pool
}

func Conn(name DBName) DB {
	return DB{pool: dataPool[name]}
}

func (p DB) Query(query string, args ...any) (rows pgx.Rows, err error) {
	if p.tx != nil {
		if rows, err = p.tx.Query(context.Background(), query, args...); err != nil {
			log.Println(pcolor.Error(err))
			return
		}
		return
	}
	if p.pool == nil {
		err = errors.New("pool is nil")
		log.Println(pcolor.Error(err))
		return
	}
	if rows, err = p.pool.Query(context.Background(), query, args...); err != nil {
		log.Println(pcolor.Error(err))
		return
	}
	return
}

func (p DB) Exec(query string, args ...any) (result pgconn.CommandTag, err error) {
	if p.tx != nil {
		if result, err = p.tx.Exec(context.Background(), query, args...); err != nil {
			log.Println(pcolor.Error(err))
			return
		}
		return
	}

	if p.pool == nil {
		err = errors.New("pool is nil")
		log.Println(pcolor.Error(err))
		return
	}

	if result, err = p.pool.Exec(context.Background(), query, args...); err != nil {
		log.Println(pcolor.Error(err))
		return
	}
	return
}

func (p DB) Batch(query string, data [][]any) (err error) {
	if p.tx == nil {
		err = errors.New("tx is nil")
		log.Println(pcolor.Error(err))
		return
	}
	batch := &pgx.Batch{}
	for _, v := range data {
		_ = batch.Queue(query, v...)
	}
	br := p.tx.SendBatch(context.Background(), batch)
	if err = br.Close(); err != nil {
		log.Println(pcolor.Error(err))
		return
	}
	return
}

func (p DB) Copy(tableName string, columnNames []string, data [][]any) (rowsAffected int64, err error) {
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
		log.Println(pcolor.Error(err))
		return
	}
	return
}

// QueryScan 自动扫描结果并关闭rows，对 DB.Query 的包装
func QueryScan[T any](db DB, query string, args ...any) (result []T, err error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		log.Println(pcolor.Error(err))
		return
	}
	defer rows.Close()
	if result, err = Scan[T](rows); err != nil {
		log.Println(pcolor.Error(err))
		return
	}
	return
}
