package db

import (
	"context"
	"fmt"
	"log"

	pcolor "github.com/clong1995/go-ansi-color"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Handle struct {
	name DBName
	tx   pgx.Tx
	pool *pgxpool.Pool
}

func (p Handle) Query(query string, args ...any) (rows pgx.Rows, err error) {
	if p.tx != nil {
		if rows, err = p.tx.Query(context.Background(), query, args...); err != nil {
			log.Println(pcolor.Error(err))
			return
		}
		return
	}
	if p.pool == nil {
		err = fmt.Errorf("%s pool is nil", p.name)
		log.Println(pcolor.Error(err))
		return
	}
	if rows, err = p.pool.Query(context.Background(), query, args...); err != nil {
		log.Println(pcolor.Error(err))
		return
	}
	return
}

func (p Handle) Exec(query string, args ...any) (result pgconn.CommandTag, err error) {
	if p.tx != nil {
		if result, err = p.tx.Exec(context.Background(), query, args...); err != nil {
			log.Println(pcolor.Error(err))
			return
		}
		return
	}

	if p.pool == nil {
		err = fmt.Errorf("%v pool is nil", p.name)
		log.Println(pcolor.Error(err))
		return
	}

	if result, err = p.pool.Exec(context.Background(), query, args...); err != nil {
		log.Println(pcolor.Error(err))
		return
	}
	return
}

func (p Handle) Batch(query string, data [][]any) (err error) {
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

func (p Handle) Copy(tableName string, columnNames []string, data [][]any) (rowsAffected int64, err error) {
	if p.tx == nil {
		err = fmt.Errorf("%s tx is nil", p.name)
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
