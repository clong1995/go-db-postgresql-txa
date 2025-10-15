package db

import (
	"errors"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"golang.org/x/net/context"
)

// MultiTx 但数据库事物和跨数据库事物
func MultiTx(dbNames ...DBName) (txConns []TxConn, commit func(err error), err error) {
	txConns = make([]TxConn, len(dbNames))
	for i, v := range dbNames {
		p := databasePool[v]
		if p == nil {
			err = errors.New(fmt.Sprintf("db[%s] is not exist", v))
			log.Println(err)
			break
		}
		var tx pgx.Tx
		if tx, err = p.Begin(context.Background()); err != nil {
			log.Println(err)
			break
		}
		txConns[i] = TxConn{
			tx: tx,
		}
	}

	if err != nil {
		//回滚
		for _, txConn := range txConns {
			if txConn.tx == nil {
				continue
			}
			if rollbackErr := txConn.tx.Rollback(context.Background()); rollbackErr != nil {
				log.Println(rollbackErr)
			}
		}
		return
	}

	commit = func(err error) {
		for _, txConn := range txConns {
			if txConn.tx == nil || err != nil {
				if rollbackErr := txConn.tx.Rollback(context.Background()); rollbackErr != nil {
					log.Println(rollbackErr)
				}
			} else {
				if commitErr := txConn.tx.Commit(context.Background()); commitErr != nil {
					log.Println(commitErr)
				}
			}
		}
	}

	return
}

// Tx 对单个数据库使用 MultiTx 的简化
func Tx(dbName DBName) (txConn TxConn, commit func(err error), err error) {
	txConns, commit, err := MultiTx(dbName)
	if err != nil {
		return
	}
	txConn = txConns[0]
	return
}

// Tx2 对2个数据库使用 MultiTx 的简化
func Tx2(dbName1, dbName2 DBName) (txConn1, txConn2 TxConn, commit func(err error), err error) {
	txConns, commit, err := MultiTx(dbName1, dbName2)
	if err != nil {
		return
	}
	txConn1, txConn2 = txConns[0], txConns[1]
	return
}

// Tx3 对3个数据库使用 MultiTx 的简化
func Tx3(dbName1, dbName2, dbName3 DBName) (txConn1, txConn2, txConn3 TxConn, commit func(err error), err error) {
	txConns, commit, err := MultiTx(dbName1, dbName2, dbName3)
	if err != nil {
		return
	}
	txConn1, txConn2, txConn3 = txConns[0], txConns[1], txConns[2]
	return
}

// Tx4 对4个数据库使用 MultiTx 的简化
func Tx4(dbName1, dbName2, dbName3, dbName4 DBName) (txConn1, txConn2, txConn3, txConn4 TxConn, commit func(err error), err error) {
	txConns, commit, err := MultiTx(dbName1, dbName2, dbName3, dbName4)
	if err != nil {
		return
	}
	txConn1, txConn2, txConn3, txConn4 = txConns[0], txConns[1], txConns[2], txConns[3]
	return
}

// Tx5 对5个数据库使用 MultiTx 的简化
func Tx5(dbName1, dbName2, dbName3, dbName4, dbName5 DBName) (txConn1, txConn2, txConn3, txConn4, txConn5 TxConn, commit func(err error), err error) {
	txConns, commit, err := MultiTx(dbName1, dbName2, dbName3, dbName4, dbName5)
	if err != nil {
		return
	}
	txConn1, txConn2, txConn3, txConn4, txConn5 = txConns[0], txConns[1], txConns[2], txConns[3], txConns[4]
	return
}

type TxConn struct {
	tx pgx.Tx
}

func (p TxConn) Query(query string, args ...any) (rows pgx.Rows, err error) {
	if p.tx == nil {
		err = errors.New("tx is nil")
		log.Println(err)
		return
	}
	if rows, err = p.tx.Query(context.Background(), query, args...); err != nil {
		log.Println(err)
		return
	}
	return
}

func (p TxConn) Exec(query string, args ...any) (result pgconn.CommandTag, err error) {
	if p.tx == nil {
		err = errors.New("tx is nil")
		log.Println(err)
		return
	}

	if result, err = p.tx.Exec(context.Background(), query, args...); err != nil {
		log.Println(err)
		return
	}
	return
}

func (p TxConn) Batch(query string, data [][]any) (err error) {
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

func (p TxConn) Copy(tableName string, columnNames []string, data [][]any) (rowsAffected int64, err error) {
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

// TxQueryScan 自动扫描结果并关闭rows，对 Conn.Query 的包装
func TxQueryScan[T any](txConn TxConn, query string, args ...any) (result []T, err error) {
	rows, err := txConn.Query(query, args...)
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

// TxQueryScanOne 自动扫描结果并关闭rows，对 Conn.Query 的包装
func TxQueryScanOne[T any](txConn TxConn, query string, args ...any) (result T, err error) {
	rows, err := txConn.Query(query, args...)
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
