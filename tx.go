package db

import (
	"log"

	"github.com/pkg/errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"golang.org/x/net/context"
)

// MultiTx 单数据库事物和跨数据库事物
func MultiTx(dbNames ...DBName) ([]TxConn, func(err error), error) {
	txConns := make([]TxConn, len(dbNames))
	var err error
	for i, v := range dbNames {
		p := databasePool[v]
		if p == nil {
			err = errors.Errorf("db[%s] is not exist", v)
			break
		}
		var tx pgx.Tx
		if tx, err = p.Begin(context.Background()); err != nil {
			err = errors.Wrap(err, "")
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

			//采用Rollback的方式关闭开启的事物，或者有更好的方式？
			if rollbackErr := txConn.tx.Rollback(context.Background()); rollbackErr != nil {
				//TODO 这个错误怎么处理？
			}
		}
		return nil, nil, errors.Wrap(err, "")
	}

	//TODO 在这里有潜在问题，如果某库提交成功后，某库提交失败，则无法会滚
	commit := func(err error) {
		for _, txConn := range txConns {
			if txConn.tx == nil || err != nil {
				if rollbackErr := txConn.tx.Rollback(context.Background()); rollbackErr != nil {
					//TODO 这个错误怎么处理？
				}
			} else {
				if commitErr := txConn.tx.Commit(context.Background()); commitErr != nil {
					//TODO 这个错误怎么处理？
				}
			}
		}
	}

	return txConns, commit, nil
}

// Tx 对单个数据库使用 MultiTx 的简化
func Tx(dbName DBName) (TxConn, func(err error), error) {
	var txConn TxConn
	txConns, commit, err := MultiTx(dbName)
	if err != nil {
		return txConn, nil, errors.Wrap(err, "")
	}
	return txConns[0], commit, nil
}

// Tx2 对2个数据库使用 MultiTx 的简化
func Tx2(dbName1, dbName2 DBName) (TxConn, TxConn, func(err error), error) {
	var txConn TxConn
	txConns, commit, err := MultiTx(dbName1, dbName2)
	if err != nil {
		return txConn, txConn, nil, errors.Wrap(err, "")
	}

	return txConns[0], txConns[1], commit, errors.Wrap(err, "")
}

// Tx3 对3个数据库使用 MultiTx 的简化
func Tx3(dbName1, dbName2, dbName3 DBName) (TxConn, TxConn, TxConn, func(err error), error) {
	var txConn TxConn
	txConns, commit, err := MultiTx(dbName1, dbName2, dbName3)
	if err != nil {
		return txConn, txConn, txConn, nil, errors.Wrap(err, "")
	}
	return txConns[0], txConns[1], txConns[2], commit, errors.Wrap(err, "")
}

// Tx4 对4个数据库使用 MultiTx 的简化
func Tx4(dbName1, dbName2, dbName3, dbName4 DBName) (TxConn, TxConn, TxConn, TxConn, func(err error), error) {
	var txConn TxConn
	txConns, commit, err := MultiTx(dbName1, dbName2, dbName3, dbName4)
	if err != nil {
		return txConn, txConn, txConn, txConn, nil, errors.Wrap(err, "")
	}
	return txConns[0], txConns[1], txConns[2], txConns[3], commit, errors.Wrap(err, "")
}

// Tx5 对5个数据库使用 MultiTx 的简化
func Tx5(dbName1, dbName2, dbName3, dbName4, dbName5 DBName) (TxConn, TxConn, TxConn, TxConn, TxConn, func(err error), error) {
	var txConn TxConn
	txConns, commit, err := MultiTx(dbName1, dbName2, dbName3, dbName4, dbName5)
	if err != nil {
		return txConn, txConn, txConn, txConn, txConn, nil, errors.Wrap(err, "")
	}

	return txConns[0], txConns[1], txConns[2], txConns[3], txConns[4], commit, errors.Wrap(err, "")
}

type TxConn struct {
	tx pgx.Tx
}

func (p TxConn) Query(query string, args ...any) (pgx.Rows, error) {
	if p.tx == nil {
		return nil, errors.New("tx is nil")
	}
	rows, err := p.tx.Query(context.Background(), query, args...)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	return rows, nil
}

func (p TxConn) Exec(query string, args ...any) (pgconn.CommandTag, error) {
	var result pgconn.CommandTag
	if p.tx == nil {
		return result, errors.New("tx is nil")
	}

	result, err := p.tx.Exec(context.Background(), query, args...)
	if err != nil {
		return result, errors.New("tx is nil")
	}
	return result, nil
}

func (p TxConn) Batch(query string, data [][]any) error {
	if p.tx == nil {
		return errors.New("tx is nil")
	}
	batch := &pgx.Batch{}
	for _, v := range data {
		_ = batch.Queue(query, v...)
	}
	br := p.tx.SendBatch(context.Background(), batch)
	if err := br.Close(); err != nil {
		return errors.Wrap(err, "")
	}
	return nil
}

func (p TxConn) Copy(tableName string, columnNames []string, data [][]any) (int64, error) {
	if p.tx == nil {
		return 0, errors.New("tx is nil")
	}
	table := pgx.Identifier{tableName}
	rowsAffected, err := p.tx.CopyFrom(
		context.Background(),
		table,
		columnNames,
		pgx.CopyFromRows(data),
	)
	if err != nil {
		return 0, errors.Wrap(err, "")
	}
	return rowsAffected, nil
}

// TxQueryScan 自动扫描结果并关闭rows，对 Conn.Query 的包装
func TxQueryScan[T any](txConn TxConn, query string, args ...any) (result []T, err error) {
	rows, err := txConn.Query(query, args...)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	defer rows.Close()
	result, err = Scan[T](rows)
	if err != nil {
		return nil, errors.Wrap(err, "")
	}
	return result, nil
}

// TxQueryScanOne 自动扫描结果并关闭rows，对 Conn.Query 的包装
func TxQueryScanOne[T any](txConn TxConn, query string, args ...any) (T, bool, error) {
	var result T
	scan, err := TxQueryScan[T](txConn, query, args...)
	if err != nil {
		log.Println(err)
		return result, false, errors.Wrap(err, "")
	}
	if len(scan) == 0 {
		return result, false, nil
	}
	return scan[0], true, nil
}
