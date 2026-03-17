package db

import (
	"log"

	stderrors "errors"

	"github.com/pkg/errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"golang.org/x/net/context"
)

// MultiTx 单数据库事物和跨数据库事物
func MultiTx(dbNames ...DBName) ([]TxConn, func(error) error, error) {
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

	rollBackAll := func() error {
		var errJoin error
		for _, txConn := range txConns {
			if txConn.tx == nil {
				continue
			}
			rollbackErr := txConn.tx.Rollback(context.Background())
			if rollbackErr != nil {
				errJoin = stderrors.Join(errJoin, rollbackErr)
			}
		}
		return errJoin
	}

	//构建事物期间出错
	if err != nil {
		rollbackErr := rollBackAll()
		err = stderrors.Join(err, rollbackErr)
		return nil, nil, errors.Wrap(err, "")
	}

	//TODO 在这里**不是**两阶段提交（2PC），这里只会简单的检查事物连接是有关闭。
	commit := func(err error) error {
		if err != nil { //输入本身是错误
			rollbackErr := rollBackAll()
			err = stderrors.Join(err, rollbackErr)
			return errors.Wrap(err, "")
		}
		//检查事物可用性
		for _, txConn := range txConns {
			var isCloseErr error
			if txConn.tx.Conn().IsClosed() { //其中一个tx不可用了
				dbName := databaseName(txConn)
				isCloseErr = stderrors.Join(isCloseErr, errors.Errorf("%s already closed", dbName))
			}
			if isCloseErr != nil {
				rollbackErr := rollBackAll()
				err = stderrors.Join(err, rollbackErr)
				return errors.Wrap(err, "")
			}
		}

		//执行提交
		for _, txConn := range txConns {
			if commitErr := txConn.tx.Commit(context.Background()); commitErr != nil {
				//某个事物提交失败了
				rollbackErr := rollBackAll()
				err = stderrors.Join(err, commitErr, rollbackErr)
				return errors.Wrap(err, "")
			}
		}
		return err
	}

	return txConns, commit, nil
}

// Tx 对单个数据库使用 MultiTx 的简化
func Tx(dbName DBName) (TxConn, func(error) error, error) {
	var txConn TxConn
	txConns, commit, err := MultiTx(dbName)
	if err != nil {
		return txConn, nil, errors.Wrap(err, "")
	}
	return txConns[0], commit, nil
}

// Tx2 对2个数据库使用 MultiTx 的简化
func Tx2(dbName1, dbName2 DBName) (TxConn, TxConn, func(error) error, error) {
	var txConn TxConn
	txConns, commit, err := MultiTx(dbName1, dbName2)
	if err != nil {
		return txConn, txConn, nil, errors.Wrap(err, "")
	}

	return txConns[0], txConns[1], commit, nil
}

// Tx3 对3个数据库使用 MultiTx 的简化
func Tx3(dbName1, dbName2, dbName3 DBName) (TxConn, TxConn, TxConn, func(error) error, error) {
	var txConn TxConn
	txConns, commit, err := MultiTx(dbName1, dbName2, dbName3)
	if err != nil {
		return txConn, txConn, txConn, nil, errors.Wrap(err, "")
	}
	return txConns[0], txConns[1], txConns[2], commit, nil
}

// Tx4 对4个数据库使用 MultiTx 的简化
func Tx4(dbName1, dbName2, dbName3, dbName4 DBName) (TxConn, TxConn, TxConn, TxConn, func(error) error, error) {
	var txConn TxConn
	txConns, commit, err := MultiTx(dbName1, dbName2, dbName3, dbName4)
	if err != nil {
		return txConn, txConn, txConn, txConn, nil, errors.Wrap(err, "")
	}
	return txConns[0], txConns[1], txConns[2], txConns[3], commit, nil
}

// Tx5 对5个数据库使用 MultiTx 的简化
func Tx5(dbName1, dbName2, dbName3, dbName4, dbName5 DBName) (TxConn, TxConn, TxConn, TxConn, TxConn, func(error) error, error) {
	var txConn TxConn
	txConns, commit, err := MultiTx(dbName1, dbName2, dbName3, dbName4, dbName5)
	if err != nil {
		return txConn, txConn, txConn, txConn, txConn, nil, errors.Wrap(err, "")
	}

	return txConns[0], txConns[1], txConns[2], txConns[3], txConns[4], commit, nil
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
		return result, errors.Wrap(err, "")
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

func databaseName(txConn TxConn) string {
	return txConn.tx.Conn().Config().Database
}
