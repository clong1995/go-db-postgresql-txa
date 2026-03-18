package db

import (
	stderrors "errors"

	"github.com/pkg/errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"golang.org/x/net/context"
)

// MultiTx 用于开启一个或多个数据库的事务。
// 它返回一个 TxConn 切片，每个 TxConn 对应一个数据库连接，以及一个用于提交或回滚所有事务的函数。
// 如果在开启事务期间发生任何错误，它会尝试回滚所有已开启的事务，并返回一个错误。
func MultiTx(dbNames ...DBName) ([]TxConn, func(error) error, error) {
	txConns := make([]TxConn, len(dbNames))
	var err error
	// 遍历所有指定的数据库名称，为每个数据库开启一个事务
	for i, v := range dbNames {
		p := databasePool[v]
		if p == nil {
			err = errors.Errorf("数据库[%s]不存在", v)
			break
		}
		var tx pgx.Tx
		if tx, err = p.Begin(context.Background()); err != nil {
			err = errors.WithStack(err)
			break
		}
		txConns[i] = TxConn{
			tx: tx,
		}
	}

	// rollBackAll 是一个辅助函数，用于回滚所有已开启的事务
	rollBackAll := func() error {
		var errJoin error
		for _, txConn := range txConns {
			if txConn.tx == nil {
				continue
			}
			rollbackErr := txConn.tx.Rollback(context.Background())
			if rollbackErr != nil {
				errJoin = stderrors.Join(errJoin, errors.WithStack(rollbackErr))
			}
		}
		return errJoin
	}

	// 如果在构建事务期间出错，则回滚所有事务并返回错误
	if err != nil {
		rollbackErr := rollBackAll()
		err = stderrors.Join(err, rollbackErr)
		return nil, nil, err
	}

	// commit 函数用于最终提交或回滚事务
	// 它接收一个 error 参数。如果该 error 不为 nil，则回滚所有事务。
	// 否则，它会先检查所有事务连接是否仍然有效，然后提交它们。
	// 这并非一个严格的“两阶段提交”（2PC），而是一个简化的实现。
	commit := func(inputErr error) error {
		var errCommit error
		if inputErr != nil { // 如果传入的错误不为 nil，则回滚
			rollbackErr := rollBackAll()
			return stderrors.Join(errCommit, inputErr, rollbackErr)
		}
		// 检查所有事务的可用性
		for _, txConn := range txConns {
			//var isCloseErr error
			if txConn.tx.Conn().IsClosed() { // 如果其中一个事务连接已关闭
				dbName := databaseName(txConn)
				errCommit = errors.Errorf("数据库[%s]的事务连接已关闭", dbName)
				//关闭所有
				rollbackErr := rollBackAll()
				return stderrors.Join(errCommit, rollbackErr)
			}
		}

		// 执行提交
		for _, txConn := range txConns {
			if commitErr := txConn.tx.Commit(context.Background()); commitErr != nil {
				// 如果某个事务提交失败，则回滚所有事务
				rollbackErr := rollBackAll()
				err = stderrors.Join(err, errors.WithStack(commitErr), rollbackErr)
				return err
			}
		}
		return err
	}

	return txConns, commit, nil
}

// Tx 是对 MultiTx 的简化，用于处理单个数据库的事务。
func Tx(dbName DBName) (TxConn, func(error) error, error) {
	var txConn TxConn
	txConns, commit, err := MultiTx(dbName)
	if err != nil {
		return txConn, nil, err
	}
	return txConns[0], commit, nil
}

// Tx2 是对 MultiTx 的简化，用于处理两个数据库的事务。
func Tx2(dbName1, dbName2 DBName) (TxConn, TxConn, func(error) error, error) {
	var txConn TxConn
	txConns, commit, err := MultiTx(dbName1, dbName2)
	if err != nil {
		return txConn, txConn, nil, err
	}

	return txConns[0], txConns[1], commit, nil
}

// Tx3 是对 MultiTx 的简化，用于处理三个数据库的事务。
func Tx3(dbName1, dbName2, dbName3 DBName) (TxConn, TxConn, TxConn, func(error) error, error) {
	var txConn TxConn
	txConns, commit, err := MultiTx(dbName1, dbName2, dbName3)
	if err != nil {
		return txConn, txConn, txConn, nil, err
	}
	return txConns[0], txConns[1], txConns[2], commit, nil
}

// Tx4 是对 MultiTx 的简化，用于处理四个数据库的事务。
func Tx4(dbName1, dbName2, dbName3, dbName4 DBName) (TxConn, TxConn, TxConn, TxConn, func(error) error, error) {
	var txConn TxConn
	txConns, commit, err := MultiTx(dbName1, dbName2, dbName3, dbName4)
	if err != nil {
		return txConn, txConn, txConn, txConn, nil, err
	}
	return txConns[0], txConns[1], txConns[2], txConns[3], commit, nil
}

// Tx5 是对 MultiTx 的简化，用于处理五个数据库的事务。
func Tx5(dbName1, dbName2, dbName3, dbName4, dbName5 DBName) (TxConn, TxConn, TxConn, TxConn, TxConn, func(error) error, error) {
	var txConn TxConn
	txConns, commit, err := MultiTx(dbName1, dbName2, dbName3, dbName4, dbName5)
	if err != nil {
		return txConn, txConn, txConn, txConn, txConn, nil, err
	}

	return txConns[0], txConns[1], txConns[2], txConns[3], txConns[4], commit, nil
}

// TxConn 包装了 pgx.Tx，提供在事务上下文中执行数据库操作的方法。
type TxConn struct {
	tx pgx.Tx
}

// Query 在事务中执行一个查询，并返回 pgx.Rows。
func (p TxConn) Query(query string, args ...any) (pgx.Rows, error) {
	if p.tx == nil {
		return nil, errors.New("事务为 nil")
	}
	rows, err := p.tx.Query(context.Background(), query, args...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return rows, nil
}

// Exec 在事务中执行一个 SQL 命令（如 INSERT, UPDATE, DELETE），并返回命令的结果标签。
func (p TxConn) Exec(query string, args ...any) (pgconn.CommandTag, error) {
	var result pgconn.CommandTag
	if p.tx == nil {
		return result, errors.New("事务为 nil")
	}

	result, err := p.tx.Exec(context.Background(), query, args...)
	if err != nil {
		return result, errors.WithStack(err)
	}
	return result, nil
}

// Batch 在事务中执行批量操作。
func (p TxConn) Batch(query string, data [][]any) error {
	if p.tx == nil {
		return errors.New("事务为 nil")
	}
	batch := &pgx.Batch{}
	for _, v := range data {
		_ = batch.Queue(query, v...)
	}
	br := p.tx.SendBatch(context.Background(), batch)
	if err := br.Close(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// Copy 在事务中使用 PostgreSQL 的 COPY协议 从数据源高效地批量插入数据。
func (p TxConn) Copy(tableName string, columnNames []string, data [][]any) (int64, error) {
	if p.tx == nil {
		return 0, errors.New("事务为 nil")
	}
	table := pgx.Identifier{tableName}
	rowsAffected, err := p.tx.CopyFrom(
		context.Background(),
		table,
		columnNames,
		pgx.CopyFromRows(data),
	)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return rowsAffected, nil
}

// TxQueryScan 是一个便捷函数，它在事务中执行查询，并将结果自动扫描到指定的类型切片中。
func TxQueryScan[T any](txConn TxConn, query string, args ...any) (result []T, err error) {
	rows, err := txConn.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result, err = Scan[T](rows)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// TxQueryScanOne 与 TxQueryScan 类似，但只返回查询结果的第一行。
// 它返回结果、一个布尔值（指示是否找到记录）和可能的错误。
func TxQueryScanOne[T any](txConn TxConn, query string, args ...any) (T, bool, error) {
	var result T
	scan, err := TxQueryScan[T](txConn, query, args...)
	if err != nil {
		return result, false, err
	}
	if len(scan) == 0 {
		return result, false, nil
	}
	return scan[0], true, nil
}

// databaseName 从事务连接中获取数据库的名称。
func databaseName(txConn TxConn) string {
	return txConn.tx.Conn().Config().Database
}
