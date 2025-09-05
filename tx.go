package db

import (
	"errors"
	"fmt"
	"log"

	pcolor "github.com/clong1995/go-ansi-color"
	"golang.org/x/net/context"
)

// MultiTx 但数据库事物和跨数据库事物
func MultiTx(dbNames []DBName, handle func([]DB) (err error)) (err error) {
	dbs := make([]DB, len(dbNames))
	for i, v := range dbNames {
		p := dataPool[v]
		if p == nil {
			err = errors.New(fmt.Sprintf("db[%s] is not exist", v))
			log.Println(pcolor.Error(err))
			return
		}
		dbs[i] = DB{}
		if dbs[i].tx, err = p.Begin(context.Background()); err != nil {
			log.Println(pcolor.Error(err))
			return
		}
	}

	defer func() {
		for _, d := range dbs {
			if d.tx == nil {
				continue
			}
			if err != nil {
				if rollbackErr := d.tx.Rollback(context.Background()); rollbackErr != nil {
					log.Println(pcolor.Error(rollbackErr))
				}
			} else {
				if commitErr := d.tx.Commit(context.Background()); commitErr != nil {
					log.Println(pcolor.Error(commitErr))
				}
			}
		}
	}()

	if err = handle(dbs); err != nil {
		log.Println(pcolor.Error(err))
		return
	}

	return
}

// Tx 对单个数据库使用 MultiTx 的简化
func Tx(dbName DBName, handle func(DB) (err error)) (err error) {
	return MultiTx([]DBName{dbName}, func(dbs []DB) (err error) {
		return handle(dbs[0])
	})
}

// Tx2 对2个数据库使用 MultiTx 的简化
func Tx2(dbName1, dbName2 DBName, handle func(db1, db2 DB) (err error)) (err error) {
	return MultiTx([]DBName{dbName1, dbName2}, func(dbs []DB) (err error) {
		return handle(dbs[0], dbs[1])
	})
}

// Tx3 对3个数据库使用 MultiTx 的简化
func Tx3(dbName1, dbName2, dbName3 DBName, handle func(db1, db2, db3 DB) (err error)) (err error) {
	return MultiTx([]DBName{dbName1, dbName2, dbName3}, func(dbs []DB) (err error) {
		return handle(dbs[0], dbs[1], dbs[2])
	})
}

// Tx4 对4个数据库使用 MultiTx 的简化
func Tx4(dbName1, dbName2, dbName3, dbName4 DBName, handle func(db1, db2, db3, db4 DB) (err error)) (err error) {
	return MultiTx([]DBName{dbName1, dbName2, dbName3, dbName4}, func(dbs []DB) (err error) {
		return handle(dbs[0], dbs[1], dbs[2], dbs[3])
	})
}

// Tx5 对5个数据库使用 MultiTx 的简化
func Tx5(dbName1, dbName2, dbName3, dbName4, dbName5 DBName, handle func(db1, db2, db3, db4, db5 DB) (err error)) (err error) {
	return MultiTx([]DBName{dbName1, dbName2, dbName3, dbName4, dbName5}, func(dbs []DB) (err error) {
		return handle(dbs[0], dbs[1], dbs[2], dbs[3], dbs[4])
	})
}
