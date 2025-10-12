package db

import (
	"errors"
	"fmt"
	"log"

	"golang.org/x/net/context"
)

// MultiTx 但数据库事物和跨数据库事物
func MultiTx(dbNames ...DBName) (connects []*Conn, commit func(err error), err error) {
	connects = make([]*Conn, len(dbNames))
	commit = func(err error) {
		for _, d := range connects {
			if d == nil || d.tx == nil {
				continue
			}
			if err != nil {
				if rollbackErr := d.tx.Rollback(context.Background()); rollbackErr != nil {
					log.Println(rollbackErr)
				}
			} else {
				if commitErr := d.tx.Commit(context.Background()); commitErr != nil {
					log.Println(commitErr)
				}
			}
		}
	}
	for i, v := range dbNames {
		p := dataPool[v]
		if p == nil {
			err = errors.New(fmt.Sprintf("db[%s] is not exist", v))
			commit(err)
			log.Println(err)
			return
		}
		connects[i] = &Conn{}
		if connects[i].tx, err = p.Begin(context.Background()); err != nil {
			commit(err)
			log.Println(err)
			return
		}
	}

	return
}

// Tx 对单个数据库使用 MultiTx 的简化
func Tx(dbName DBName) (conn *Conn, commit func(err error), err error) {
	connects, commit, err := MultiTx(dbName)
	if err != nil {
		return
	}
	conn = connects[0]
	return
}

// Tx2 对2个数据库使用 MultiTx 的简化
func Tx2(dbName1, dbName2 DBName) (conn1, conn2 *Conn, commit func(err error), err error) {
	connects, commit, err := MultiTx(dbName1, dbName2)
	if err != nil {
		return
	}
	conn1, conn2 = connects[0], connects[1]
	return
}

// Tx3 对3个数据库使用 MultiTx 的简化
func Tx3(dbName1, dbName2, dbName3 DBName) (conn1, conn2, conn3 *Conn, commit func(err error), err error) {
	connects, commit, err := MultiTx(dbName1, dbName2, dbName3)
	if err != nil {
		return
	}
	conn1, conn2, conn3 = connects[0], connects[1], connects[2]
	return
}

// Tx4 对4个数据库使用 MultiTx 的简化
func Tx4(dbName1, dbName2, dbName3, dbName4 DBName) (conn1, conn2, conn3, conn4 *Conn, commit func(err error), err error) {
	connects, commit, err := MultiTx(dbName1, dbName2, dbName3, dbName4)
	if err != nil {
		return
	}
	conn1, conn2, conn3, conn4 = connects[0], connects[1], connects[2], connects[3]
	return
}

// Tx5 对5个数据库使用 MultiTx 的简化
func Tx5(dbName1, dbName2, dbName3, dbName4, dbName5 DBName) (conn1, conn2, conn3, conn4, conn5 *Conn, commit func(err error), err error) {
	connects, commit, err := MultiTx(dbName1, dbName2, dbName3, dbName4, dbName5)
	if err != nil {
		return
	}
	conn1, conn2, conn3, conn4, conn5 = connects[0], connects[1], connects[2], connects[3], connects[4]
	return
}
