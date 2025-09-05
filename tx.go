package db

import (
	"errors"
	"fmt"
	"log"

	pcolor "github.com/clong1995/go-ansi-color"
	"github.com/jackc/pgx/v5"
	"golang.org/x/net/context"
)

type Xa map[DBName]pgx.Tx

func Tx(dbs []DBName, handle func(Xa) (err error)) (err error) {
	xa := make(map[DBName]pgx.Tx)
	for i, v := range dbs {
		p := dataPool[v]
		if p == nil {
			err = errors.New(fmt.Sprintf("db[%s] is not exist", v))
			log.Println(pcolor.Error(err))
			return
		}
		if xa[dbs[i]], err = p.Begin(context.Background()); err != nil {
			log.Println(pcolor.Error(err))
			return
		}
	}

	defer func() {
		if err != nil {
			for _, tx := range xa {
				if rollbackErr := tx.Rollback(context.Background()); rollbackErr != nil {
					log.Println(pcolor.Error(rollbackErr))
				}
			}
		} else {
			for _, tx := range xa {
				if commitErr := tx.Commit(context.Background()); commitErr != nil {
					log.Println(pcolor.Error(commitErr))
				}
			}
		}
	}()

	if err = handle(xa); err != nil {
		log.Println(pcolor.Error(err))
		return
	}

	return
}
