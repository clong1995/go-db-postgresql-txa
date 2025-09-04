package db

import (
	"github.com/jackc/pgx/v5"
)

type DB struct {
	name DBName
	Handle
}

func NewDB(name DBName, xa ...Xa) DB {
	var tx pgx.Tx
	if len(xa) > 0 {
		tx = xa[0][name]
	}

	return DB{
		name: name,
		Handle: Handle{
			name: name,
			tx:   tx,
			pool: connPools[name], //access的pool
		},
	}
}
