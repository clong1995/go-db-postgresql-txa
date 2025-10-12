package db

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/clong1995/go-ansi-color"
	"github.com/clong1995/go-config"
	"github.com/jackc/pgx/v5/pgxpool"
)

var dataPool map[DBName]*pgxpool.Pool

func DataSource(dbNames ...*DBName) {
	num, err := strconv.ParseInt(config.Value("MAXCONNS"), 10, 32)
	if err != nil {
		pcolor.PrintFatal(err.Error())
	}

	dataSource := config.Value("DATASOURCE")
	ds := strings.Split(dataSource, ",")

	if len(dbNames) != len(ds) {
		err = errors.New("db names != data source")
		pcolor.PrintFatal(err.Error())
	}

	maxConn := int32(num)

	dataPool = make(map[DBName]*pgxpool.Pool)
	for i, v := range ds {
		var conf *pgxpool.Config
		if conf, err = pgxpool.ParseConfig(v); err != nil {
			pcolor.PrintFatal(err.Error())
		}
		conf.MaxConns = maxConn

		var pool *pgxpool.Pool
		if pool, err = pgxpool.NewWithConfig(context.Background(), conf); err != nil {
			pcolor.PrintFatal(err.Error())
		}

		if err = pool.Ping(context.Background()); err != nil {
			pcolor.PrintFatal(err.Error())
		}
		database := DBName(conf.ConnConfig.Database)
		dataPool[database] = pool

		*dbNames[i] = database

		pcolor.PrintSucc("conn %v", database)
	}
}

func Close() {
	for k, v := range dataPool {
		v.Close()
		pcolor.PrintSucc("%v closed", k)
	}
}
