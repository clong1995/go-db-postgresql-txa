package db

import (
	"context"
	"log"
	"strconv"
	"strings"

	pcolor "github.com/clong1995/go-ansi-color"
	"github.com/clong1995/go-config"
	"github.com/jackc/pgx/v5/pgxpool"
)

var connPools map[DBName]*pgxpool.Pool

func Conn(dbNames ...*DBName) {
	num, err := strconv.ParseInt(config.Value("MAXCONNS"), 10, 32)
	if err != nil {
		log.Fatalln(pcolor.Error(err))
	}
	maxConn := int32(num)

	connPools = make(map[DBName]*pgxpool.Pool)
	ds := config.Value("DATASOURCE")
	for i, v := range strings.Split(ds, ",") {
		var conf *pgxpool.Config
		if conf, err = pgxpool.ParseConfig(v); err != nil {
			log.Fatalln(pcolor.Error(err))
		}
		conf.MaxConns = maxConn

		var pool *pgxpool.Pool
		if pool, err = pgxpool.NewWithConfig(context.Background(), conf); err != nil {
			log.Fatalln(pcolor.Error(err))
		}

		if err = pool.Ping(context.Background()); err != nil {
			log.Fatalln(pcolor.Error(err))
		}
		database := DBName(conf.ConnConfig.Database)
		connPools[database] = pool

		*dbNames[i] = database

		log.Println(pcolor.Succ("[PostgreSQL] conn %v", database))
	}
}

func Close() {
	for k, v := range connPools {
		v.Close()
		log.Println(pcolor.Succ("[PostgreSQL] %v closed", k))
	}
}
