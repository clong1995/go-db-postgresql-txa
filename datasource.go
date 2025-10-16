package db

import (
	"context"
	"strconv"
	"strings"

	"github.com/clong1995/go-ansi-color"
	"github.com/clong1995/go-config"
	"github.com/jackc/pgx/v5/pgxpool"
)

var databasePool map[DBName]*pgxpool.Pool
var prefix = "postgresql-txa"

func MultiDataSource() (dbNames []DBName) {
	configMaxConns := config.Value("MAXCONNS")
	var maxConn int32
	if configMaxConns == "" {
		maxConn = 10
	} else {
		i, err := strconv.ParseInt(configMaxConns, 10, 32)
		if err != nil {
			pcolor.PrintFatal(prefix, err.Error())
			return
		}
		maxConn = int32(i)
	}

	configDataSource := config.Value("DATASOURCE")
	dataSource := strings.Split(configDataSource, ",")

	dbNames = make([]DBName, len(dataSource))
	databasePool = make(map[DBName]*pgxpool.Pool)

	for i, v := range dataSource {
		conf, err := pgxpool.ParseConfig(v)
		if err != nil {
			pcolor.PrintFatal(prefix, err.Error())
			return
		}
		conf.MaxConns = maxConn

		pool, err := pgxpool.NewWithConfig(context.Background(), conf)
		if err != nil {
			pcolor.PrintFatal(prefix, err.Error())
			return
		}

		if err = pool.Ping(context.Background()); err != nil {
			pcolor.PrintFatal(prefix, err.Error())
			return
		}
		dbName := DBName(conf.ConnConfig.Database)
		databasePool[dbName] = pool

		dbNames[i] = dbName

		pcolor.PrintSucc(prefix, "conn %v", dbName)
	}
	return
}

func DataSource() (dbName DBName) {
	dbnames := MultiDataSource()
	if len(dbnames) != 1 {
		pcolor.PrintFatal(prefix, "data source should contain 1 db names")
		return
	}
	return dbnames[0]
}

func DataSource2() (dbName1, dbName2 DBName) {
	dbnames := MultiDataSource()
	if len(dbnames) != 2 {
		pcolor.PrintFatal(prefix, "data source should contain 2 db names")
		return
	}
	return dbnames[0], dbnames[1]
}

func DataSource3() (dbName1, dbName2, dbName3 DBName) {
	dbnames := MultiDataSource()
	if len(dbnames) != 3 {
		pcolor.PrintFatal(prefix, "data source should contain 3 db names")
		return
	}
	return dbnames[0], dbnames[1], dbnames[2]
}
func DataSource4() (dbName1, dbName2, dbName3, dbName4 DBName) {
	dbnames := MultiDataSource()
	if len(dbnames) != 4 {
		pcolor.PrintFatal(prefix, "data source should contain 4 db names")
		return
	}
	return dbnames[0], dbnames[1], dbnames[2], dbnames[3]
}
func DataSource5() (dbName1, dbName2, dbName3, dbName4, dbName5 DBName) {
	dbnames := MultiDataSource()
	if len(dbnames) != 5 {
		pcolor.PrintFatal(prefix, "data source should contain 5 db names")
		return
	}
	return dbnames[0], dbnames[1], dbnames[2], dbnames[3], dbnames[4]
}

func Close() {
	for k, v := range databasePool {
		v.Close()
		pcolor.PrintSucc(prefix, "%v closed", k)
	}
}
