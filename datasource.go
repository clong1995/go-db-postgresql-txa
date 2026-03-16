package db

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
)

var databasePool map[DBName]*pgxpool.Pool
var prefix = "postgresql-txa"

func MultiDatasource() ([]DBName, error) {
	//dataSource := strings.Split(configDatasource, ",")
	dbNames := make([]DBName, len(configDatasource))
	databasePool = make(map[DBName]*pgxpool.Pool)

	for i, v := range configDatasource {
		conf, err := pgxpool.ParseConfig(v)
		if err != nil {
			return nil, errors.Wrap(err, "")
		}
		conf.MaxConns = configMaxConns
		conf.MinConns = 1
		conf.MaxConnIdleTime = time.Minute * 30

		pool, err := pgxpool.NewWithConfig(context.Background(), conf)
		if err != nil {
			return nil, errors.Wrap(err, "")
		}

		if err = pool.Ping(context.Background()); err != nil {
			return nil, errors.Wrap(err, "")
		}
		dbName := DBName(conf.ConnConfig.Database)
		databasePool[dbName] = pool

		dbNames[i] = dbName

		//pcolor.PrintSucc(prefix, "conn %v", dbName)
	}
	return dbNames, nil
}

func Datasource() (DBName, error) {
	var dbName DBName
	dbnames, err := MultiDatasource()
	if err != nil {
		return dbName, errors.Wrap(err, "")
	}
	if len(dbnames) != 1 {
		return dbName, errors.New("data source should contain 1 db names")
	}
	return dbnames[0], nil
}

func Datasource2() (DBName, DBName, error) {
	var dbName DBName
	dbnames, err := MultiDatasource()
	if err != nil {
		return dbName, dbName, errors.Wrap(err, "")
	}
	if len(dbnames) != 2 {
		return dbName, dbName, errors.New("data source should contain 2 db names")
	}
	return dbnames[0], dbnames[1], nil
}

func Datasource3() (DBName, DBName, DBName, error) {
	var dbName DBName
	dbnames, err := MultiDatasource()
	if err != nil {
		return dbName, dbName, dbName, errors.Wrap(err, "")
	}
	if len(dbnames) != 3 {
		return dbName, dbName, dbName, errors.New("data source should contain 3 db names")
	}
	return dbnames[0], dbnames[1], dbnames[2], nil
}
func Datasource4() (DBName, DBName, DBName, DBName, error) {
	var dbName DBName
	dbnames, err := MultiDatasource()
	if err != nil {
		return dbName, dbName, dbName, dbName, errors.Wrap(err, "")
	}
	if len(dbnames) != 4 {
		return dbName, dbName, dbName, dbName, errors.New("data source should contain 4 db names")
	}
	return dbnames[0], dbnames[1], dbnames[2], dbnames[3], nil
}
func Datasource5() (DBName, DBName, DBName, DBName, DBName, error) {
	var dbName DBName
	dbnames, err := MultiDatasource()
	if err != nil {
		return dbName, dbName, dbName, dbName, dbName, errors.Wrap(err, "")
	}
	if len(dbnames) != 5 {
		return dbName, dbName, dbName, dbName, dbName, errors.New("data source should contain 5 db names")
	}
	return dbnames[0], dbnames[1], dbnames[2], dbnames[3], dbnames[4], nil
}

func Close() {
	for _, v := range databasePool {
		v.Close()
		//pcolor.PrintSucc(prefix, "%v closed", k)
	}
}
