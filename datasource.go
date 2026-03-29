package db

import (
	"context"
	"time"

	pcolor "github.com/clong1995/go-ansi-color"
	"github.com/jackc/pgx/v5/pgxpool"
)

// databasePool 是一个全局映射，用于存储数据库名称到其对应连接池的映射。
var databasePool map[string]*pgxpool.Pool
var dbName []string

// multiDatasource 根据全局配置 `configDatasource` 初始化一个或多个数据库连接池。
// 它会解析每个数据源字符串，创建并配置连接池，然后测试连接。
// 成功后，它会将连接池存储在全局的 `databasePool` 中，并返回所有数据库的名称。
func start() {
	dbName = make([]string, len(datasource))
	databasePool = make(map[string]*pgxpool.Pool)

	for i, v := range datasource {
		// 解析连接字符串
		conf, err := pgxpool.ParseConfig(v)
		if err != nil {
			Close() // 清理已创建的连接池
			pcolor.PrintFatal(prefix, err.Error())
			return
		}
		// 配置连接池参数
		conf.MaxConns = maxConns
		conf.MinConns = 1
		conf.MaxConnIdleTime = time.Minute * 30

		// 创建新的连接池
		pool, err := pgxpool.NewWithConfig(context.Background(), conf)
		if err != nil {
			Close() // 清理已创建的连接池
			pcolor.PrintFatal(prefix, err.Error())
			return
		}

		// Ping 数据库以验证连接
		if err = pool.Ping(context.Background()); err != nil {
			pool.Close()
			Close() // 清理已创建的连接池
			pcolor.PrintFatal(prefix, err.Error())
			return
		}
		name := conf.ConnConfig.Database
		databasePool[name] = pool

		dbName[i] = name
	}
	return
}

/*
// Datasource 是 MultiDatasource 的一个简化版本，用于初始化单个数据库连接。
// 如果配置的数据源数量不是 1，它会返回一个错误。
func Datasource() (DBName, error) {
	var dbName DBName
	dbnames, err := MultiDatasource()
	if err != nil {
		return dbName, err
	}
	if len(dbnames) != 1 {
		return dbName, errors.New("数据源应包含一个数据库名称")
	}
	return dbnames[0], nil
}

// Datasource2 是 MultiDatasource 的一个简化版本，用于初始化两个数据库连接。
func Datasource2() (DBName, DBName, error) {
	var dbName DBName
	dbnames, err := MultiDatasource()
	if err != nil {
		return dbName, dbName, err
	}
	if len(dbnames) != 2 {
		return dbName, dbName, errors.New("数据源应包含两个数据库名称")
	}
	return dbnames[0], dbnames[1], nil
}

// Datasource3 是 MultiDatasource 的一个简化版本，用于初始化三个数据库连接。
func Datasource3() (DBName, DBName, DBName, error) {
	var dbName DBName
	dbnames, err := MultiDatasource()
	if err != nil {
		return dbName, dbName, dbName, err
	}
	if len(dbnames) != 3 {
		return dbName, dbName, dbName, errors.New("数据源应包含三个数据库名称")
	}
	return dbnames[0], dbnames[1], dbnames[2], nil
}

// Datasource4 是 MultiDatasource 的一个简化版本，用于初始化四个数据库连接。
func Datasource4() (DBName, DBName, DBName, DBName, error) {
	var dbName DBName
	dbnames, err := MultiDatasource()
	if err != nil {
		return dbName, dbName, dbName, dbName, err
	}
	if len(dbnames) != 4 {
		return dbName, dbName, dbName, dbName, errors.New("数据源应包含四个数据库名称")
	}
	return dbnames[0], dbnames[1], dbnames[2], dbnames[3], nil
}

// Datasource5 是 MultiDatasource 的一个简化版本，用于初始化五个数据库连接。
func Datasource5() (DBName, DBName, DBName, DBName, DBName, error) {
	var dbName DBName
	dbnames, err := MultiDatasource()
	if err != nil {
		return dbName, dbName, dbName, dbName, dbName, err
	}
	if len(dbnames) != 5 {
		return dbName, dbName, dbName, dbName, dbName, errors.New("数据源应包含五个数据库名称")
	}
	return dbnames[0], dbnames[1], dbnames[2], dbnames[3], dbnames[4], nil
}
*/

// Close 关闭所有已打开的数据库连接池。
// 在应用程序退出时调用此函数以释放资源。
func Close() {
	for _, v := range databasePool {
		v.Close()
	}
	pcolor.PrintSucc(prefix, "datasourc closed")
}
