package db

import (
	pcolor "github.com/clong1995/go-ansi-color"
	conf "github.com/clong1995/go-config"
)

// 全局变量，用于存储从配置中读取的数据库设置
var maxConns int32      // 最大连接数
var datasource []string // 数据源连接字符串列表

func config() {
	var exists bool

	// 从配置中读取 "DATASOURCE"
	// 这是一个必需的配置项，如果不存在或为空，程序将打印致命错误并退出。
	if datasource, exists = conf.Value[[]string]("DATASOURCE"); !exists || len(datasource) == 0 {
		pcolor.PrintFatal(prefix, "未找到或为空的 'DATASOURCE' 配置")
		return
	}

	// 从配置中读取 "MAX CONNS"
	// 这是一个可选配置。如果不存在或为0，则使用默认值 10。
	configMaxConns, exists := conf.Value[int64]("MAX CONNS")
	if !exists || configMaxConns == 0 {
		maxConns = 10 // 默认最大连接数
	} else {
		maxConns = int32(configMaxConns)
	}
}
