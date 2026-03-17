package db

import (
	pcolor "github.com/clong1995/go-ansi-color"
	"github.com/clong1995/go-config"
)

// 全局变量，用于存储从配置中读取的数据库设置
var configMaxConns int32      // 最大连接数
var configDatasource []string // 数据源连接字符串列表

// init 函数在包初始化时自动执行，用于加载数据库配置。
func init() {
	var exists bool

	// 从配置中读取 "DATASOURCE"
	// 这是一个必需的配置项，如果不存在或为空，程序将打印致命错误并退出。
	if configDatasource, exists = config.Value[[]string]("DATASOURCE"); !exists || len(configDatasource) == 0 {
		pcolor.PrintFatal(prefix, "未找到或为空的 'DATASOURCE' 配置")
		return
	}

	// 从配置中读取 "MAX CONNS"
	// 这是一个可选配置。如果不存在或为0，则使用默认值 10。
	configMaxConns_, exists := config.Value[int64]("MAX CONNS")
	if !exists || configMaxConns_ == 0 {
		configMaxConns = 10 // 默认最大连接数
	} else {
		configMaxConns = int32(configMaxConns_)
	}
}
