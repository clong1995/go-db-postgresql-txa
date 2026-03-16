package db

import (
	pcolor "github.com/clong1995/go-ansi-color"
	"github.com/clong1995/go-config"
)

var configMaxConns int32
var configDatasource []string

func init() {
	var exists bool

	//DataSource
	if configDatasource, exists = config.Value[[]string]("DATASOURCE"); !exists || len(configDatasource) == 0 {
		pcolor.PrintFatal(prefix, "")
		return
	}

	//MaxConns
	configMaxConns_, exists := config.Value[int64]("MAX CONNS")
	if !exists || configMaxConns_ == 0 {
		configMaxConns = 10
	} else {
		configMaxConns = int32(configMaxConns_)
	}
}
