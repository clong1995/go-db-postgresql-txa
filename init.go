package db

const prefix = "postgresql-txa"

func init() {
	//加载配置
	config()
	//启动数据库
	start()
}
