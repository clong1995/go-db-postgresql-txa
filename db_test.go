package db

import (
	"log"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestDB_Batch(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			name: "test batch",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			//连接数据源
			var account, access DBName
			DataSource(&account, &access)
			//关闭数据源
			defer Close()

			//启动事物
			if err := Tx(account, func(db DB) (err error) {
				if err = db.Batch(
					"INSERT INTO demo (id,name) VALUES($1,$2)",
					[][]any{
						{34, "hh"},
						{35, "ii"},
					},
				); err != nil {
					log.Println(err)
					return
				}
				return
			}); (err != nil) != tt.wantErr {
				t.Errorf("Batch() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDB_Copy(t *testing.T) {
	tests := []struct {
		name             string
		wantRowsAffected int64
		wantErr          bool
	}{
		{
			name: "test copy",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			//连接数据源
			var account, access DBName
			DataSource(&account, &access)
			//关闭数据源
			defer Close()

			//启动事物
			if err := Tx(account, func(db DB) (err error) {
				if _, err = db.Copy(
					"demo",
					[]string{"id", "name"},
					[][]any{
						{32, "ff"},
						{33, "gg"},
					},
				); err != nil {
					log.Println(err)
					return
				}
				return
			}); (err != nil) != tt.wantErr {
				t.Errorf("Copy() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestDB_Exec(t *testing.T) {
	tests := []struct {
		name       string
		wantResult pgconn.CommandTag
		wantErr    bool
	}{
		{
			name: "test exec",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			//连接数据源
			var account, access DBName
			DataSource(&account, &access)
			//关闭数据源
			defer Close()

			//测试
			db := Conn(account)

			if _, err := db.Exec(`INSERT INTO demo (id,name) VALUES($1,$2)`, 31, "ee"); (err != nil) != tt.wantErr {
				t.Errorf("Exec() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestDB_ExecTx(t *testing.T) {
	tests := []struct {
		name       string
		wantResult pgconn.CommandTag
		wantErr    bool
	}{
		{
			name: "test exec tx",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			//连接数据源
			var account, access DBName
			DataSource(&account, &access)
			//关闭数据源
			defer Close()

			if err := Tx(account, func(db DB) (err error) {
				//操作1
				if _, err = db.Exec(`INSERT INTO demo (id,name) VALUES($1,$2)`, 31, "ee"); (err != nil) != tt.wantErr {
					log.Println(err)
					return
				}

				//操作2
				if _, err = db.Exec(`INSERT INTO demo (id,name) VALUES($1,$2)`, 30, "ff"); (err != nil) != tt.wantErr {
					log.Println(err)
					return
				}

				return
			}); (err != nil) != tt.wantErr {
				t.Errorf("Exec() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestDB_ExecTxa(t *testing.T) {
	tests := []struct {
		name       string
		wantResult pgconn.CommandTag
		wantErr    bool
	}{
		{
			name: "test exec txa",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			//连接数据源
			var account, access DBName
			DataSource(&account, &access)
			//关闭数据源
			defer Close()

			if err := Tx2(account, access, func(accountDB, accessDB DB) (err error) {
				//操作1
				if _, err = accountDB.Exec(`INSERT INTO demo (id,name) VALUES($1,$2)`, 26, "z"); (err != nil) != tt.wantErr {
					log.Println(err)
					return
				}

				//操作2
				if _, err = accessDB.Exec(`INSERT INTO demo (id,name) VALUES($1,$2)`, 27, "aa"); (err != nil) != tt.wantErr {
					log.Println(err)
					return
				}

				return
			}); (err != nil) != tt.wantErr {
				t.Errorf("Exec() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestDB_Query(t *testing.T) {
	tests := []struct {
		name     string
		wantRows pgx.Rows
		wantErr  bool
	}{
		{
			name: "test query",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			//连接数据源
			var account, access DBName
			DataSource(&account, &access)
			//关闭数据源
			defer Close()

			//测试
			db := Conn(account)
			rows, err := db.Query("SELECT id,name FROM demo WHERE id < $1", 3)
			if (err != nil) != tt.wantErr {
				t.Errorf("Query() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			defer rows.Close()

			type field struct {
				Id   int64
				Name string
			}

			//转化数据
			res, err := Scan[field](rows)
			if err != nil {
				t.Errorf("Scan() error = %v", err)
				return
			}
			for _, v := range res {
				t.Logf("Query() gotRows = %#v", v)
			}
		})
	}
}

func TestDB_QueryScan(t *testing.T) {
	tests := []struct {
		name     string
		wantRows pgx.Rows
		wantErr  bool
	}{
		{
			name: "test query",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			//连接数据源
			var account, access DBName
			DataSource(&account, &access)
			//关闭数据源
			defer Close()

			//测试
			db := Conn(account)

			type field struct {
				Id   int64
				Name string
			}
			result, err := QueryScan[field](db, "SELECT id,name FROM demo WHERE id < $1", 3)
			if (err != nil) != tt.wantErr {
				t.Errorf("Query() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			for _, v := range result {
				t.Logf("Query() gotRows = %#v", v)
			}
		})
	}
}
