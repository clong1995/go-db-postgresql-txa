package db

import (
	"log"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestHandle_Batch(t *testing.T) {
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
			if err := Tx([]DBName{account}, func(xa Xa) (err error) {
				//连接数据库
				accountDB := Conn(account, xa)
				if err = accountDB.Batch(
					"INSERT INTO demo (id,name) VALUES($1,$2)",
					[][]any{
						{21, "u"},
						{22, "v"},
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

func TestHandle_Copy(t *testing.T) {
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
			if err := Tx([]DBName{account}, func(xa Xa) (err error) {
				//连接数据库
				accountDB := Conn(account, xa)
				if _, err = accountDB.Copy(
					"demo",
					[]string{"id", "name"},
					[][]any{
						{19, "s"},
						{20, "t"},
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

func TestHandle_Exec(t *testing.T) {
	tests := []struct {
		name       string
		wantResult pgconn.CommandTag
		wantErr    bool
	}{
		{
			name: "test Exec",
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
			accountDB := Conn(account)

			if _, err := accountDB.Exec(`INSERT INTO demo (id,name) VALUES($1,$2)`, 18, "r"); (err != nil) != tt.wantErr {
				t.Errorf("Exec() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestHandle_Query(t *testing.T) {
	tests := []struct {
		name     string
		wantRows pgx.Rows
		wantErr  bool
	}{
		{
			name: "test Query",
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
			accountDB := Conn(account)
			rows, err := accountDB.Query("SELECT id,name FROM demo WHERE id < $1", 3)
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
