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
			var err error
			defer func() {
				//捕获堆栈
				log.Printf("%+v\n", err)

				if (err != nil) != tt.wantErr {
					t.Errorf("Batch() error = %v, wantErr %v", err, tt.wantErr)
				}
			}()

			//连接数据源
			demo_01, _, err := Datasource2()
			if err != nil {
				return
			}
			//关闭数据源
			defer Close()

			//启动事物
			tx, commit, err := Tx(demo_01)
			if err != nil {
				return
			}
			defer func() {
				//实际开发中，需要上层函数是命名返回值，用于修改最终返回值。
				err = commit(err)
			}()

			if err = tx.Batch(
				"INSERT INTO foo (id,name) VALUES($1,$2)",
				[][]any{
					{1, "a"},
					{2, "b"},
				},
			); err != nil {
				return
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
			var err error
			defer func() {
				//捕获堆栈
				log.Printf("%+v\n", err)

				if (err != nil) != tt.wantErr {
					t.Errorf("Copy() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
			}()
			//连接数据源
			demo_01, _, err := Datasource2()
			if err != nil {
				return
			}
			//关闭数据源
			defer Close()

			//启动事物
			tx, commit, err := Tx(demo_01)
			if err != nil {
				return
			}
			defer func() {
				//实际开发中，需要上层函数是命名返回值，用于修改最终返回值。
				err = commit(err)
			}()

			if _, err = tx.Copy(
				"foo",
				[]string{"id", "name"},
				[][]any{
					{3, "c"},
					{4, "d"},
				},
			); err != nil {
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
			var err error
			defer func() {
				//捕获堆栈
				log.Printf("%+v\n", err)

				if (err != nil) != tt.wantErr {
					t.Errorf("Exec() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
			}()

			//连接数据源
			demo_01, _, err := Datasource2()
			if err != nil {
				return
			}
			//关闭数据源
			defer Close()

			//测试
			conn := NewConn(demo_01)

			if _, err = conn.Exec(`INSERT INTO foo (id,name) VALUES($1,$2)`, 5, "e"); err != nil {
				return
			}
		})
	}
}

func TestDB_ExecTx(t *testing.T) {
	//log.SetFlags(log.Llongfile)
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
			var err error
			defer func() {
				//捕获堆栈
				log.Printf("%+v\n", err)

				if (err != nil) != tt.wantErr {
					t.Errorf("Exec() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
			}()

			//连接数据源
			demo_01, demo_02, err := Datasource2()
			if err != nil {
				return
			}
			//关闭数据源
			defer Close()

			//启动事物
			demo01Tx, demo02Tx, commit, err := Tx2(demo_01, demo_02)
			if err != nil {
				return
			}
			defer func() {
				//实际开发中，需要上层函数是命名返回值，用于修改最终返回值。
				err = commit(err)
			}()

			//操作1
			if _, err = demo01Tx.Exec(`INSERT foo demo (id,name) VALUES($1,$2)`, 6, "f"); (err != nil) != tt.wantErr {
				return
			}

			//操作2
			if _, err = demo02Tx.Exec(`INSERT INTO foo (id,name) VALUES($1,$2)`, 6, "f"); (err != nil) != tt.wantErr {
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
			var err error
			defer func() {
				//捕获堆栈
				log.Printf("%+v\n", err)

				if (err != nil) != tt.wantErr {
					t.Errorf("Query() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
			}()
			//连接数据源
			demo01, _, err := Datasource2()
			if err != nil {
				return
			}
			//关闭数据源
			defer Close()

			//测试
			conn := NewConn(demo01)
			rows, err := conn.Query("SELECT id,name FROM foo WHERE id < $1", 3)
			if err != nil {
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
			name: "test query_scan",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var err error
			defer func() {
				//捕获堆栈
				log.Printf("%+v\n", err)

				if (err != nil) != tt.wantErr {
					t.Errorf("QueryScan() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
			}()

			//连接数据源
			demo01, _, err := Datasource2()
			//关闭数据源
			defer Close()

			//测试
			conn := NewConn(demo01)

			type field struct {
				Id   int64
				Name string
			}
			result, err := ConnQueryScan[field](conn, "SELECT id,name FROM foo WHERE id < $1", 3)
			if err != nil {
				return
			}

			for _, v := range result {
				t.Logf("Query() gotRows = %#v", v)
			}
		})
	}
}
