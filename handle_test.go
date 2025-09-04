package db

import (
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestHandle_Batch(t *testing.T) {
	type fields struct {
		name DBName
		tx   pgx.Tx
		pool *pgxpool.Pool
	}
	type args struct {
		query string
		data  [][]any
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := Exec{
				name: tt.fields.name,
				tx:   tt.fields.tx,
				pool: tt.fields.pool,
			}
			if err := p.Batch(tt.args.query, tt.args.data); (err != nil) != tt.wantErr {
				t.Errorf("Batch() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHandle_Copy(t *testing.T) {
	type fields struct {
		name DBName
		tx   pgx.Tx
		pool *pgxpool.Pool
	}
	type args struct {
		tableName   string
		columnNames []string
		data        [][]any
	}
	tests := []struct {
		name             string
		fields           fields
		args             args
		wantRowsAffected int64
		wantErr          bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := Exec{
				name: tt.fields.name,
				tx:   tt.fields.tx,
				pool: tt.fields.pool,
			}
			gotRowsAffected, err := p.Copy(tt.args.tableName, tt.args.columnNames, tt.args.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("Copy() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotRowsAffected != tt.wantRowsAffected {
				t.Errorf("Copy() gotRowsAffected = %v, want %v", gotRowsAffected, tt.wantRowsAffected)
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
			//连接数据库
			var account, access DBName
			Conn(&account, &access)

			//测试
			accountDB := NewDB(account)
			_, err := accountDB.Exec(`INSERT INTO demo (id,name) VALUES($1,$2)`, 18, "r")
			if (err != nil) != tt.wantErr {
				t.Errorf("Exec() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotResult, tt.wantResult) {
				t.Errorf("Exec() gotResult = %v, want %v", gotResult, tt.wantResult)
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
			//连接数据库
			var account, access DBName
			Conn(&account, &access)

			//测试
			accountDB := NewDB(account)
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

			//关闭数据库
			Close()
		})
	}
}
