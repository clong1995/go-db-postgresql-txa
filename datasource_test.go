package db

import (
	"log"
	"testing"
)

func TestDataSource(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "test data source",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var account, access DBName
			DataSource(&account, &access)
			log.Println(account)
			log.Println(access)
			Close()
		})
	}
}
