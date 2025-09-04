package db

import (
	"log"
	"testing"
)

func TestConn(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "test conn",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var account, access DBName
			Conn(&account, &access)
			log.Println(account)
			log.Println(access)
			Close()
		})
	}
}
