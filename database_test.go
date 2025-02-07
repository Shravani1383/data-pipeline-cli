package main

import (
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// TestConnectDBSuccess checks if the database connection is successfully established.
func TestConnectDBSuccess(t *testing.T) {
	db, err := ConnectDB()
	if err != nil {
		t.Fatalf("Failed to connect to ClickHouse: %v", err)
	}
	defer db.Close()

	if db == nil {
		t.Fatal("Database connection is nil")
	}
}

// TestConnectDBFailure simulates a failed database connection by providing an incorrect DSN.
func TestConnectDBFailure(t *testing.T) {
	// Simulating wrong DSN
	_, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"invalid_host:9000"},
		Auth: clickhouse.Auth{
			Database: "wrong_db",
			Username: "wrong_user",
			Password: "wrong_pass",
		},
	})
	if err == nil {
		t.Fatal("Expected database connection to fail, but it succeeded")
	}
}
