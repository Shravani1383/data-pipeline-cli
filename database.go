package main

import (
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// ConnectDB establishes a connection to ClickHouse
func ConnectDB() (clickhouse.Conn, error) {
	db, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"},
		Auth: clickhouse.Auth{
			Database: "heart_stroke",
			Username: "default",
			Password: "",
		},
	})
	if err != nil {
		return nil, fmt.Errorf("error connecting to ClickHouse: %w", err)
	}
	return db, nil
}
