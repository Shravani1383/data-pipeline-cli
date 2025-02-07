package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// CreateTable creates a table in ClickHouse if it doesn't exist
func CreateTable(db clickhouse.Conn, tableName string, headers []string) error {
	columnDefinitions := []string{}
	for _, col := range headers {
		columnDefinitions = append(columnDefinitions, fmt.Sprintf("`%s` String", col)) // Default to String
	}
	createTableQuery := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = MergeTree() ORDER BY tuple();",
		tableName, strings.Join(columnDefinitions, ", "),
	)

	ctx := context.Background()
	if err := db.Exec(ctx, createTableQuery); err != nil {
		return fmt.Errorf("error creating table: %v", err)
	}
	fmt.Println("Table created or already exists:", tableName)
	return nil
}

// IngestCSV reads and inserts data from CSV into ClickHouse
func IngestCSV(db clickhouse.Conn, filePath, tableName string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return err
	}

	if len(records) < 2 {
		return fmt.Errorf("CSV file must have at least a header and one row of data")
	}

	headers := records[0]
	columnNames := strings.Join(headers, ", ")

	// Construct ClickHouse insert query
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES ", tableName, columnNames)
	valuePlaceholders := make([]string, len(records)-1)

	for i := range records[1:] {
		valuePlaceholders[i] = fmt.Sprintf("(%s)", Placeholders(len(headers)))
	}
	query += strings.Join(valuePlaceholders, ", ")

	// Flatten values for the query
	values := []interface{}{}
	for _, row := range records[1:] {
		for _, v := range row {
			values = append(values, v)
		}
	}

	// Execute batch insert
	ctx := context.Background()
	if err := db.Exec(ctx, query, values...); err != nil {
		return fmt.Errorf("ClickHouse insert error: %v", err)
	}

	return nil
}

// Placeholders generates ClickHouse placeholders (e.g., "?, ?, ?" for column count)
func Placeholders(n int) string {
	parts := make([]string, n)
	for i := range parts {
		parts[i] = "?"
	}
	return strings.Join(parts, ", ")
}
