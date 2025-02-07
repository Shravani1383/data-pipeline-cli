package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
)

const (
	CLICKHOUSE_DSN = "tcp://localhost:9000?username=default&password=&database=warehouse"
)

func main() {
	csvFilePath := flag.String("file", "", "Path to the CSV file")
	tableName := flag.String("table", "", "Target database table")
	flag.Parse()

	if *csvFilePath == "" || *tableName == "" {
		fmt.Println("Usage: ./ingest --file=path/to/csv --table=table_name")
		os.Exit(1)
	}

	// Connect to ClickHouse
	db, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{"localhost:9000"},
		Auth: clickhouse.Auth{
			Database: "heart_stroke",
			Username: "default",
			Password: "",
		},
	})
	if err != nil {
		fmt.Println("Error connecting to ClickHouse:", err)
		os.Exit(1)
	}
	defer db.Close()

	// Read CSV to get column names
	headers, err := getCSVHeaders(*csvFilePath)
	if err != nil {
		fmt.Println("Error reading CSV file:", err)
		os.Exit(1)
	}

	// Create table if it doesn't exist
	if err := createTable(db, *tableName, headers); err != nil {
		fmt.Println("Error creating table:", err)
		os.Exit(1)
	}

	// Ingest CSV data
	if err := ingestCSV(db, *csvFilePath, *tableName); err != nil {
		fmt.Println("Error ingesting CSV:", err)
		os.Exit(1)
	}

	fmt.Println("Data successfully ingested into ClickHouse!")
}

func getCSVHeaders(filePath string) ([]string, error) {
	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Read CSV content
	reader := csv.NewReader(file)
	headers, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV headers: %w", err)
	}

	// Check if headers are empty
	if len(headers) == 0 {
		return nil, fmt.Errorf("CSV file must contain headers")
	}

	// Trim spaces from headers
	for i, h := range headers {
		headers[i] = strings.TrimSpace(h)
	}

	return headers, nil
}

func createTable(db clickhouse.Conn, tableName string, headers []string) error {
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

func ingestCSV(db clickhouse.Conn, filePath, tableName string) error {
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
		valuePlaceholders[i] = fmt.Sprintf("(%s)", placeholders(len(headers)))
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

// Generates ClickHouse placeholders (e.g., "?, ?, ?" for column count)
func placeholders(n int) string {
	parts := make([]string, n)
	for i := range parts {
		parts[i] = "?"
	}
	return strings.Join(parts, ", ")
}
