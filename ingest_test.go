package main

import (
	"context"
	"os"
	"testing"
)

// TestCreateTableValid ensures a table is successfully created.
func TestCreateTableValid(t *testing.T) {
	db, _ := ConnectDB()
	defer db.Close()

	tableName := "test_table_valid"
	headers := []string{"name", "age", "city"}

	err := CreateTable(db, tableName, headers)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	query := "EXISTS TABLE " + tableName
	var exists int
	err = db.QueryRow(context.Background(), query).Scan(&exists)
	if err != nil || exists == 0 {
		t.Fatalf("Table creation verification failed")
	}
}

// TestCreateTableEmpty ensures error handling for an empty header list.
func TestCreateTableEmpty(t *testing.T) {
	db, _ := ConnectDB()
	defer db.Close()

	err := CreateTable(db, "test_empty_table", []string{})
	if err == nil {
		t.Fatal("Expected error when creating a table with no columns, but got none")
	}
}

// TestIngestCSVValid ensures correct data ingestion.
func TestIngestCSVValid(t *testing.T) {
	db, _ := ConnectDB()
	defer db.Close()

	tableName := "test_ingest"
	testCSV := "name,age,city\nJohn,30,New York\nDoe,25,San Francisco"

	tmpFile, _ := os.CreateTemp("", "test.csv")
	defer os.Remove(tmpFile.Name())

	tmpFile.Write([]byte(testCSV))
	tmpFile.Close()

	headers := []string{"name", "age", "city"}
	_ = CreateTable(db, tableName, headers)

	err := IngestCSV(db, tmpFile.Name(), tableName)
	if err != nil {
		t.Fatalf("IngestCSV failed: %v", err)
	}

	// Verify data ingestion
	query := "SELECT count(*) FROM " + tableName
	var count int
	err = db.QueryRow(context.Background(), query).Scan(&count)
	if err != nil || count == 0 {
		t.Fatalf("Data ingestion verification failed")
	}
}

// TestIngestCSVEmptyFile ensures an empty file fails ingestion.
func TestIngestCSVEmptyFile(t *testing.T) {
	db, _ := ConnectDB()
	defer db.Close()

	tableName := "test_empty_ingest"
	tmpFile, _ := os.CreateTemp("", "empty.csv")
	defer os.Remove(tmpFile.Name())

	headers := []string{"name", "age", "city"}
	_ = CreateTable(db, tableName, headers)

	err := IngestCSV(db, tmpFile.Name(), tableName)
	if err == nil {
		t.Fatal("Expected error for empty CSV file but got none")
	}
}
