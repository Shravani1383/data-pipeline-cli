package main

import (
	"os"
	"testing"
)

// TestGetCSVHeadersValid ensures that CSV headers are read correctly.
func TestGetCSVHeadersValid(t *testing.T) {
	testCSV := "name,age,city\nJohn,30,New York\nDoe,25,San Francisco"
	tmpFile, _ := os.CreateTemp("", "test.csv")
	defer os.Remove(tmpFile.Name())

	tmpFile.Write([]byte(testCSV))
	tmpFile.Close()

	headers, err := GetCSVHeaders(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to get CSV headers: %v", err)
	}

	expectedHeaders := []string{"name", "age", "city"}
	for i, h := range expectedHeaders {
		if headers[i] != h {
			t.Errorf("Expected header %s but got %s", h, headers[i])
		}
	}
}

// TestGetCSVHeadersEmpty ensures error handling for an empty file.
func TestGetCSVHeadersEmpty(t *testing.T) {
	tmpFile, _ := os.CreateTemp("", "empty.csv")
	defer os.Remove(tmpFile.Name())

	_, err := GetCSVHeaders(tmpFile.Name())
	if err == nil {
		t.Fatal("Expected an error for an empty CSV file but got none")
	}
}

// TestGetCSVHeadersMissing ensures error handling when CSV lacks headers.
func TestGetCSVHeadersMissing(t *testing.T) {
	testCSV := "\nJohn,30,New York"
	tmpFile, _ := os.CreateTemp("", "no_headers.csv")
	defer os.Remove(tmpFile.Name())

	tmpFile.Write([]byte(testCSV))
	tmpFile.Close()

	_, err := GetCSVHeaders(tmpFile.Name())
	if err == nil {
		t.Fatal("Expected an error for CSV without headers but got none")
	}
}
