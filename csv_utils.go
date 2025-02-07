package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strings"
)

// GetCSVHeaders reads the CSV file and extracts the headers
func GetCSVHeaders(filePath string) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	headers, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV headers: %w", err)
	}

	// Trim spaces from headers
	for i, h := range headers {
		headers[i] = strings.TrimSpace(h)
	}

	return headers, nil
}
