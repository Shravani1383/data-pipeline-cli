package main

import (
	"flag"
	"fmt"
	"os"
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
	db, err := ConnectDB()
	if err != nil {
		fmt.Println("Error connecting to ClickHouse:", err)
		os.Exit(1)
	}
	defer db.Close()

	// Read CSV to get column names
	headers, err := GetCSVHeaders(*csvFilePath)
	if err != nil {
		fmt.Println("Error reading CSV file:", err)
		os.Exit(1)
	}

	// Create table if it doesn't exist
	if err := CreateTable(db, *tableName, headers); err != nil {
		fmt.Println("Error creating table:", err)
		os.Exit(1)
	}

	// Ingest CSV data
	if err := IngestCSV(db, *csvFilePath, *tableName); err != nil {
		fmt.Println("Error ingesting CSV:", err)
		os.Exit(1)
	}

	fmt.Println("Data successfully ingested into ClickHouse!")
}
