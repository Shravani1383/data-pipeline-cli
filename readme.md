# Ingest Pipeline with ClickHouse

## Overview
This guide provides step-by-step instructions to set up and run an ingestion pipeline using ClickHouse. The pipeline enables data ingestion from a CSV file into a specified ClickHouse table.

## Prerequisites
Before you begin, ensure you have the following:
- ClickHouse installed on your system
- Go installed for building the ingestion tool
- A CSV data file for ingestion

## Installation
### 1. Build the Ingestion Tool
The ingestion tool is built using Go. Use the following commands to compile it:

For Linux/macOS:
```sh
go build -o ingest
```

For Windows:
```sh
go build -o ingest.exe
```

## Setting Up ClickHouse
### 2. Start ClickHouse Client
Connect to ClickHouse using the client:
```sh
clickhouse-client --host=127.0.0.1 --port=9000 --user=default
```

### 3. Create a Database
Create a new database in ClickHouse:
```sql
CREATE DATABASE database_name;
```
Verify the database creation:
```sql
SHOW DATABASES;
```

## Data Ingestion
### 4. Run the Ingestion Tool
To ingest data from a CSV file into a specified table:
```sh
./ingest --file=data.csv --table=table_name
```

## Querying Data
### 5. Verify Data Ingestion
Reconnect to ClickHouse:
```sh
clickhouse-client --host=127.0.0.1 --port=9000 --user=default
```
Query the table to verify that data has been ingested correctly:
```sql
SELECT * FROM database_name.table_name LIMIT 10;
```
### 6. Performing Unit Testing
In order execute all the unit Tests- run this command
```sh
go test -v ./...
```

## Conclusion
This setup allows seamless ingestion of CSV data into ClickHouse, making it easy to query and analyze data efficiently.

