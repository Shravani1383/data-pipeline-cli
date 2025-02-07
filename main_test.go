package main

import (
	"encoding/csv"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Helper function to create a temporary file
func createTempCSV(content [][]string) (string, error) {
	file, err := os.CreateTemp("", "test.csv")
	if err != nil {
		return "", err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	err = writer.WriteAll(content)
	if err != nil {
		return "", err
	}
	writer.Flush()
	return file.Name(), nil
}

// Test: Valid CSV file
func TestGetCSVHeaders_ValidCSV(t *testing.T) {
	filePath, err := createTempCSV([][]string{
		{"col1", "col2", "col3"},
	})
	assert.NoError(t, err)
	defer os.Remove(filePath)

	headers, err := getCSVHeaders(filePath)

	assert.NoError(t, err)
	assert.Equal(t, []string{"col1", "col2", "col3"}, headers)
}

// Test: File does not exist
func TestGetCSVHeaders_FileNotExist(t *testing.T) {
	_, err := getCSVHeaders("nonexistent.csv")

	assert.Error(t, err)
}

// Test: Empty file
func TestGetCSVHeaders_EmptyFile(t *testing.T) {
	filePath, err := createTempCSV([][]string{})
	assert.NoError(t, err)
	defer os.Remove(filePath)

	_, err = getCSVHeaders(filePath)

	assert.Error(t, err)
}

// Test: File exists but is not a CSV (binary data)
func TestGetCSVHeaders_InvalidCSVFormat(t *testing.T) {
	file, err := os.CreateTemp("", "test.bin")
	assert.NoError(t, err)
	defer os.Remove(file.Name())

	_, err = file.Write([]byte{0x00, 0x01, 0x02, 0x03}) // Writing binary data
	assert.NoError(t, err)

	_, err = getCSVHeaders(file.Name())

	assert.Error(t, err)
}

// Test: CSV file with no headers (first row empty)
func TestGetCSVHeaders_NoHeaders(t *testing.T) {
	filePath, err := createTempCSV([][]string{
		{},
		{"data1", "data2", "data3"},
	})
	assert.NoError(t, err)
	defer os.Remove(filePath)

	_, err = getCSVHeaders(filePath)

	assert.Error(t, err)
}

// Test: CSV file with extra spaces in headers
func TestGetCSVHeaders_TrimmedHeaders(t *testing.T) {
	filePath, err := createTempCSV([][]string{
		{" col1 ", " col2 ", " col3 "},
	})
	assert.NoError(t, err)
	defer os.Remove(filePath)

	headers, err := getCSVHeaders(filePath)

	assert.NoError(t, err)
	assert.Equal(t, []string{" col1 ", " col2 ", " col3 "}, headers) // Spaces should remain unless explicitly trimmed
}

// package main

// import (
// 	"context"
// 	"fmt"
// 	"testing"

// 	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
// 	"github.com/stretchr/testify/assert"
// 	"github.com/stretchr/testify/mock"
// )

// // Mock ClickHouse Connection
// type MockClickhouse struct {
// 	mock.Mock
// }

// func (m *MockClickhouse) Exec(ctx context.Context, query string, args ...interface{}) error {
// 	argsList := m.Called(ctx, query, args)
// 	return argsList.Error(0)
// }

// // Other required methods to implement driver.Conn interface
// func (m *MockClickhouse) AsyncInsert(ctx context.Context, query string, onDuplicate bool, args ...any) error {
// 	return nil
// }
// func (m *MockClickhouse) Close() error               { return nil }
// func (m *MockClickhouse) IsClosed() bool             { return false }
// func (m *MockClickhouse) Ping(context.Context) error { return nil }
// func (m *MockClickhouse) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
// 	return nil, nil
// }
// func (m *MockClickhouse) Query(context.Context, string, ...interface{}) (driver.Rows, error) {
// 	return nil, nil
// }
// func (m *MockClickhouse) QueryRow(context.Context, string, ...interface{}) driver.Row {
// 	return nil
// }
// func (m *MockClickhouse) Stats() driver.Stats { return driver.Stats{} }
// func (m *MockClickhouse) Select(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
// 	return nil
// }
// func (m *MockClickhouse) Contributors() []string                        { return nil }
// func (m *MockClickhouse) ServerVersion() (*driver.ServerVersion, error) { return nil, nil }

// // ✅ Test: Table is created successfully
// func TestCreateTable_Success(t *testing.T) {
// 	mockDB := new(MockClickhouse)
// 	mockDB.On("Exec", mock.Anything, mock.Anything, mock.Anything).Return(nil)

// 	headers := []string{"id", "name"}
// 	err := createTable(mockDB, "users", headers)

// 	assert.NoError(t, err)
// 	mockDB.AssertExpectations(t)
// }

// // ❌ Test: Database execution fails
// func TestCreateTable_FailExec(t *testing.T) {
// 	mockDB := new(MockClickhouse)
// 	mockDB.On("Exec", mock.Anything, mock.Anything, mock.Anything).Return(fmt.Errorf("DB error"))

// 	headers := []string{"id", "name"}
// 	err := createTable(mockDB, "users", headers)

// 	assert.Error(t, err)
// 	assert.Equal(t, "error creating table: DB error", err.Error())
// 	mockDB.AssertExpectations(t)
// }

// // ✅ Test: Table name with special characters
// func TestCreateTable_SpecialCharactersInTableName(t *testing.T) {
// 	mockDB := new(MockClickhouse)
// 	mockDB.On("Exec", mock.Anything, mock.Anything, mock.Anything).Return(nil)

// 	headers := []string{"id", "name"}
// 	err := createTable(mockDB, "user-data-123", headers)

// 	assert.NoError(t, err)
// 	mockDB.AssertExpectations(t)
// }

// // ✅ Test: Column names with special characters
// func TestCreateTable_SpecialCharactersInColumnNames(t *testing.T) {
// 	mockDB := new(MockClickhouse)
// 	mockDB.On("Exec", mock.Anything, mock.Anything, mock.Anything).Return(nil)

// 	headers := []string{"user@id", "first-name", "last_name"}
// 	err := createTable(mockDB, "users", headers)

// 	assert.NoError(t, err)
// 	mockDB.AssertExpectations(t)
// }

// // ✅ Test: Empty column list
// // ✅ Test: Fail if no columns are provided
// func TestCreateTable_EmptyColumns(t *testing.T) {
// 	mockDB := new(MockClickhouse)

// 	// Ensure the mockDB does NOT expect Exec to be called because the function should return an error before that.
// 	headers := []string{}
// 	err := createTable(mockDB, "empty_table", headers)

// 	assert.Error(t, err)
// 	assert.Equal(t, "error creating table: no columns provided", err.Error())

// 	mockDB.AssertNotCalled(t, "Exec", mock.Anything, mock.Anything, mock.Anything)
// }

// // ✅ Test: Single-column table
// func TestCreateTable_SingleColumn(t *testing.T) {
// 	mockDB := new(MockClickhouse)
// 	mockDB.On("Exec", mock.Anything, mock.Anything, mock.Anything).Return(nil)

// 	headers := []string{"id"}
// 	err := createTable(mockDB, "single_column_table", headers)

// 	assert.NoError(t, err)
// 	mockDB.AssertExpectations(t)
// }

// // ✅ Test: Multiple columns
// func TestCreateTable_MultipleColumns(t *testing.T) {
// 	mockDB := new(MockClickhouse)
// 	mockDB.On("Exec", mock.Anything, mock.Anything, mock.Anything).Return(nil)

// 	headers := []string{"id", "name", "email", "created_at"}
// 	err := createTable(mockDB, "users", headers)

// 	assert.NoError(t, err)
// 	mockDB.AssertExpectations(t)
// }
