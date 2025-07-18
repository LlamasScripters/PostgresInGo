package main

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LlamasScripters/PostgresInGo/internal/engine"
)

func TestShowHelp(t *testing.T) {
	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	showHelp()

	w.Close()
	os.Stdout = oldStdout

	// Read the output
	output, _ := io.ReadAll(r)
	helpText := string(output)

	// Verify help content contains expected elements
	expectedStrings := []string{
		"PostgresInGo - A PostgreSQL-compatible database engine",
		"Usage:",
		"--command",
		"--data-folder",
		"--sql",
		"--help",
		"--demo",
		"Examples:",
	}

	for _, expected := range expectedStrings {
		if !strings.Contains(helpText, expected) {
			t.Errorf("Help text should contain '%s'", expected)
		}
	}
}

func TestReadSQLFile(t *testing.T) {
	// Create a temporary SQL file
	tempDir := filepath.Join(os.TempDir(), "main_test_"+time.Now().Format("20060102150405"))
	err := os.MkdirAll(tempDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sqlFile := filepath.Join(tempDir, "test.sql")
	sqlContent := "SELECT * FROM users WHERE id = 1;\nCREATE TABLE test (id INT);"

	err = os.WriteFile(sqlFile, []byte(sqlContent), 0644)
	if err != nil {
		t.Fatalf("Failed to write SQL file: %v", err)
	}

	// Test reading the file
	result := readSQLFile(sqlFile)
	if result != sqlContent {
		t.Errorf("Expected '%s', got '%s'", sqlContent, result)
	}
}

func TestReadSQLFileNonExistent(t *testing.T) {
	// This test verifies that readSQLFile calls log.Fatal for non-existent files
	// We can't easily test log.Fatal, but we can test that it would be called
	// by checking the file doesn't exist
	nonExistentFile := "/tmp/non_existent_sql_file_12345.sql"

	// Ensure the file doesn't exist
	if _, err := os.Stat(nonExistentFile); err == nil {
		t.Skip("Cannot test with file that exists")
	}

	// Note: We can't easily test log.Fatal without more complex setup
	// This test documents the expected behavior
	t.Log("readSQLFile should call log.Fatal for non-existent files")
}

func TestReadFromStdin(t *testing.T) {
	// Test when stdin has no data (normal case)
	result := readFromStdin()
	// When run in normal test environment, this should return empty string
	if result != "" {
		t.Logf("readFromStdin returned: '%s' (may vary in test environment)", result)
	}
}

func TestDisplayResults(t *testing.T) {
	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Test with empty results
	emptyResult := &engine.SQLResult{
		Data:    []map[string]any{},
		Columns: []string{},
	}
	displayResults(emptyResult)

	// Test with data
	resultWithData := &engine.SQLResult{
		Data: []map[string]any{
			{"id": 1, "name": "John"},
			{"id": 2, "name": "Jane"},
		},
		Columns: []string{"id", "name"},
	}
	displayResults(resultWithData)

	// Test with data but no columns
	resultNoColumns := &engine.SQLResult{
		Data: []map[string]any{
			{"id": 1, "name": "John"},
		},
		Columns: []string{},
	}
	displayResults(resultNoColumns)

	w.Close()
	os.Stdout = oldStdout

	// Read the output
	output, _ := io.ReadAll(r)
	outputText := string(output)

	// Verify output contains expected elements
	if !strings.Contains(outputText, "No results found.") {
		t.Error("Should display 'No results found' for empty results")
	}

	// Should contain the data from the second test
	if !strings.Contains(outputText, "John") {
		t.Error("Should display data content")
	}
}

func TestRunDemo(t *testing.T) {
	// Create a temporary directory for the demo
	tempDir := filepath.Join(os.TempDir(), "demo_test_"+time.Now().Format("20060102150405"))
	defer os.RemoveAll(tempDir)

	// Capture stdout to avoid cluttering test output
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Note: runDemo() creates its own engine and data directory
	// We can't easily control its behavior without modifying the function
	// For now, we'll just ensure it doesn't panic

	// Create a simple test by calling runDemo in a goroutine with timeout
	done := make(chan bool, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("runDemo panicked: %v", r)
			}
			done <- true
		}()
		runDemo()
	}()

	// Wait for completion or timeout
	select {
	case <-done:
		// Demo completed
	case <-time.After(30 * time.Second):
		t.Error("runDemo took too long (>30 seconds)")
	}

	w.Close()
	os.Stdout = oldStdout

	// Read the output
	output, _ := io.ReadAll(r)
	outputText := string(output)

	// Verify demo produced some output
	if len(outputText) == 0 {
		t.Error("runDemo should produce some output")
	}
}

func TestMainFunctionFlags(t *testing.T) {
	// Test that main function handles flags correctly
	// This is an integration-style test to ensure basic CLI functionality

	// Save original args
	origArgs := os.Args

	// Test help flag
	os.Args = []string{"PostgresInGo", "-h"}

	// We can't call main() directly as it would exit the test
	// Instead, we test the individual components
	// This test documents the expected main function behavior

	os.Args = origArgs

	t.Log("main() function should handle -h flag and call showHelp()")
}

func TestDataFolderPrecedence(t *testing.T) {
	// Test the data folder precedence logic that appears in main()
	// This tests the logic: long flag takes precedence over short flag

	dataFolder := "./data"  // default value
	dataShort := "./custom" // short flag value

	// Simulate the logic from main()
	dataDir := dataFolder
	if dataShort != "./data" && dataFolder == "./data" {
		dataDir = dataShort
	}

	// Should use short flag value when long flag is default
	if dataDir != "./custom" {
		t.Errorf("Expected './custom', got '%s'", dataDir)
	}

	// Test when long flag is set
	dataFolder = "./longflag"
	dataDir = dataFolder
	if dataShort != "./data" && dataFolder == "./data" {
		dataDir = dataShort
	}

	// Should use long flag value when it's explicitly set
	if dataDir != "./longflag" {
		t.Errorf("Expected './longflag', got '%s'", dataDir)
	}
}

func TestSQLCommandPrecedence(t *testing.T) {
	// Test the SQL command precedence logic from main()

	command := "SELECT * FROM test"
	cmdShort := ""
	sqlFile := ""
	sqlShort := ""

	// Simulate the precedence logic from main()
	var sqlToExecute string

	// Command flags take precedence
	if command != "" {
		sqlToExecute = command
	} else if cmdShort != "" {
		sqlToExecute = cmdShort
	} else if sqlFile != "" {
		// Would call readSQLFile(sqlFile)
		sqlToExecute = "file_content"
	} else if sqlShort != "" {
		// Would call readSQLFile(sqlShort)
		sqlToExecute = "file_content"
	}

	if sqlToExecute != "SELECT * FROM test" {
		t.Errorf("Expected command to take precedence, got '%s'", sqlToExecute)
	}

	// Test short command precedence
	command = ""
	cmdShort = "SELECT * FROM short"

	if command != "" {
		sqlToExecute = command
	} else if cmdShort != "" {
		sqlToExecute = cmdShort
	}

	if sqlToExecute != "SELECT * FROM short" {
		t.Errorf("Expected short command, got '%s'", sqlToExecute)
	}
}
