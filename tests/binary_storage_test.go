package main

import (
	"os"
	"testing"

	"github.com/LlamasScripters/PostgresInGo/internal/engine"
	"github.com/LlamasScripters/PostgresInGo/internal/types"
)

// TestBinaryStorageBasics tests basic binary storage functionality
func TestBinaryStorageBasics(t *testing.T) {
	// Test binary storage
	testDir := "/tmp/test_binary_storage"
	defer os.RemoveAll(testDir)

	// Create engine with binary storage
	eng, err := engine.NewPostgresEngineWithBinary(testDir)
	if err != nil {
		t.Fatalf("Failed to create binary engine: %v", err)
	}
	defer eng.Close()

	// Create test table
	schema := types.Schema{
		Columns: []types.Column{
			{Name: "id", Type: types.IntType, Nullable: false},
			{Name: "name", Type: types.VarcharType, Size: 100, Nullable: false},
			{Name: "active", Type: types.BoolType, Nullable: false},
		},
	}

	err = eng.CreateTable("test_table", schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	testData := map[string]any{
		"id":     1,
		"name":   "Test User",
		"active": true,
	}

	err = eng.Insert("test_table", testData)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Select data back
	results, err := eng.Select("test_table", nil)
	if err != nil {
		t.Fatalf("Failed to select data: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}

	t.Logf("Binary storage test passed successfully!")
}

// TestStorageModeComparison compares JSON vs Binary storage
func TestStorageModeComparison(t *testing.T) {
	testCases := []struct {
		name string
		mode engine.StorageMode
	}{
		{"JSON Storage", engine.JSONStorage},
		{"Binary Storage", engine.BinaryStorage},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testDir := "/tmp/test_" + tc.name
			defer os.RemoveAll(testDir)

			config := engine.EngineConfig{
				DataDir:     testDir,
				StorageMode: tc.mode,
			}

			eng, err := engine.NewPostgresEngineWithConfig(config)
			if err != nil {
				t.Fatalf("Failed to create engine: %v", err)
			}
			defer eng.Close()

			// Create test table
			schema := types.Schema{
				Columns: []types.Column{
					{Name: "id", Type: types.IntType},
					{Name: "data", Type: types.VarcharType, Size: 50},
				},
			}

			err = eng.CreateTable("comparison_test", schema)
			if err != nil {
				t.Fatalf("Failed to create table: %v", err)
			}

			// Insert multiple records
			for i := 0; i < 10; i++ {
				data := map[string]any{
					"id":   i,
					"data": "test_data_" + string(rune('a'+i)),
				}
				err = eng.Insert("comparison_test", data)
				if err != nil {
					t.Fatalf("Failed to insert data: %v", err)
				}
			}

			// Verify data retrieval
			results, err := eng.Select("comparison_test", nil)
			if err != nil {
				t.Fatalf("Failed to select data: %v", err)
			}

			if len(results) != 10 {
				t.Fatalf("Expected 10 results, got %d", len(results))
			}

			t.Logf("%s test completed successfully with %d records", tc.name, len(results))
		})
	}
}
