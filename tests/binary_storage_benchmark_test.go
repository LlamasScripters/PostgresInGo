package main

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/LlamasScripters/PostgresInGo/internal/engine"
	"github.com/LlamasScripters/PostgresInGo/internal/types"
)

// BenchmarkStorageComparison compares JSON vs Binary storage performance
func BenchmarkStorageComparison(b *testing.B) {
	// Test data setup
	testCases := []struct {
		name     string
		rowCount int
		colCount int
	}{
		{"Small_10x5", 10, 5},
		{"Medium_100x10", 100, 10},
		{"Large_1000x20", 1000, 20},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			b.Run("JSON_Storage", func(b *testing.B) {
				benchmarkStorageMode(b, engine.JSONStorage, tc.rowCount, tc.colCount)
			})
			b.Run("Binary_Storage", func(b *testing.B) {
				benchmarkStorageMode(b, engine.BinaryStorage, tc.rowCount, tc.colCount)
			})
		})
	}
}

// benchmarkStorageMode benchmarks a specific storage mode
func benchmarkStorageMode(b *testing.B, mode engine.StorageMode, rowCount, colCount int) {
	// Create temporary directory for each test
	dataDir := fmt.Sprintf("/tmp/benchmark_%s_%d", fmt.Sprintf("%d", time.Now().UnixNano()), mode)
	defer os.RemoveAll(dataDir)

	// Create engine with specified storage mode
	var eng *engine.PostgresEngine
	var err error

	config := engine.EngineConfig{
		DataDir:     dataDir,
		StorageMode: mode,
	}
	eng, err = engine.NewPostgresEngineWithConfig(config)
	if err != nil {
		b.Fatalf("Failed to create engine: %v", err)
	}
	defer eng.Close()

	// Create test table schema
	schema := createBenchmarkSchema(colCount)
	err = eng.CreateTable("benchmark_table", schema)
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	// Reset timer before benchmark
	b.ResetTimer()

	// Benchmark write operations
	b.Run("Insert", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for row := 0; row < rowCount; row++ {
				data := generateBenchmarkData(row, colCount)
				err := eng.Insert("benchmark_table", data)
				if err != nil {
					b.Fatalf("Insert failed: %v", err)
				}
			}
		}
	})

	// Benchmark read operations
	b.Run("Select", func(b *testing.B) {
		// First insert some data
		for row := 0; row < rowCount; row++ {
			data := generateBenchmarkData(row, colCount)
			eng.Insert("benchmark_table", data)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := eng.Select("benchmark_table", nil)
			if err != nil {
				b.Fatalf("Select failed: %v", err)
			}
		}
	})
}

// createBenchmarkSchema creates a schema for benchmarking
func createBenchmarkSchema(colCount int) types.Schema {
	columns := make([]types.Column, colCount)

	// Create mixed column types for realistic testing
	for i := 0; i < colCount; i++ {
		switch i % 5 {
		case 0:
			columns[i] = types.Column{
				Name:     fmt.Sprintf("id_%d", i),
				Type:     types.IntType,
				Nullable: false,
			}
		case 1:
			columns[i] = types.Column{
				Name:     fmt.Sprintf("name_%d", i),
				Type:     types.VarcharType,
				Size:     100,
				Nullable: true,
			}
		case 2:
			columns[i] = types.Column{
				Name:     fmt.Sprintf("amount_%d", i),
				Type:     types.DoubleType,
				Nullable: true,
			}
		case 3:
			columns[i] = types.Column{
				Name:     fmt.Sprintf("active_%d", i),
				Type:     types.BoolType,
				Nullable: false,
			}
		case 4:
			columns[i] = types.Column{
				Name:     fmt.Sprintf("timestamp_%d", i),
				Type:     types.TimestampType,
				Nullable: true,
			}
		}
	}

	return types.Schema{Columns: columns}
}

// generateBenchmarkData generates test data for benchmarking
func generateBenchmarkData(row, colCount int) map[string]any {
	data := make(map[string]any)

	for i := 0; i < colCount; i++ {
		switch i % 5 {
		case 0:
			data[fmt.Sprintf("id_%d", i)] = row*colCount + i
		case 1:
			data[fmt.Sprintf("name_%d", i)] = fmt.Sprintf("test_name_%d_%d", row, i)
		case 2:
			data[fmt.Sprintf("amount_%d", i)] = float64(row*100+i) / 100.0
		case 3:
			data[fmt.Sprintf("active_%d", i)] = (row+i)%2 == 0
		case 4:
			data[fmt.Sprintf("timestamp_%d", i)] = time.Now().Add(time.Duration(row*i) * time.Second)
		}
	}

	return data
}

// BenchmarkSerializationSpeed compares serialization speed
func BenchmarkSerializationSpeed(b *testing.B) {
	dataDir := "/tmp/benchmark_serialization"
	defer os.RemoveAll(dataDir)

	// Test data
	testData := map[string]any{
		"id":          12345,
		"name":        "John Doe",
		"email":       "john.doe@example.com",
		"age":         30,
		"salary":      75000.50,
		"active":      true,
		"created_at":  time.Now(),
		"description": "A test user with various data types for benchmarking serialization performance",
	}

	schema := types.Schema{
		Columns: []types.Column{
			{Name: "id", Type: types.IntType},
			{Name: "name", Type: types.VarcharType, Size: 100},
			{Name: "email", Type: types.VarcharType, Size: 255},
			{Name: "age", Type: types.IntType},
			{Name: "salary", Type: types.DoubleType},
			{Name: "active", Type: types.BoolType},
			{Name: "created_at", Type: types.TimestampType},
			{Name: "description", Type: types.TextType},
		},
	}

	b.Run("JSON_Serialization", func(b *testing.B) {
		config := engine.EngineConfig{
			DataDir:     dataDir + "_json",
			StorageMode: engine.JSONStorage,
		}
		eng, err := engine.NewPostgresEngineWithConfig(config)
		if err != nil {
			b.Fatalf("Failed to create engine: %v", err)
		}
		defer eng.Close()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Test JSON serialization speed
			serialized := eng.DeserializeDataForTesting([]byte("id:12345;name:John Doe;email:john.doe@example.com;"))
			_ = serialized
		}
	})

	b.Run("Binary_Serialization", func(b *testing.B) {
		config := engine.EngineConfig{
			DataDir:     dataDir + "_binary",
			StorageMode: engine.BinaryStorage,
		}
		eng, err := engine.NewPostgresEngineWithConfig(config)
		if err != nil {
			b.Fatalf("Failed to create engine: %v", err)
		}
		defer eng.Close()

		err = eng.CreateTable("test_table", schema)
		if err != nil {
			b.Fatalf("Failed to create table: %v", err)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Test binary serialization speed
			err := eng.Insert("test_table", testData)
			if err != nil {
				b.Fatalf("Insert failed: %v", err)
			}
		}
	})
}

// BenchmarkMemoryUsage compares memory usage patterns
func BenchmarkMemoryUsage(b *testing.B) {
	rowCounts := []int{100, 500, 1000}

	for _, rowCount := range rowCounts {
		b.Run(fmt.Sprintf("Rows_%d", rowCount), func(b *testing.B) {
			b.Run("JSON_Memory", func(b *testing.B) {
				benchmarkMemoryUsage(b, engine.JSONStorage, rowCount)
			})
			b.Run("Binary_Memory", func(b *testing.B) {
				benchmarkMemoryUsage(b, engine.BinaryStorage, rowCount)
			})
		})
	}
}

// benchmarkMemoryUsage measures memory usage for storage operations
func benchmarkMemoryUsage(b *testing.B, mode engine.StorageMode, rowCount int) {
	dataDir := fmt.Sprintf("/tmp/memory_test_%d_%d", mode, time.Now().UnixNano())
	defer os.RemoveAll(dataDir)

	config := engine.EngineConfig{
		DataDir:     dataDir,
		StorageMode: mode,
	}
	eng, err := engine.NewPostgresEngineWithConfig(config)
	if err != nil {
		b.Fatalf("Failed to create engine: %v", err)
	}
	defer eng.Close()

	schema := createBenchmarkSchema(10) // 10 columns
	err = eng.CreateTable("memory_test", schema)
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for row := 0; row < rowCount; row++ {
			data := generateBenchmarkData(row, 10)
			err := eng.Insert("memory_test", data)
			if err != nil {
				b.Fatalf("Insert failed: %v", err)
			}
		}

		// Read all data back
		_, err := eng.Select("memory_test", nil)
		if err != nil {
			b.Fatalf("Select failed: %v", err)
		}
	}
}

// BenchmarkCacheEfficiency tests cache line efficiency
func BenchmarkCacheEfficiency(b *testing.B) {
	b.Run("Sequential_Access", func(b *testing.B) {
		benchmarkCachePattern(b, true)
	})
	b.Run("Random_Access", func(b *testing.B) {
		benchmarkCachePattern(b, false)
	})
}

// benchmarkCachePattern tests different access patterns
func benchmarkCachePattern(b *testing.B, sequential bool) {
	dataDir := fmt.Sprintf("/tmp/cache_test_%d", time.Now().UnixNano())
	defer os.RemoveAll(dataDir)

	config := engine.EngineConfig{
		DataDir:     dataDir,
		StorageMode: engine.BinaryStorage,
	}
	eng, err := engine.NewPostgresEngineWithConfig(config)
	if err != nil {
		b.Fatalf("Failed to create engine: %v", err)
	}
	defer eng.Close()

	// Create table with cache-aligned columns
	schema := types.Schema{
		Columns: []types.Column{
			{Name: "id", Type: types.IntType},
			{Name: "data1", Type: types.IntType},
			{Name: "data2", Type: types.IntType},
			{Name: "data3", Type: types.IntType},
			{Name: "data4", Type: types.IntType},
			{Name: "data5", Type: types.IntType},
			{Name: "data6", Type: types.IntType},
			{Name: "data7", Type: types.IntType},
			{Name: "data8", Type: types.IntType}, // 8 ints = 32 bytes, fits in half cache line
		},
	}

	err = eng.CreateTable("cache_test", schema)
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	const numRows = 1000
	for i := 0; i < numRows; i++ {
		data := map[string]any{
			"id":    i,
			"data1": i * 1,
			"data2": i * 2,
			"data3": i * 3,
			"data4": i * 4,
			"data5": i * 5,
			"data6": i * 6,
			"data7": i * 7,
			"data8": i * 8,
		}
		err := eng.Insert("cache_test", data)
		if err != nil {
			b.Fatalf("Insert failed: %v", err)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if sequential {
			// Sequential access pattern (cache-friendly)
			_, err := eng.Select("cache_test", nil)
			if err != nil {
				b.Fatalf("Select failed: %v", err)
			}
		} else {
			// Random access pattern (less cache-friendly)
			for j := 0; j < 100; j++ {
				id := (j * 17) % numRows // Pseudo-random access
				filter := map[string]any{"id": id}
				_, err := eng.Select("cache_test", filter)
				if err != nil {
					b.Fatalf("Select failed: %v", err)
				}
			}
		}
	}
}
