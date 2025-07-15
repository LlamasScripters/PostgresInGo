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
		for b.Loop() {
			for row := range rowCount {
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
		for row := range rowCount {
			data := generateBenchmarkData(row, colCount)
			eng.Insert("benchmark_table", data)
		}

		b.ResetTimer()
		for b.Loop() {
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
	for i := range colCount {
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

	for i := range colCount {
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

// benchmarkVectorizedProcessing tests vectorized column processing
func benchmarkVectorizedProcessing(b *testing.B, dataDir string) {
	config := engine.EngineConfig{
		DataDir:     dataDir + "_vectorized",
		StorageMode: engine.BinaryStorage,
	}
	eng, err := engine.NewPostgresEngineWithConfig(config)
	if err != nil {
		b.Fatalf("Failed to create engine: %v", err)
	}
	defer eng.Close()

	// Create schema with many columns to test vectorization
	schema := createWideSchema(32) // 32 columns
	err = eng.CreateTable("vectorized_test", schema)
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		// Insert many rows to test vectorized bitmap processing
		for i := range 100 {
			data := generateWideData(i, 32)
			err := eng.Insert("vectorized_test", data)
			if err != nil {
				b.Fatalf("Insert failed: %v", err)
			}
		}
	}
}

// benchmarkAdaptiveBatching tests adaptive batch size optimization
func benchmarkAdaptiveBatching(b *testing.B, dataDir string) {
	config := engine.EngineConfig{
		DataDir:     dataDir + "_adaptive",
		StorageMode: engine.BinaryStorage,
	}
	eng, err := engine.NewPostgresEngineWithConfig(config)
	if err != nil {
		b.Fatalf("Failed to create engine: %v", err)
	}
	defer eng.Close()

	// Test different schema sizes
	testCases := []struct {
		name     string
		colCount int
		rowCount int
	}{
		{"SmallSchema_8cols", 8, 200},
		{"MediumSchema_24cols", 24, 200},
		{"LargeSchema_64cols", 64, 200},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			schema := createWideSchema(tc.colCount)
			tableName := fmt.Sprintf("adaptive_test_%d", tc.colCount)
			err = eng.CreateTable(tableName, schema)
			if err != nil {
				b.Fatalf("Failed to create table: %v", err)
			}

			b.ResetTimer()
			b.ReportAllocs()

			for b.Loop() {
				for i := range tc.rowCount {
					data := generateWideData(i, tc.colCount)
					err := eng.Insert(tableName, data)
					if err != nil {
						b.Fatalf("Insert failed: %v", err)
					}
				}
			}
		})
	}
}

// benchmarkParallelSerialization tests parallel processing performance
func benchmarkParallelSerialization(b *testing.B, dataDir string) {
	config := engine.EngineConfig{
		DataDir:     dataDir + "_parallel",
		StorageMode: engine.BinaryStorage,
	}
	eng, err := engine.NewPostgresEngineWithConfig(config)
	if err != nil {
		b.Fatalf("Failed to create engine: %v", err)
	}
	defer eng.Close()

	schema := createWideSchema(16)
	err = eng.CreateTable("parallel_test", schema)
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	// Test different dataset sizes to see where parallel processing helps
	datasetSizes := []int{50, 200, 1000, 5000}

	for _, size := range datasetSizes {
		b.Run(fmt.Sprintf("Dataset_%d", size), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for b.Loop() {
				for i := range size {
					data := generateWideData(i, 16)
					err := eng.Insert("parallel_test", data)
					if err != nil {
						b.Fatalf("Insert failed: %v", err)
					}
				}
			}
		})
	}
}

// benchmarkUnrolledValueWriting tests optimized value writing
func benchmarkUnrolledValueWriting(b *testing.B, dataDir string) {
	config := engine.EngineConfig{
		DataDir:     dataDir + "_unrolled",
		StorageMode: engine.BinaryStorage,
	}
	eng, err := engine.NewPostgresEngineWithConfig(config)
	if err != nil {
		b.Fatalf("Failed to create engine: %v", err)
	}
	defer eng.Close()

	// Create schema focused on types that benefit from unrolled writing
	schema := types.Schema{
		Columns: []types.Column{
			{Name: "id", Type: types.IntType},
			{Name: "value1", Type: types.BigIntType},
			{Name: "value2", Type: types.IntType},
			{Name: "value3", Type: types.BigIntType},
			{Name: "name", Type: types.VarcharType, Size: 50},
			{Name: "description", Type: types.TextType},
			{Name: "active", Type: types.BoolType},
			{Name: "score", Type: types.DoubleType},
		},
	}

	err = eng.CreateTable("unrolled_test", schema)
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		for i := range 500 {
			data := map[string]any{
				"id":          i,
				"value1":      int64(i * 1000),
				"value2":      i * 100,
				"value3":      int64(i * 10000),
				"name":        fmt.Sprintf("user_%d", i),
				"description": fmt.Sprintf("Description for user %d with some additional text", i),
				"active":      i%2 == 0,
				"score":       float64(i) * 1.5,
			}
			err := eng.Insert("unrolled_test", data)
			if err != nil {
				b.Fatalf("Insert failed: %v", err)
			}
		}
	}
}

// createWideSchema creates a schema with specified number of columns
func createWideSchema(colCount int) types.Schema {
	columns := make([]types.Column, colCount)

	for i := range colCount {
		switch i % 6 {
		case 0:
			columns[i] = types.Column{
				Name:     fmt.Sprintf("int_col_%d", i),
				Type:     types.IntType,
				Nullable: true,
			}
		case 1:
			columns[i] = types.Column{
				Name:     fmt.Sprintf("bigint_col_%d", i),
				Type:     types.BigIntType,
				Nullable: true,
			}
		case 2:
			columns[i] = types.Column{
				Name:     fmt.Sprintf("varchar_col_%d", i),
				Type:     types.VarcharType,
				Size:     50,
				Nullable: true,
			}
		case 3:
			columns[i] = types.Column{
				Name:     fmt.Sprintf("double_col_%d", i),
				Type:     types.DoubleType,
				Nullable: true,
			}
		case 4:
			columns[i] = types.Column{
				Name:     fmt.Sprintf("bool_col_%d", i),
				Type:     types.BoolType,
				Nullable: true,
			}
		case 5:
			columns[i] = types.Column{
				Name:     fmt.Sprintf("text_col_%d", i),
				Type:     types.TextType,
				Nullable: true,
			}
		}
	}

	return types.Schema{Columns: columns}
}

// generateWideData creates test data for wide schemas
func generateWideData(row, colCount int) map[string]any {
	data := make(map[string]any)

	for i := range colCount {
		// Add some null values to test bitmap processing
		if (row+i)%7 == 0 {
			continue // Leave as null
		}

		switch i % 6 {
		case 0:
			data[fmt.Sprintf("int_col_%d", i)] = row*colCount + i
		case 1:
			data[fmt.Sprintf("bigint_col_%d", i)] = int64((row*colCount + i) * 1000)
		case 2:
			data[fmt.Sprintf("varchar_col_%d", i)] = fmt.Sprintf("test_string_%d_%d", row, i)
		case 3:
			data[fmt.Sprintf("double_col_%d", i)] = float64(row*colCount+i) / 100.0
		case 4:
			data[fmt.Sprintf("bool_col_%d", i)] = (row+i)%3 == 0
		case 5:
			data[fmt.Sprintf("text_col_%d", i)] = fmt.Sprintf("Long text description for row %d column %d with additional content", row, i)
		}
	}

	return data
}

// BenchmarkParallelReading tests parallel reading performance
func BenchmarkParallelReading(b *testing.B) {
	dataDir := "/tmp/benchmark_parallel_reading"
	defer os.RemoveAll(dataDir)

	// Test different dataset sizes for parallel reading
	testCases := []struct {
		name     string
		rowCount int
		colCount int
	}{
		{"Small_50x8", 50, 8},
		{"Medium_200x16", 200, 16},
		{"Large_1000x32", 1000, 32},
		{"XLarge_5000x16", 5000, 16},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			benchmarkParallelReadingScenario(b, dataDir, tc.rowCount, tc.colCount)
		})
	}
}

// benchmarkParallelReadingScenario tests parallel reading for specific scenario
func benchmarkParallelReadingScenario(b *testing.B, dataDir string, rowCount, colCount int) {
	testDataDir := fmt.Sprintf("%s_%dx%d", dataDir, rowCount, colCount)
	config := engine.EngineConfig{
		DataDir:     testDataDir,
		StorageMode: engine.BinaryStorage,
	}
	eng, err := engine.NewPostgresEngineWithConfig(config)
	if err != nil {
		b.Fatalf("Failed to create engine: %v", err)
	}
	defer eng.Close()

	// Create schema and populate data
	schema := createWideSchema(colCount)
	tableName := fmt.Sprintf("parallel_read_test_%dx%d", rowCount, colCount)
	err = eng.CreateTable(tableName, schema)
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	for i := range rowCount {
		data := generateWideData(i, colCount)
		err := eng.Insert(tableName, data)
		if err != nil {
			b.Fatalf("Insert failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	// Benchmark parallel reading
	for b.Loop() {
		_, err := eng.Select(tableName, nil)
		if err != nil {
			b.Fatalf("Select failed: %v", err)
		}
	}
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
		for b.Loop() {
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
		for b.Loop() {
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

	for b.Loop() {
		for row := range rowCount {
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
	for i := range numRows {
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

	for b.Loop() {
		if sequential {
			// Sequential access pattern (cache-friendly)
			_, err := eng.Select("cache_test", nil)
			if err != nil {
				b.Fatalf("Select failed: %v", err)
			}
		} else {
			// Random access pattern (less cache-friendly)
			for j := range 100 {
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

// BenchmarkLoopOptimizations tests the new loop optimization features
func BenchmarkLoopOptimizations(b *testing.B) {
	dataDir := "/tmp/benchmark_loop_optimizations"
	defer os.RemoveAll(dataDir)

	// Test vectorized processing
	b.Run("VectorizedProcessing", func(b *testing.B) {
		benchmarkVectorizedProcessing(b, dataDir)
	})

	// Test adaptive batching
	b.Run("AdaptiveBatching", func(b *testing.B) {
		benchmarkAdaptiveBatching(b, dataDir)
	})

	// Test parallel serialization
	b.Run("ParallelSerialization", func(b *testing.B) {
		benchmarkParallelSerialization(b, dataDir)
	})

	// Test unrolled value writing
	b.Run("UnrolledValueWriting", func(b *testing.B) {
		benchmarkUnrolledValueWriting(b, dataDir)
	})
}
