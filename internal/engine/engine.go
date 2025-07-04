package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/esgi-git/postgres-engine/internal/execution"
	"github.com/esgi-git/postgres-engine/internal/index"
	"github.com/esgi-git/postgres-engine/internal/storage"
	"github.com/esgi-git/postgres-engine/internal/transaction"
	"github.com/esgi-git/postgres-engine/internal/types"
)

// StorageMode defines the storage format type
type StorageMode int

const (
	JSONStorage   StorageMode = iota // Default JSON-based storage
	BinaryStorage                    // Optimized binary storage
)

// EngineConfig contains engine configuration options
type EngineConfig struct {
	DataDir     string
	StorageMode StorageMode
}

// PostgresEngine represents the main database engine
type PostgresEngine struct {
	storageManager     *storage.StorageManager
	binaryStorage      *storage.BinaryStorageManager
	transactionManager *transaction.TransactionManager
	indexManager       *index.IndexManager
	queryExecutor      *execution.ExecutionEngine
	dataDir            string
	databases          map[string]bool
	currentDB          string
	storageMode        StorageMode
	mu                 sync.RWMutex
}

// NewPostgresEngine creates a new PostgreSQL engine with default JSON storage
func NewPostgresEngine(dataDir string) (*PostgresEngine, error) {
	return NewPostgresEngineWithConfig(EngineConfig{
		DataDir:     dataDir,
		StorageMode: JSONStorage,
	})
}

// NewPostgresEngineWithBinary creates a new PostgreSQL engine with binary storage
func NewPostgresEngineWithBinary(dataDir string) (*PostgresEngine, error) {
	return NewPostgresEngineWithConfig(EngineConfig{
		DataDir:     dataDir,
		StorageMode: BinaryStorage,
	})
}

// NewPostgresEngineWithConfig creates a new PostgreSQL engine with custom configuration
func NewPostgresEngineWithConfig(config EngineConfig) (*PostgresEngine, error) {
	// Create data directory if it doesn't exist
	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	var storageManager *storage.StorageManager
	var binaryStorage *storage.BinaryStorageManager
	var err error

	// Initialize appropriate storage manager
	switch config.StorageMode {
	case BinaryStorage:
		binaryStorage, err = storage.NewBinaryStorageManager(config.DataDir)
		if err != nil {
			return nil, fmt.Errorf("failed to create binary storage manager: %w", err)
		}
		storageManager = binaryStorage.StorageManager
	default: // JSONStorage
		storageManager, err = storage.NewStorageManager(config.DataDir)
		if err != nil {
			return nil, fmt.Errorf("failed to create storage manager: %w", err)
		}
	}

	// Initialize transaction manager
	transactionManager, err := transaction.NewTransactionManager(config.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction manager: %w", err)
	}

	// Initialize index manager
	indexManager := index.NewIndexManager()

	// Initialize query executor
	queryExecutor := execution.NewExecutionEngine(storageManager, indexManager)

	engine := &PostgresEngine{
		storageManager:     storageManager,
		binaryStorage:      binaryStorage,
		transactionManager: transactionManager,
		indexManager:       indexManager,
		queryExecutor:      queryExecutor,
		dataDir:            config.DataDir,
		databases:          make(map[string]bool),
		storageMode:        config.StorageMode,
	}

	// Load existing databases
	engine.loadDatabases()

	return engine, nil
}

// Insert adds new data to a table
func (pe *PostgresEngine) Insert(tableName string, data map[string]any) error {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	// Start transaction
	txn := pe.transactionManager.Begin()
	defer func() {
		if err := recover(); err != nil {
			pe.transactionManager.Rollback(txn)
			panic(err)
		} else {
			pe.transactionManager.Commit(txn)
		}
	}()

	// Validate constraints before insertion
	if err := pe.validateConstraintsForInsert(tableName, data); err != nil {
		return err
	}

	// Create tuple from data using optimized serialization when available
	tuple := &types.Tuple{
		Data: pe.serializeDataWithSchema(data, tableName),
	}

	return pe.storageManager.InsertTuple(tableName, tuple)
}

// Select retrieves data from a table with optional filtering
func (pe *PostgresEngine) Select(tableName string, filter map[string]any) ([]*types.Tuple, error) {
	pe.mu.RLock()
	defer pe.mu.RUnlock()

	return pe.selectInternal(tableName, filter)
}

// selectInternal performs selection without acquiring locks (for internal use)
func (pe *PostgresEngine) selectInternal(tableName string, filter map[string]any) ([]*types.Tuple, error) {
	// Get all tuples for this table directly from storage manager
	return pe.getAllTuplesForTable(tableName, filter)
}

// getAllTuplesForTable retrieves all tuples for a table with optional filtering
func (pe *PostgresEngine) getAllTuplesForTable(tableName string, filter map[string]any) ([]*types.Tuple, error) {
	// Get all tuples from storage manager
	tuples, err := pe.storageManager.GetAllTuples(tableName)
	if err != nil {
		return nil, err
	}

	var results []*types.Tuple
	for _, tuple := range tuples {
		// Apply filter if provided
		if filter == nil || pe.matchesFilter(tuple, filter) {
			results = append(results, tuple)
		}
	}

	return results, nil
}

// Update modifies existing data in a table
func (pe *PostgresEngine) Update(tableName string, filter map[string]any, updates map[string]any) (int64, error) {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	// Start transaction
	txn := pe.transactionManager.Begin()
	defer func() {
		if err := recover(); err != nil {
			pe.transactionManager.Rollback(txn)
			panic(err)
		} else {
			pe.transactionManager.Commit(txn)
		}
	}()

	// Find matching tuples (use internal method to avoid deadlock)
	tuples, err := pe.selectInternal(tableName, filter)
	if err != nil {
		return 0, err
	}

	updated := int64(0)
	for _, tuple := range tuples {
		// Update tuple data
		newData := pe.mergeData(tuple.Data, updates)
		updatedTuple := &types.Tuple{
			TID:  tuple.TID,
			Data: newData,
		}

		err := pe.storageManager.UpdateTuple(tableName, tuple.TID, updatedTuple)
		if err != nil {
			return updated, err
		}
		updated++
	}

	return updated, nil
}

// Delete removes data from a table
func (pe *PostgresEngine) Delete(tableName string, filter map[string]any) (int64, error) {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	// Start transaction
	txn := pe.transactionManager.Begin()
	defer func() {
		if err := recover(); err != nil {
			pe.transactionManager.Rollback(txn)
			panic(err)
		} else {
			pe.transactionManager.Commit(txn)
		}
	}()

	// Find matching tuples (use internal method to avoid deadlock)
	tuples, err := pe.selectInternal(tableName, filter)
	if err != nil {
		return 0, err
	}

	deleted := int64(0)
	for _, tuple := range tuples {
		err := pe.storageManager.DeleteTuple(tableName, tuple.TID)
		if err != nil {
			return deleted, err
		}
		deleted++
	}

	return deleted, nil
}

// serializeData converts a map to byte slice for storage
func (pe *PostgresEngine) serializeData(data map[string]any) []byte {
	if pe.storageMode == BinaryStorage {
		// For binary storage, we need the table schema, but we don't have it here
		// Fall back to JSON-style serialization for now
		// This will be optimized when we can pass schema information
		return pe.serializeDataJSON(data)
	}
	return pe.serializeDataJSON(data)
}

// serializeDataWithSchema converts a map to byte slice using table schema for binary optimization
func (pe *PostgresEngine) serializeDataWithSchema(data map[string]any, tableName string) []byte {
	if pe.storageMode == BinaryStorage && pe.binaryStorage != nil {
		// Get table schema for binary serialization
		table, err := pe.storageManager.GetTable(tableName)
		if err == nil {
			return pe.binaryStorage.SerializeTupleBinary(data, table.Schema)
		}
	}
	return pe.serializeDataJSON(data)
}

// serializeDataJSON converts a map to byte slice using JSON-style format
func (pe *PostgresEngine) serializeDataJSON(data map[string]any) []byte {
	// Simplified serialization - in a real implementation, this would use a proper format
	result := make([]byte, 0, 256)
	for key, value := range data {
		keyBytes := []byte(key + ":")
		result = append(result, keyBytes...)
		
		switch v := value.(type) {
		case int:
			result = append(result, []byte(fmt.Sprintf("%d", v))...)
		case string:
			result = append(result, []byte(v)...)
		default:
			result = append(result, []byte(fmt.Sprintf("%v", v))...)
		}
		result = append(result, ';')
	}
	return result
}

// matchesFilter checks if a tuple matches the given filter
func (pe *PostgresEngine) matchesFilter(tuple *types.Tuple, filter map[string]any) bool {
	if len(filter) == 0 {
		return true
	}
	
	// Parse the serialized data back to a map for comparison
	tupleData := pe.deserializeData(tuple.Data)
	
	// Check each filter condition
	for key, value := range filter {
		tupleValue, exists := tupleData[key]
		if !exists {
			return false
		}
		
		// Compare values (simplified comparison)
		if fmt.Sprintf("%v", tupleValue) != fmt.Sprintf("%v", value) {
			return false
		}
	}
	
	return true
}

// mergeData merges updates into existing tuple data
func (pe *PostgresEngine) mergeData(_ []byte, updates map[string]any) []byte {
	// Simplified merge - in a real implementation, this would deserialize,
	// apply updates, and reserialize
	return pe.serializeData(updates)
}

// deserializeData converts byte slice back to a map
func (pe *PostgresEngine) deserializeData(data []byte) map[string]any {
	// Try binary format first if binary storage is enabled
	if pe.storageMode == BinaryStorage && pe.binaryStorage != nil && len(data) > 32 {
		// Check if this looks like binary data (has binary tuple header)
		if pe.isBinaryFormat(data) {
			// We need schema for proper binary deserialization
			// For now, fall back to JSON format
			return pe.deserializeDataJSON(data)
		}
	}
	return pe.deserializeDataJSON(data)
}

// deserializeDataWithSchema converts byte slice back to a map using table schema
func (pe *PostgresEngine) deserializeDataWithSchema(data []byte, tableName string) map[string]any {
	if pe.storageMode == BinaryStorage && pe.binaryStorage != nil {
		table, err := pe.storageManager.GetTable(tableName)
		if err == nil && pe.isBinaryFormat(data) {
			return pe.binaryStorage.DeserializeTupleBinary(data, table.Schema)
		}
	}
	return pe.deserializeDataJSON(data)
}

// isBinaryFormat checks if data is in binary format
func (pe *PostgresEngine) isBinaryFormat(data []byte) bool {
	// Check if data starts with binary tuple header pattern
	// Binary data will have specific structure, while JSON data will be text
	if len(data) < 32 {
		return false
	}
	// Simple heuristic: binary data will have non-printable characters early on
	for i := 0; i < 16 && i < len(data); i++ {
		if data[i] < 32 && data[i] != 0 {
			return true
		}
	}
	return false
}

// deserializeDataJSON converts byte slice back to a map using JSON-style format
func (pe *PostgresEngine) deserializeDataJSON(data []byte) map[string]any {
	result := make(map[string]any)
	dataStr := string(data)
	
	// Split by semicolon to get key-value pairs
	pairs := strings.Split(dataStr, ";")
	for _, pair := range pairs {
		if len(pair) == 0 {
			continue
		}
		
		// Split by colon to get key and value
		parts := strings.SplitN(pair, ":", 2)
		if len(parts) != 2 {
			continue
		}
		
		key := parts[0]
		valueStr := parts[1]
		
		// Try to convert to int, otherwise keep as string
		if intVal, err := strconv.Atoi(valueStr); err == nil {
			result[key] = intVal
		} else {
			result[key] = valueStr
		}
	}
	
	return result
}

// DeserializeDataForTesting exposes data deserialization for testing purposes
func (pe *PostgresEngine) DeserializeDataForTesting(data []byte) map[string]any {
	return pe.deserializeData(data)
}

// CreateDatabase creates a new database
func (pe *PostgresEngine) CreateDatabase(name string) error {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	if _, exists := pe.databases[name]; exists {
		return fmt.Errorf("database %s already exists", name)
	}

	// Create database directory
	dbDir := fmt.Sprintf("%s/%s", pe.dataDir, name)
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		return fmt.Errorf("failed to create database directory: %w", err)
	}

	pe.databases[name] = true
	
	// Save databases metadata
	pe.saveDatabases()
	
	return nil
}

// DropDatabase drops a database
func (pe *PostgresEngine) DropDatabase(name string) error {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	if _, exists := pe.databases[name]; !exists {
		return fmt.Errorf("database %s does not exist", name)
	}

	// Remove database directory
	dbDir := fmt.Sprintf("%s/%s", pe.dataDir, name)
	if err := os.RemoveAll(dbDir); err != nil {
		return fmt.Errorf("failed to remove database directory: %w", err)
	}

	delete(pe.databases, name)
	
	// Save databases metadata
	pe.saveDatabases()
	
	return nil
}

// UseDatabase switches to a database
func (pe *PostgresEngine) UseDatabase(name string) error {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	if _, exists := pe.databases[name]; !exists {
		return fmt.Errorf("database %s does not exist", name)
	}

	pe.currentDB = name
	return nil
}

// CreateTable creates a new table
func (pe *PostgresEngine) CreateTable(name string, schema types.Schema) error {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	return pe.storageManager.CreateTable(name, schema)
}

// DropTable drops a table
func (pe *PostgresEngine) DropTable(name string) error {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	// In a real implementation, this would remove the table from storage
	return fmt.Errorf("drop table not implemented")
}

// CreateIndex creates a new index
func (pe *PostgresEngine) CreateIndex(name, table string, columns []string) error {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	// Get table to determine column type
	tableObj, err := pe.storageManager.GetTable(table)
	if err != nil {
		return err
	}

	// Find column type (simplified - use first column)
	var colType types.DataType = types.IntType
	for _, col := range tableObj.Schema.Columns {
		if col.Name == columns[0] {
			colType = col.Type
			break
		}
	}

	return pe.indexManager.CreateIndex(name, colType)
}

// DropIndex drops an index
func (pe *PostgresEngine) DropIndex(name string) error {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	return pe.indexManager.DropIndex(name)
}

// BeginTransaction starts a new transaction
func (pe *PostgresEngine) BeginTransaction() (*types.Transaction, error) {
	return pe.transactionManager.Begin(), nil
}

// CommitTransaction commits a transaction
func (pe *PostgresEngine) CommitTransaction(txn *types.Transaction) error {
	return pe.transactionManager.Commit(txn)
}

// RollbackTransaction rolls back a transaction
func (pe *PostgresEngine) RollbackTransaction(txn *types.Transaction) error {
	return pe.transactionManager.Rollback(txn)
}

// InsertTuple inserts a tuple into a table
func (pe *PostgresEngine) InsertTuple(tableName string, tuple *types.Tuple) error {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	return pe.storageManager.InsertTuple(tableName, tuple)
}

// SelectTuple selects a tuple by TID
func (pe *PostgresEngine) SelectTuple(tableName string, tid types.TupleID) (*types.Tuple, error) {
	pe.mu.RLock()
	defer pe.mu.RUnlock()

	return pe.storageManager.SelectTuple(tableName, tid)
}

// UpdateTuple updates a tuple
func (pe *PostgresEngine) UpdateTuple(tableName string, tid types.TupleID, tuple *types.Tuple) error {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	return pe.storageManager.UpdateTuple(tableName, tid, tuple)
}

// DeleteTuple deletes a tuple
func (pe *PostgresEngine) DeleteTuple(tableName string, tid types.TupleID) error {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	return pe.storageManager.DeleteTuple(tableName, tid)
}

// GetTable retrieves a table by name
func (pe *PostgresEngine) GetTable(name string) (*types.Table, error) {
	pe.mu.RLock()
	defer pe.mu.RUnlock()

	return pe.storageManager.GetTable(name)
}

// GetIndex retrieves an index by name
func (pe *PostgresEngine) GetIndex(name string) (*index.BTree, error) {
	pe.mu.RLock()
	defer pe.mu.RUnlock()

	return pe.indexManager.GetIndex(name)
}

// GetStats returns database statistics
func (pe *PostgresEngine) GetStats() map[string]interface{} {
	pe.mu.RLock()
	defer pe.mu.RUnlock()

	stats := make(map[string]interface{})
	stats["databases"] = len(pe.databases)
	stats["current_database"] = pe.currentDB
	stats["data_directory"] = pe.dataDir

	return stats
}

// loadDatabases loads database metadata from disk
func (pe *PostgresEngine) loadDatabases() {
	dbFile := filepath.Join(pe.dataDir, "databases.json")
	
	data, err := os.ReadFile(dbFile)
	if err != nil {
		// File doesn't exist, start fresh
		return
	}

	var databases map[string]bool
	err = json.Unmarshal(data, &databases)
	if err != nil {
		fmt.Printf("Error loading databases: %v\n", err)
		return
	}

	pe.databases = databases
}

// saveDatabases saves database metadata to disk
func (pe *PostgresEngine) saveDatabases() {
	dbFile := filepath.Join(pe.dataDir, "databases.json")

	data, err := json.MarshalIndent(pe.databases, "", "  ")
	if err != nil {
		fmt.Printf("Error marshaling databases: %v\n", err)
		return
	}

	err = os.WriteFile(dbFile, data, 0644)
	if err != nil {
		fmt.Printf("Error saving databases: %v\n", err)
	}
}

// Close closes the database engine
func (pe *PostgresEngine) Close() error {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	// Save databases metadata
	pe.saveDatabases()

	// Close storage manager
	if err := pe.storageManager.Close(); err != nil {
		return err
	}

	return nil
}

// QueryOptimizer provides query optimization capabilities
type QueryOptimizer struct {
	statistics *Statistics
	costModel  *CostModel
}

// Statistics holds database statistics
type Statistics struct {
	TableStats map[string]*types.TableStats
}

// CostModel defines cost parameters for different operations
type CostModel struct {
	SeqScanCost    float64
	IndexScanCost  float64
	NestedLoopCost float64
	HashJoinCost   float64
	SortMergeCost  float64
}

// NewQueryOptimizer creates a new query optimizer
func NewQueryOptimizer() *QueryOptimizer {
	return &QueryOptimizer{
		statistics: &Statistics{
			TableStats: make(map[string]*types.TableStats),
		},
		costModel: &CostModel{
			SeqScanCost:    1.0,
			IndexScanCost:  0.1,
			NestedLoopCost: 1.0,
			HashJoinCost:   0.5,
			SortMergeCost:  0.8,
		},
	}
}

// QueryPlan represents an optimized query plan
type QueryPlan struct {
	Root        execution.Operator
	Cost        float64
	Cardinality int64
}

// Optimize optimizes a query plan for operations
func (qo *QueryOptimizer) Optimize(operation string, tableName string) (*QueryPlan, error) {
	// Simplified optimization - just return a basic plan
	plan := &QueryPlan{
		Cost:        1.0,
		Cardinality: 1,
	}

	return plan, nil
}

// AlterTableChange represents a table alteration
type AlterTableChange struct {
	Type   string
	Column types.Column
}

// AlterTable alters a table structure
func (pe *PostgresEngine) AlterTable(name string, changes []AlterTableChange) error {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	// In a real implementation, this would modify the table schema
	return fmt.Errorf("alter table not implemented")
}

// Constraint validation methods

// validateConstraintsForInsert validates all constraints for an insert operation
func (pe *PostgresEngine) validateConstraintsForInsert(tableName string, data map[string]any) error {
	// Validate primary key constraints
	if err := pe.storageManager.ValidatePrimaryKey(tableName, data); err != nil {
		return err
	}

	// Validate foreign key constraints
	if err := pe.storageManager.ValidateForeignKey(tableName, data); err != nil {
		return err
	}

	// Validate unique constraints
	if err := pe.storageManager.ValidateUniqueConstraints(tableName, data); err != nil {
		return err
	}

	// Validate not null constraints
	if err := pe.validateNotNullConstraints(tableName, data); err != nil {
		return err
	}

	return nil
}

// validateNotNullConstraints validates not null constraints
func (pe *PostgresEngine) validateNotNullConstraints(tableName string, data map[string]any) error {
	table, err := pe.storageManager.GetTable(tableName)
	if err != nil {
		return err
	}

	for _, col := range table.Schema.Columns {
		if !col.Nullable {
			value, exists := data[col.Name]
			if !exists || value == nil {
				return fmt.Errorf("column '%s' cannot be null", col.Name)
			}
		}
	}

	return nil
}

// AddPrimaryKey adds a primary key constraint to a table
func (pe *PostgresEngine) AddPrimaryKey(tableName string, columns []string) error {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	table, err := pe.storageManager.GetTable(tableName)
	if err != nil {
		return err
	}

	// Check if table already has a primary key constraint (not just marked columns)
	if table.PrimaryKey != nil {
		return fmt.Errorf("table '%s' already has a primary key", tableName)
	}

	// Validate that all columns exist
	for _, col := range columns {
		if !table.Schema.HasColumn(col) {
			return fmt.Errorf("column '%s' does not exist in table '%s'", col, tableName)
		}
	}

	// Create primary key constraint
	pkConstraint := &types.Constraint{
		Name:    fmt.Sprintf("pk_%s", tableName),
		Type:    types.PrimaryKeyConstraint,
		Columns: columns,
	}

	// Add to table
	table.PrimaryKey = pkConstraint
	table.Schema.Constraints = append(table.Schema.Constraints, *pkConstraint)

	// Mark columns as primary key
	for _, colName := range columns {
		for i := range table.Schema.Columns {
			if table.Schema.Columns[i].Name == colName {
				table.Schema.Columns[i].IsPrimaryKey = true
				table.Schema.Columns[i].Nullable = false // Primary key columns cannot be null
			}
		}
	}

	// Create unique index for primary key
	indexName := fmt.Sprintf("idx_pk_%s", tableName)
	return pe.CreateIndex(indexName, tableName, columns)
}

// AddForeignKey adds a foreign key constraint to a table
func (pe *PostgresEngine) AddForeignKey(tableName string, columns []string, refTable string, refColumns []string, onDelete, onUpdate string) error {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	table, err := pe.storageManager.GetTable(tableName)
	if err != nil {
		return err
	}

	// Validate that all columns exist
	for _, col := range columns {
		if !table.Schema.HasColumn(col) {
			return fmt.Errorf("column '%s' does not exist in table '%s'", col, tableName)
		}
	}

	// Validate that referenced table exists
	refTableObj, err := pe.storageManager.GetTable(refTable)
	if err != nil {
		return fmt.Errorf("referenced table '%s' does not exist", refTable)
	}

	// Validate that all referenced columns exist
	for _, col := range refColumns {
		if !refTableObj.Schema.HasColumn(col) {
			return fmt.Errorf("referenced column '%s' does not exist in table '%s'", col, refTable)
		}
	}

	// Validate that the number of columns matches
	if len(columns) != len(refColumns) {
		return fmt.Errorf("number of foreign key columns must match number of referenced columns")
	}

	// Create foreign key constraint
	fkConstraint := &types.Constraint{
		Name:         fmt.Sprintf("fk_%s_%s", tableName, refTable),
		Type:         types.ForeignKeyConstraint,
		Columns:      columns,
		RefTable:     refTable,
		RefColumns:   refColumns,
		OnDeleteRule: onDelete,
		OnUpdateRule: onUpdate,
	}

	// Add to table
	table.ForeignKeys = append(table.ForeignKeys, fkConstraint)
	table.Schema.Constraints = append(table.Schema.Constraints, *fkConstraint)

	return nil
}

// AddUniqueConstraint adds a unique constraint to a table
func (pe *PostgresEngine) AddUniqueConstraint(tableName string, columns []string) error {
	pe.mu.Lock()
	defer pe.mu.Unlock()

	table, err := pe.storageManager.GetTable(tableName)
	if err != nil {
		return err
	}

	// Validate that all columns exist
	for _, col := range columns {
		if !table.Schema.HasColumn(col) {
			return fmt.Errorf("column '%s' does not exist in table '%s'", col, tableName)
		}
	}

	// Create unique constraint
	uniqueConstraint := &types.Constraint{
		Name:    fmt.Sprintf("uk_%s_%s", tableName, strings.Join(columns, "_")),
		Type:    types.UniqueConstraint,
		Columns: columns,
	}

	// Add to table
	table.UniqueKeys = append(table.UniqueKeys, uniqueConstraint)
	table.Schema.Constraints = append(table.Schema.Constraints, *uniqueConstraint)

	// Create unique index
	indexName := fmt.Sprintf("idx_uk_%s_%s", tableName, strings.Join(columns, "_"))
	return pe.CreateIndex(indexName, tableName, columns)
}

// JOIN operations

// InnerJoin performs an inner join between two tables
func (pe *PostgresEngine) InnerJoin(leftTable, rightTable, leftColumn, rightColumn string) ([]*types.Tuple, error) {
	pe.mu.RLock()
	defer pe.mu.RUnlock()

	return pe.performJoin(leftTable, rightTable, leftColumn, rightColumn, types.InnerJoin)
}

// LeftJoin performs a left outer join between two tables
func (pe *PostgresEngine) LeftJoin(leftTable, rightTable, leftColumn, rightColumn string) ([]*types.Tuple, error) {
	pe.mu.RLock()
	defer pe.mu.RUnlock()

	return pe.performJoin(leftTable, rightTable, leftColumn, rightColumn, types.LeftJoin)
}

// RightJoin performs a right outer join between two tables
func (pe *PostgresEngine) RightJoin(leftTable, rightTable, leftColumn, rightColumn string) ([]*types.Tuple, error) {
	pe.mu.RLock()
	defer pe.mu.RUnlock()

	return pe.performJoin(leftTable, rightTable, leftColumn, rightColumn, types.RightJoin)
}

// CrossJoin performs a cross join (cartesian product) between two tables
func (pe *PostgresEngine) CrossJoin(leftTable, rightTable string) ([]*types.Tuple, error) {
	pe.mu.RLock()
	defer pe.mu.RUnlock()

	return pe.performJoin(leftTable, rightTable, "", "", types.CrossJoin)
}

// performJoin is the internal method that performs the actual join operation
func (pe *PostgresEngine) performJoin(leftTable, rightTable, leftColumn, rightColumn string, joinType types.JoinType) ([]*types.Tuple, error) {
	// Get table objects
	leftTableObj, err := pe.storageManager.GetTable(leftTable)
	if err != nil {
		return nil, fmt.Errorf("left table '%s' not found: %w", leftTable, err)
	}

	rightTableObj, err := pe.storageManager.GetTable(rightTable)
	if err != nil {
		return nil, fmt.Errorf("right table '%s' not found: %w", rightTable, err)
	}

	// Create scan operators for both tables
	leftScan := execution.NewSeqScanOperator(leftTableObj, nil, pe.storageManager)
	rightScan := execution.NewSeqScanOperator(rightTableObj, nil, pe.storageManager)

	// Create join operator based on join type
	var joinOp execution.Operator
	
	if joinType == types.CrossJoin {
		joinOp = execution.NewCrossJoinOperator(leftScan, rightScan)
	} else {
		// Validate join columns exist
		if leftColumn != "" && rightColumn != "" {
			if !leftTableObj.Schema.HasColumn(leftColumn) {
				return nil, fmt.Errorf("column '%s' not found in table '%s'", leftColumn, leftTable)
			}
			if !rightTableObj.Schema.HasColumn(rightColumn) {
				return nil, fmt.Errorf("column '%s' not found in table '%s'", rightColumn, rightTable)
			}
		}

		// Create join predicate
		predicate := &execution.JoinPredicate{
			LeftColumn:  leftColumn,
			RightColumn: rightColumn,
			Operator:    "=",
			LeftSchema:  leftTableObj.Schema,
			RightSchema: rightTableObj.Schema,
		}

		// Create appropriate join operator
		switch joinType {
		case types.InnerJoin:
			joinOp = execution.NewNestedLoopJoinOperator(leftScan, rightScan, predicate)
		case types.LeftJoin:
			joinOp = execution.NewLeftJoinOperator(leftScan, rightScan, predicate)
		case types.RightJoin:
			joinOp = execution.NewRightJoinOperator(leftScan, rightScan, predicate)
		default:
			return nil, fmt.Errorf("unsupported join type: %v", joinType)
		}
	}

	// Execute the join
	err = joinOp.Open()
	if err != nil {
		return nil, fmt.Errorf("failed to open join operator: %w", err)
	}
	defer joinOp.Close()

	// Collect results
	var results []*types.Tuple
	for {
		tuple, err := joinOp.Next()
		if err != nil {
			break // End of results
		}
		results = append(results, tuple)
	}

	return results, nil
}

// Query executes a complex query with joins, filters, and projections
func (pe *PostgresEngine) Query(query *types.QueryPlan) ([]*types.Tuple, error) {
	pe.mu.RLock()
	defer pe.mu.RUnlock()

	if len(query.Tables) == 0 {
		return nil, fmt.Errorf("no tables specified in query")
	}

	// Start with the first table
	var currentOp execution.Operator
	currentOp = execution.NewSeqScanOperator(query.Tables[0], nil, pe.storageManager)

	// Add joins for additional tables
	for i := 1; i < len(query.Tables); i++ {
		rightScan := execution.NewSeqScanOperator(query.Tables[i], nil, pe.storageManager)
		
		// Find join condition for this table
		var joinPredicate *execution.JoinPredicate
		for _, joinCond := range query.JoinConditions {
			// Simple matching - in a real implementation, this would be more sophisticated
			joinPredicate = &execution.JoinPredicate{
				LeftColumn:  joinCond.LeftColumn,
				RightColumn: joinCond.RightColumn,
				Operator:    joinCond.Operator,
				LeftSchema:  currentOp.GetSchema(),
				RightSchema: rightScan.GetSchema(),
			}
			break
		}

		// Create join operator
		switch query.JoinType {
		case types.InnerJoin:
			currentOp = execution.NewNestedLoopJoinOperator(currentOp, rightScan, joinPredicate)
		case types.LeftJoin:
			currentOp = execution.NewLeftJoinOperator(currentOp, rightScan, joinPredicate)
		case types.RightJoin:
			currentOp = execution.NewRightJoinOperator(currentOp, rightScan, joinPredicate)
		case types.CrossJoin:
			currentOp = execution.NewCrossJoinOperator(currentOp, rightScan)
		default:
			currentOp = execution.NewNestedLoopJoinOperator(currentOp, rightScan, joinPredicate)
		}
	}

	// Add projection if specified
	if len(query.SelectColumns) > 0 {
		currentOp = execution.NewProjectionOperator(currentOp, query.SelectColumns)
	}

	// Execute the query
	err := currentOp.Open()
	if err != nil {
		return nil, fmt.Errorf("failed to open query operator: %w", err)
	}
	defer currentOp.Close()

	// Collect results
	var results []*types.Tuple
	for {
		tuple, err := currentOp.Next()
		if err != nil {
			break // End of results
		}
		results = append(results, tuple)
	}

	return results, nil
}