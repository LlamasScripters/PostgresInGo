package execution

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/LlamasScripters/PostgresInGo/internal/index"
	"github.com/LlamasScripters/PostgresInGo/internal/storage"
	"github.com/LlamasScripters/PostgresInGo/internal/types"
)

// TestNewExecutionEngine tests execution engine creation
func TestNewExecutionEngine(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("exec_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(testDir)

	// Create storage and index managers
	storageManager, err := storage.NewStorageManager(testDir)
	if err != nil {
		t.Fatalf("Failed to create storage manager: %v", err)
	}
	defer storageManager.Close()

	indexManager := index.NewIndexManager()

	// Create execution engine
	engine := NewExecutionEngine(storageManager, indexManager)
	if engine == nil {
		t.Fatal("Execution engine should not be nil")
	}

	if engine.storageManager != storageManager {
		t.Error("Storage manager should be set correctly")
	}

	if engine.indexManager != indexManager {
		t.Error("Index manager should be set correctly")
	}
}

// TestSeqScanOperator tests sequential scan operator
func TestSeqScanOperator(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("exec_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(testDir)

	// Create storage manager
	storageManager, err := storage.NewStorageManager(testDir)
	if err != nil {
		t.Fatalf("Failed to create storage manager: %v", err)
	}
	defer storageManager.Close()

	// Create test table
	schema := types.Schema{
		Columns: []types.Column{
			{Name: "id", Type: types.IntType},
			{Name: "name", Type: types.VarcharType},
		},
	}

	table := &types.Table{
		Name:   "test_table",
		Schema: schema,
		Pages:  []uint64{1, 2, 3},
	}

	// Create sequential scan operator
	scanOp := NewSeqScanOperator(table, nil, storageManager)
	if scanOp == nil {
		t.Fatal("Sequential scan operator should not be nil")
	}

	if scanOp.table != table {
		t.Error("Table should be set correctly")
	}

	if scanOp.storage != storageManager {
		t.Error("Storage manager should be set correctly")
	}

	// Test open
	err = scanOp.Open()
	if err != nil {
		t.Fatalf("Failed to open scan operator: %v", err)
	}

	if !scanOp.opened {
		t.Error("Operator should be marked as opened")
	}

	// Test double open
	err = scanOp.Open()
	if err == nil {
		t.Error("Should fail when opening already opened operator")
	}

	// Test close
	err = scanOp.Close()
	if err != nil {
		t.Fatalf("Failed to close scan operator: %v", err)
	}

	if scanOp.opened {
		t.Error("Operator should be marked as closed")
	}
}

// TestSeqScanOperatorWithPredicate tests sequential scan with predicate
func TestSeqScanOperatorWithPredicate(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("exec_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(testDir)

	// Create storage manager
	storageManager, err := storage.NewStorageManager(testDir)
	if err != nil {
		t.Fatalf("Failed to create storage manager: %v", err)
	}
	defer storageManager.Close()

	// Create test table
	schema := types.Schema{
		Columns: []types.Column{
			{Name: "id", Type: types.IntType},
			{Name: "name", Type: types.VarcharType},
		},
	}

	table := &types.Table{
		Name:   "test_table",
		Schema: schema,
		Pages:  []uint64{1},
	}

	// Create predicate
	predicate := &Predicate{
		Column:   "id",
		Operator: "=",
		Value:    1,
	}

	// Create sequential scan operator with predicate
	scanOp := NewSeqScanOperator(table, predicate, storageManager)
	if scanOp.predicate != predicate {
		t.Error("Predicate should be set correctly")
	}

	// Test schema
	schema2 := scanOp.GetSchema()
	if len(schema2.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(schema2.Columns))
	}
}

// TestIndexScanOperator tests index scan operator
func TestIndexScanOperator(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("exec_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(testDir)

	// Create storage manager
	storageManager, err := storage.NewStorageManager(testDir)
	if err != nil {
		t.Fatalf("Failed to create storage manager: %v", err)
	}
	defer storageManager.Close()

	// Create test table
	schema := types.Schema{
		Columns: []types.Column{
			{Name: "id", Type: types.IntType},
			{Name: "name", Type: types.VarcharType},
		},
	}

	table := &types.Table{
		Name:   "test_table",
		Schema: schema,
		Pages:  []uint64{1},
	}

	// Create index
	indexManager := index.NewIndexManager()
	err = indexManager.CreateIndex("test_idx", types.IntType)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	btree, err := indexManager.GetIndex("test_idx")
	if err != nil {
		t.Fatalf("Failed to get index: %v", err)
	}

	// Create predicate
	predicate := &Predicate{
		Column:   "id",
		Operator: "=",
		Value:    1,
	}

	// Create index scan operator
	indexOp := NewIndexScanOperator(btree, predicate, storageManager, table)
	if indexOp == nil {
		t.Fatal("Index scan operator should not be nil")
	}

	if indexOp.index != btree {
		t.Error("Index should be set correctly")
	}

	if indexOp.predicate != predicate {
		t.Error("Predicate should be set correctly")
	}

	// Test open
	err = indexOp.Open()
	if err != nil {
		t.Fatalf("Failed to open index operator: %v", err)
	}

	// Test close
	err = indexOp.Close()
	if err != nil {
		t.Fatalf("Failed to close index operator: %v", err)
	}
}

// TestNestedLoopJoinOperator tests nested loop join operator
func TestNestedLoopJoinOperator(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("exec_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(testDir)

	// Create storage manager
	storageManager, err := storage.NewStorageManager(testDir)
	if err != nil {
		t.Fatalf("Failed to create storage manager: %v", err)
	}
	defer storageManager.Close()

	// Create test tables
	leftTable := &types.Table{
		Name: "left_table",
		Schema: types.Schema{
			Columns: []types.Column{
				{Name: "id", Type: types.IntType},
				{Name: "name", Type: types.VarcharType},
			},
		},
		Pages: []uint64{1},
	}

	rightTable := &types.Table{
		Name: "right_table",
		Schema: types.Schema{
			Columns: []types.Column{
				{Name: "id", Type: types.IntType},
				{Name: "value", Type: types.IntType},
			},
		},
		Pages: []uint64{2},
	}

	// Create scan operators
	leftScan := NewSeqScanOperator(leftTable, nil, storageManager)
	rightScan := NewSeqScanOperator(rightTable, nil, storageManager)

	// Create join predicate
	joinPredicate := &JoinPredicate{
		LeftColumn:  "id",
		RightColumn: "id",
		Operator:    "=",
		LeftSchema:  leftTable.Schema,
		RightSchema: rightTable.Schema,
	}

	// Create nested loop join operator
	joinOp := NewNestedLoopJoinOperator(leftScan, rightScan, joinPredicate)
	if joinOp == nil {
		t.Fatal("Nested loop join operator should not be nil")
	}

	if joinOp.left != leftScan {
		t.Error("Left operator should be set correctly")
	}

	if joinOp.right != rightScan {
		t.Error("Right operator should be set correctly")
	}

	if joinOp.predicate != joinPredicate {
		t.Error("Join predicate should be set correctly")
	}

	// Test schema
	joinedSchema := joinOp.GetSchema()
	if len(joinedSchema.Columns) != 4 {
		t.Errorf("Expected 4 columns in joined schema, got %d", len(joinedSchema.Columns))
	}

	// Test open
	err = joinOp.Open()
	if err != nil {
		t.Fatalf("Failed to open join operator: %v", err)
	}

	// Test close
	err = joinOp.Close()
	if err != nil {
		t.Fatalf("Failed to close join operator: %v", err)
	}
}

// TestHashJoinOperator tests hash join operator
func TestHashJoinOperator(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("exec_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(testDir)

	// Create storage manager
	storageManager, err := storage.NewStorageManager(testDir)
	if err != nil {
		t.Fatalf("Failed to create storage manager: %v", err)
	}
	defer storageManager.Close()

	// Create test tables
	leftTable := &types.Table{
		Name: "left_table",
		Schema: types.Schema{
			Columns: []types.Column{
				{Name: "id", Type: types.IntType},
				{Name: "name", Type: types.VarcharType},
			},
		},
		Pages: []uint64{1},
	}

	rightTable := &types.Table{
		Name: "right_table",
		Schema: types.Schema{
			Columns: []types.Column{
				{Name: "id", Type: types.IntType},
				{Name: "value", Type: types.IntType},
			},
		},
		Pages: []uint64{2},
	}

	// Create scan operators
	leftScan := NewSeqScanOperator(leftTable, nil, storageManager)
	rightScan := NewSeqScanOperator(rightTable, nil, storageManager)

	// Create join predicate
	joinPredicate := &JoinPredicate{
		LeftColumn:  "id",
		RightColumn: "id",
		Operator:    "=",
		LeftSchema:  leftTable.Schema,
		RightSchema: rightTable.Schema,
	}

	// Create hash join operator
	hashJoinOp := NewHashJoinOperator(leftScan, rightScan, joinPredicate)
	if hashJoinOp == nil {
		t.Fatal("Hash join operator should not be nil")
	}

	if hashJoinOp.left != leftScan {
		t.Error("Left operator should be set correctly")
	}

	if hashJoinOp.right != rightScan {
		t.Error("Right operator should be set correctly")
	}

	if hashJoinOp.predicate != joinPredicate {
		t.Error("Join predicate should be set correctly")
	}

	// Test hash table initialization
	if hashJoinOp.hashTable == nil {
		t.Error("Hash table should be initialized")
	}

	// Test schema
	joinedSchema := hashJoinOp.GetSchema()
	if len(joinedSchema.Columns) != 4 {
		t.Errorf("Expected 4 columns in joined schema, got %d", len(joinedSchema.Columns))
	}

	// Test open
	err = hashJoinOp.Open()
	if err != nil {
		t.Fatalf("Failed to open hash join operator: %v", err)
	}

	// Test close
	err = hashJoinOp.Close()
	if err != nil {
		t.Fatalf("Failed to close hash join operator: %v", err)
	}
}

// TestProjectionOperator tests projection operator
func TestProjectionOperator(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("exec_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(testDir)

	// Create storage manager
	storageManager, err := storage.NewStorageManager(testDir)
	if err != nil {
		t.Fatalf("Failed to create storage manager: %v", err)
	}
	defer storageManager.Close()

	// Create test table
	table := &types.Table{
		Name: "test_table",
		Schema: types.Schema{
			Columns: []types.Column{
				{Name: "id", Type: types.IntType},
				{Name: "name", Type: types.VarcharType},
				{Name: "age", Type: types.IntType},
			},
		},
		Pages: []uint64{1},
	}

	// Create scan operator
	scanOp := NewSeqScanOperator(table, nil, storageManager)

	// Create projection columns
	columns := []string{"id", "name"}

	// Create projection operator
	projOp := NewProjectionOperator(scanOp, columns)
	if projOp == nil {
		t.Fatal("Projection operator should not be nil")
	}

	if projOp.child != scanOp {
		t.Error("Child operator should be set correctly")
	}

	if len(projOp.columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(projOp.columns))
	}

	// Test schema
	projectedSchema := projOp.GetSchema()
	if len(projectedSchema.Columns) != 2 {
		t.Errorf("Expected 2 columns in projected schema, got %d", len(projectedSchema.Columns))
	}

	// Test open
	err = projOp.Open()
	if err != nil {
		t.Fatalf("Failed to open projection operator: %v", err)
	}

	// Test close
	err = projOp.Close()
	if err != nil {
		t.Fatalf("Failed to close projection operator: %v", err)
	}
}

// TestLeftJoinOperator tests left join operator
func TestLeftJoinOperator(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("exec_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(testDir)

	// Create storage manager
	storageManager, err := storage.NewStorageManager(testDir)
	if err != nil {
		t.Fatalf("Failed to create storage manager: %v", err)
	}
	defer storageManager.Close()

	// Create test tables
	leftTable := &types.Table{
		Name: "left_table",
		Schema: types.Schema{
			Columns: []types.Column{
				{Name: "id", Type: types.IntType},
				{Name: "name", Type: types.VarcharType},
			},
		},
		Pages: []uint64{1},
	}

	rightTable := &types.Table{
		Name: "right_table",
		Schema: types.Schema{
			Columns: []types.Column{
				{Name: "id", Type: types.IntType},
				{Name: "value", Type: types.IntType},
			},
		},
		Pages: []uint64{2},
	}

	// Create scan operators
	leftScan := NewSeqScanOperator(leftTable, nil, storageManager)
	rightScan := NewSeqScanOperator(rightTable, nil, storageManager)

	// Create join predicate
	joinPredicate := &JoinPredicate{
		LeftColumn:  "id",
		RightColumn: "id",
		Operator:    "=",
		LeftSchema:  leftTable.Schema,
		RightSchema: rightTable.Schema,
	}

	// Create left join operator
	leftJoinOp := NewLeftJoinOperator(leftScan, rightScan, joinPredicate)
	if leftJoinOp == nil {
		t.Fatal("Left join operator should not be nil")
	}

	// Test schema
	joinedSchema := leftJoinOp.GetSchema()
	if len(joinedSchema.Columns) != 4 {
		t.Errorf("Expected 4 columns in joined schema, got %d", len(joinedSchema.Columns))
	}

	// Test open
	err = leftJoinOp.Open()
	if err != nil {
		t.Fatalf("Failed to open left join operator: %v", err)
	}

	// Test close
	err = leftJoinOp.Close()
	if err != nil {
		t.Fatalf("Failed to close left join operator: %v", err)
	}
}

// TestRightJoinOperator tests right join operator
func TestRightJoinOperator(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("exec_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(testDir)

	// Create storage manager
	storageManager, err := storage.NewStorageManager(testDir)
	if err != nil {
		t.Fatalf("Failed to create storage manager: %v", err)
	}
	defer storageManager.Close()

	// Create test tables
	leftTable := &types.Table{
		Name: "left_table",
		Schema: types.Schema{
			Columns: []types.Column{
				{Name: "id", Type: types.IntType},
				{Name: "name", Type: types.VarcharType},
			},
		},
		Pages: []uint64{1},
	}

	rightTable := &types.Table{
		Name: "right_table",
		Schema: types.Schema{
			Columns: []types.Column{
				{Name: "id", Type: types.IntType},
				{Name: "value", Type: types.IntType},
			},
		},
		Pages: []uint64{2},
	}

	// Create scan operators
	leftScan := NewSeqScanOperator(leftTable, nil, storageManager)
	rightScan := NewSeqScanOperator(rightTable, nil, storageManager)

	// Create join predicate
	joinPredicate := &JoinPredicate{
		LeftColumn:  "id",
		RightColumn: "id",
		Operator:    "=",
		LeftSchema:  leftTable.Schema,
		RightSchema: rightTable.Schema,
	}

	// Create right join operator
	rightJoinOp := NewRightJoinOperator(leftScan, rightScan, joinPredicate)
	if rightJoinOp == nil {
		t.Fatal("Right join operator should not be nil")
	}

	// Test schema
	joinedSchema := rightJoinOp.GetSchema()
	if len(joinedSchema.Columns) != 4 {
		t.Errorf("Expected 4 columns in joined schema, got %d", len(joinedSchema.Columns))
	}

	// Test open
	err = rightJoinOp.Open()
	if err != nil {
		t.Fatalf("Failed to open right join operator: %v", err)
	}

	// Test close
	err = rightJoinOp.Close()
	if err != nil {
		t.Fatalf("Failed to close right join operator: %v", err)
	}
}

// TestCrossJoinOperator tests cross join operator
func TestCrossJoinOperator(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("exec_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(testDir)

	// Create storage manager
	storageManager, err := storage.NewStorageManager(testDir)
	if err != nil {
		t.Fatalf("Failed to create storage manager: %v", err)
	}
	defer storageManager.Close()

	// Create test tables
	leftTable := &types.Table{
		Name: "left_table",
		Schema: types.Schema{
			Columns: []types.Column{
				{Name: "id", Type: types.IntType},
				{Name: "name", Type: types.VarcharType},
			},
		},
		Pages: []uint64{1},
	}

	rightTable := &types.Table{
		Name: "right_table",
		Schema: types.Schema{
			Columns: []types.Column{
				{Name: "id", Type: types.IntType},
				{Name: "value", Type: types.IntType},
			},
		},
		Pages: []uint64{2},
	}

	// Create scan operators
	leftScan := NewSeqScanOperator(leftTable, nil, storageManager)
	rightScan := NewSeqScanOperator(rightTable, nil, storageManager)

	// Create cross join operator
	crossJoinOp := NewCrossJoinOperator(leftScan, rightScan)
	if crossJoinOp == nil {
		t.Fatal("Cross join operator should not be nil")
	}

	// Test schema
	joinedSchema := crossJoinOp.GetSchema()
	if len(joinedSchema.Columns) != 4 {
		t.Errorf("Expected 4 columns in joined schema, got %d", len(joinedSchema.Columns))
	}

	// Test open
	err = crossJoinOp.Open()
	if err != nil {
		t.Fatalf("Failed to open cross join operator: %v", err)
	}

	// Test close
	err = crossJoinOp.Close()
	if err != nil {
		t.Fatalf("Failed to close cross join operator: %v", err)
	}
}

// TestAggregateOperator tests aggregate operator
func TestAggregateOperator(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("exec_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(testDir)

	// Create storage manager
	storageManager, err := storage.NewStorageManager(testDir)
	if err != nil {
		t.Fatalf("Failed to create storage manager: %v", err)
	}
	defer storageManager.Close()

	// Create test table
	table := &types.Table{
		Name: "test_table",
		Schema: types.Schema{
			Columns: []types.Column{
				{Name: "id", Type: types.IntType},
				{Name: "category", Type: types.VarcharType},
				{Name: "value", Type: types.IntType},
			},
		},
		Pages: []uint64{1},
	}

	// Create scan operator
	scanOp := NewSeqScanOperator(table, nil, storageManager)

	// Create aggregate functions
	aggregates := []*AggregateFunction{
		{Type: "COUNT", Column: "*", Alias: "count"},
		{Type: "SUM", Column: "value", Alias: "sum_value"},
		{Type: "AVG", Column: "value", Alias: "avg_value"},
		{Type: "MIN", Column: "value", Alias: "min_value"},
		{Type: "MAX", Column: "value", Alias: "max_value"},
	}

	// Create aggregate operator
	aggOp := NewAggregateOperator(scanOp, []string{"category"}, aggregates)
	if aggOp == nil {
		t.Fatal("Aggregate operator should not be nil")
	}

	if aggOp.child != scanOp {
		t.Error("Child operator should be set correctly")
	}

	if len(aggOp.groupBy) != 1 {
		t.Errorf("Expected 1 group by column, got %d", len(aggOp.groupBy))
	}

	if len(aggOp.aggregates) != 5 {
		t.Errorf("Expected 5 aggregates, got %d", len(aggOp.aggregates))
	}

	// Test schema
	aggSchema := aggOp.GetSchema()
	if len(aggSchema.Columns) != 6 { // 1 group by + 5 aggregates
		t.Errorf("Expected 6 columns in aggregate schema, got %d", len(aggSchema.Columns))
	}

	// Test open
	err = aggOp.Open()
	if err != nil {
		t.Fatalf("Failed to open aggregate operator: %v", err)
	}

	// Test close
	err = aggOp.Close()
	if err != nil {
		t.Fatalf("Failed to close aggregate operator: %v", err)
	}
}

// TestPredicate tests predicate evaluation
func TestPredicate(t *testing.T) {
	// Create predicate
	predicate := &Predicate{
		Column:   "id",
		Operator: "=",
		Value:    1,
	}

	// Test basic predicate properties
	if predicate.Column != "id" {
		t.Errorf("Expected column 'id', got '%s'", predicate.Column)
	}

	if predicate.Operator != "=" {
		t.Errorf("Expected operator '=', got '%s'", predicate.Operator)
	}

	if predicate.Value != 1 {
		t.Errorf("Expected value 1, got %v", predicate.Value)
	}

	// Test evaluation (simplified)
	tuple := &types.Tuple{
		Data: []byte("id:1;name:test;"),
	}

	result := predicate.Evaluate(tuple)
	if !result {
		t.Error("Predicate should evaluate to true (simplified implementation)")
	}
}

// TestJoinPredicate tests join predicate evaluation
func TestJoinPredicate(t *testing.T) {
	// Create join predicate
	joinPredicate := &JoinPredicate{
		LeftColumn:  "id",
		RightColumn: "user_id",
		Operator:    "=",
		LeftSchema: types.Schema{
			Columns: []types.Column{
				{Name: "id", Type: types.IntType},
				{Name: "name", Type: types.VarcharType},
			},
		},
		RightSchema: types.Schema{
			Columns: []types.Column{
				{Name: "user_id", Type: types.IntType},
				{Name: "value", Type: types.IntType},
			},
		},
	}

	// Test basic predicate properties
	if joinPredicate.LeftColumn != "id" {
		t.Errorf("Expected left column 'id', got '%s'", joinPredicate.LeftColumn)
	}

	if joinPredicate.RightColumn != "user_id" {
		t.Errorf("Expected right column 'user_id', got '%s'", joinPredicate.RightColumn)
	}

	if joinPredicate.Operator != "=" {
		t.Errorf("Expected operator '=', got '%s'", joinPredicate.Operator)
	}

	// Test evaluation
	leftTuple := &types.Tuple{
		Data: []byte("id:1;name:Alice;"),
	}

	rightTuple := &types.Tuple{
		Data: []byte("user_id:1;value:100;"),
	}

	result := joinPredicate.Evaluate(leftTuple, rightTuple)
	if !result {
		t.Error("Join predicate should evaluate to true for matching ids")
	}

	// Test with non-matching values
	rightTuple2 := &types.Tuple{
		Data: []byte("user_id:2;value:200;"),
	}

	result2 := joinPredicate.Evaluate(leftTuple, rightTuple2)
	if result2 {
		t.Error("Join predicate should evaluate to false for non-matching ids")
	}
}

// TestJoinPredicateValueComparison tests value comparison methods
func TestJoinPredicateValueComparison(t *testing.T) {
	joinPredicate := &JoinPredicate{}

	// Test equality
	if !joinPredicate.compareValues(1, 1, "=") {
		t.Error("Should return true for equal values")
	}

	if joinPredicate.compareValues(1, 2, "=") {
		t.Error("Should return false for unequal values")
	}

	// Test inequality
	if joinPredicate.compareValues(1, 1, "!=") {
		t.Error("Should return false for equal values with != operator")
	}

	if !joinPredicate.compareValues(1, 2, "!=") {
		t.Error("Should return true for unequal values with != operator")
	}

	// Test less than
	if !joinPredicate.compareValues(1, 2, "<") {
		t.Error("Should return true for 1 < 2")
	}

	if joinPredicate.compareValues(2, 1, "<") {
		t.Error("Should return false for 2 < 1")
	}

	// Test greater than
	if !joinPredicate.compareValues(2, 1, ">") {
		t.Error("Should return true for 2 > 1")
	}

	if joinPredicate.compareValues(1, 2, ">") {
		t.Error("Should return false for 1 > 2")
	}

	// Test null values
	if !joinPredicate.compareValues(nil, nil, "IS NULL") {
		t.Error("Should return true for null values with IS NULL")
	}

	if joinPredicate.compareValues(1, nil, "IS NULL") {
		t.Error("Should return false for non-null value with IS NULL")
	}
}

// TestAggregateFunction tests aggregate function properties
func TestAggregateFunction(t *testing.T) {
	// Test COUNT function
	countFunc := &AggregateFunction{
		Type:   "COUNT",
		Column: "*",
		Alias:  "count",
	}

	if countFunc.Type != "COUNT" {
		t.Errorf("Expected type 'COUNT', got '%s'", countFunc.Type)
	}

	if countFunc.Column != "*" {
		t.Errorf("Expected column '*', got '%s'", countFunc.Column)
	}

	if countFunc.Alias != "count" {
		t.Errorf("Expected alias 'count', got '%s'", countFunc.Alias)
	}

	// Test SUM function
	sumFunc := &AggregateFunction{
		Type:   "SUM",
		Column: "value",
		Alias:  "sum_value",
	}

	if sumFunc.Type != "SUM" {
		t.Errorf("Expected type 'SUM', got '%s'", sumFunc.Type)
	}

	if sumFunc.Column != "value" {
		t.Errorf("Expected column 'value', got '%s'", sumFunc.Column)
	}

	if sumFunc.Alias != "sum_value" {
		t.Errorf("Expected alias 'sum_value', got '%s'", sumFunc.Alias)
	}
}

// TestOperatorErrorHandling tests error handling in operators
func TestOperatorErrorHandling(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("exec_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(testDir)

	// Create storage manager
	storageManager, err := storage.NewStorageManager(testDir)
	if err != nil {
		t.Fatalf("Failed to create storage manager: %v", err)
	}
	defer storageManager.Close()

	// Create test table
	table := &types.Table{
		Name: "test_table",
		Schema: types.Schema{
			Columns: []types.Column{
				{Name: "id", Type: types.IntType},
				{Name: "name", Type: types.VarcharType},
			},
		},
		Pages: []uint64{1},
	}

	// Create scan operator
	scanOp := NewSeqScanOperator(table, nil, storageManager)

	// Test calling Next() before Open()
	_, err = scanOp.Next()
	if err == nil {
		t.Error("Should fail when calling Next() before Open()")
	}

	// Test calling Next() after Close()
	err = scanOp.Open()
	if err != nil {
		t.Fatalf("Failed to open operator: %v", err)
	}

	err = scanOp.Close()
	if err != nil {
		t.Fatalf("Failed to close operator: %v", err)
	}

	_, err = scanOp.Next()
	if err == nil {
		t.Error("Should fail when calling Next() after Close()")
	}
}

// TestTupleDataSerialization tests tuple data serialization/deserialization
func TestTupleDataSerialization(t *testing.T) {
	// Create join operator to test its serialization methods
	joinOp := &NestedLoopJoinOperator{}

	// Test data serialization
	data := map[string]interface{}{
		"id":     1,
		"name":   "Alice",
		"active": true,
		"score":  95.5,
	}

	serialized := joinOp.serializeTupleData(data)
	if len(serialized) == 0 {
		t.Error("Serialized data should not be empty")
	}

	// Test data deserialization
	deserialized := joinOp.deserializeTupleData(serialized)
	if deserialized == nil {
		t.Error("Deserialized data should not be nil")
	}

	// Test specific field deserialization
	if val, exists := deserialized["id"]; !exists || val != 1 {
		t.Errorf("Expected id=1, got %v", val)
	}

	if val, exists := deserialized["name"]; !exists || val != "Alice" {
		t.Errorf("Expected name=Alice, got %v", val)
	}

	if val, exists := deserialized["active"]; !exists || val != true {
		t.Errorf("Expected active=true, got %v", val)
	}

	if val, exists := deserialized["score"]; !exists || val != 95.5 {
		t.Errorf("Expected score=95.5, got %v", val)
	}
}