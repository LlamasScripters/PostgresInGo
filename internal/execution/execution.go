package execution

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/esgi-git/postgres-engine/internal/index"
	"github.com/esgi-git/postgres-engine/internal/storage"
	"github.com/esgi-git/postgres-engine/internal/types"
)

// Operator represents a query execution operator
type Operator interface {
	Open() error
	Next() (*types.Tuple, error)
	Close() error
	GetSchema() types.Schema
}

// ExecutionEngine executes query plans
type ExecutionEngine struct {
	storageManager *storage.StorageManager
	indexManager   *index.IndexManager
	mu             sync.RWMutex
}

// NewExecutionEngine creates a new execution engine
func NewExecutionEngine(sm *storage.StorageManager, im *index.IndexManager) *ExecutionEngine {
	return &ExecutionEngine{
		storageManager: sm,
		indexManager:   im,
	}
}

// SeqScanOperator performs sequential scan
type SeqScanOperator struct {
	table      *types.Table
	predicate  *Predicate
	currentTID types.TupleID
	storage    *storage.StorageManager
	opened     bool
}

// NewSeqScanOperator creates a new sequential scan operator
func NewSeqScanOperator(table *types.Table, predicate *Predicate, storage *storage.StorageManager) *SeqScanOperator {
	return &SeqScanOperator{
		table:     table,
		predicate: predicate,
		storage:   storage,
	}
}

// Open opens the sequential scan
func (op *SeqScanOperator) Open() error {
	if op.opened {
		return fmt.Errorf("operator already opened")
	}
	op.opened = true
	op.currentTID = types.TupleID{PageID: 0, Offset: 0}
	return nil
}

// Next returns the next tuple
func (op *SeqScanOperator) Next() (*types.Tuple, error) {
	if !op.opened {
		return nil, fmt.Errorf("operator not opened")
	}

	// Simplified sequential scan
	for _, pageID := range op.table.Pages {
		op.currentTID.PageID = pageID
		
		// Try to get tuple from current position
		tuple, err := op.storage.SelectTuple(op.table.Name, op.currentTID)
		if err != nil {
			// Move to next position
			op.currentTID.Offset++
			continue
		}

		// Check predicate
		if op.predicate != nil && !op.predicate.Evaluate(tuple) {
			op.currentTID.Offset++
			continue
		}

		op.currentTID.Offset++
		return tuple, nil
	}

	return nil, fmt.Errorf("no more tuples")
}

// Close closes the sequential scan
func (op *SeqScanOperator) Close() error {
	op.opened = false
	return nil
}

// GetSchema returns the schema
func (op *SeqScanOperator) GetSchema() types.Schema {
	return op.table.Schema
}

// IndexScanOperator performs index scan
type IndexScanOperator struct {
	index     *index.BTree
	predicate *Predicate
	cursor    *index.IndexCursor
	storage   *storage.StorageManager
	table     *types.Table
	opened    bool
}

// NewIndexScanOperator creates a new index scan operator
func NewIndexScanOperator(idx *index.BTree, predicate *Predicate, storage *storage.StorageManager, table *types.Table) *IndexScanOperator {
	return &IndexScanOperator{
		index:     idx,
		predicate: predicate,
		storage:   storage,
		table:     table,
	}
}

// Open opens the index scan
func (op *IndexScanOperator) Open() error {
	if op.opened {
		return fmt.Errorf("operator already opened")
	}
	op.opened = true
	
	// Initialize cursor based on predicate
	if op.predicate != nil {
		cursor, err := op.index.RangeScan(op.predicate.Value, op.predicate.Value)
		if err != nil {
			return err
		}
		op.cursor = cursor
	}
	
	return nil
}

// Next returns the next tuple
func (op *IndexScanOperator) Next() (*types.Tuple, error) {
	if !op.opened {
		return nil, fmt.Errorf("operator not opened")
	}

	if op.cursor == nil {
		return nil, fmt.Errorf("no more tuples")
	}

	// Get next tuple from index
	_, tid, err := op.cursor.Next()
	if err != nil {
		return nil, err
	}

	// Fetch actual tuple from storage
	tuple, err := op.storage.SelectTuple(op.table.Name, tid)
	if err != nil {
		return nil, err
	}

	return tuple, nil
}

// Close closes the index scan
func (op *IndexScanOperator) Close() error {
	op.opened = false
	return nil
}

// GetSchema returns the schema
func (op *IndexScanOperator) GetSchema() types.Schema {
	return op.table.Schema
}

// NestedLoopJoinOperator performs nested loop join
type NestedLoopJoinOperator struct {
	left       Operator
	right      Operator
	predicate  *JoinPredicate
	leftTuple  *types.Tuple
	rightOpen  bool
	opened     bool
}

// NewNestedLoopJoinOperator creates a new nested loop join operator
func NewNestedLoopJoinOperator(left, right Operator, predicate *JoinPredicate) *NestedLoopJoinOperator {
	return &NestedLoopJoinOperator{
		left:      left,
		right:     right,
		predicate: predicate,
	}
}

// Open opens the nested loop join
func (op *NestedLoopJoinOperator) Open() error {
	if op.opened {
		return fmt.Errorf("operator already opened")
	}
	op.opened = true
	
	err := op.left.Open()
	if err != nil {
		return err
	}
	
	return nil
}

// Next returns the next joined tuple
func (op *NestedLoopJoinOperator) Next() (*types.Tuple, error) {
	if !op.opened {
		return nil, fmt.Errorf("operator not opened")
	}

	for {
		// If we don't have a left tuple, get one
		if op.leftTuple == nil {
			leftTuple, err := op.left.Next()
			if err != nil {
				return nil, err
			}
			op.leftTuple = leftTuple
			
			// Open right operator for this left tuple
			if !op.rightOpen {
				err = op.right.Open()
				if err != nil {
					return nil, err
				}
				op.rightOpen = true
			}
		}

		// Try to get a matching right tuple
		rightTuple, err := op.right.Next()
		if err != nil {
			// No more right tuples, close right and get next left
			op.right.Close()
			op.rightOpen = false
			op.leftTuple = nil
			continue
		}

		// Check join predicate
		if op.predicate != nil && !op.predicate.Evaluate(op.leftTuple, rightTuple) {
			continue
		}

		// Create joined tuple (simplified)
		joinedTuple := &types.Tuple{
			Data: append(op.leftTuple.Data, rightTuple.Data...),
		}

		return joinedTuple, nil
	}
}

// Close closes the nested loop join
func (op *NestedLoopJoinOperator) Close() error {
	op.opened = false
	op.left.Close()
	if op.rightOpen {
		op.right.Close()
	}
	return nil
}

// GetSchema returns the joined schema
func (op *NestedLoopJoinOperator) GetSchema() types.Schema {
	leftSchema := op.left.GetSchema()
	rightSchema := op.right.GetSchema()
	
	joinedSchema := types.Schema{
		Columns: append(leftSchema.Columns, rightSchema.Columns...),
	}
	
	return joinedSchema
}

// HashJoinOperator performs hash join
type HashJoinOperator struct {
	left       Operator
	right      Operator
	predicate  *JoinPredicate
	hashTable  map[string][]*types.Tuple
	buildDone  bool
	rightTuple *types.Tuple
	opened     bool
}

// NewHashJoinOperator creates a new hash join operator
func NewHashJoinOperator(left, right Operator, predicate *JoinPredicate) *HashJoinOperator {
	return &HashJoinOperator{
		left:      left,
		right:     right,
		predicate: predicate,
		hashTable: make(map[string][]*types.Tuple),
	}
}

// Open opens the hash join
func (op *HashJoinOperator) Open() error {
	if op.opened {
		return fmt.Errorf("operator already opened")
	}
	op.opened = true
	
	err := op.left.Open()
	if err != nil {
		return err
	}
	
	err = op.right.Open()
	if err != nil {
		return err
	}
	
	return nil
}

// Next returns the next joined tuple
func (op *HashJoinOperator) Next() (*types.Tuple, error) {
	if !op.opened {
		return nil, fmt.Errorf("operator not opened")
	}

	// Build hash table from left relation if not done
	if !op.buildDone {
		err := op.buildHashTable()
		if err != nil {
			return nil, err
		}
		op.buildDone = true
	}

	// Probe hash table with right tuples
	for {
		rightTuple, err := op.right.Next()
		if err != nil {
			return nil, err
		}

		// Get hash key from right tuple (simplified)
		hashKey := op.getHashKey(rightTuple)
		
		// Look up in hash table
		if leftTuples, exists := op.hashTable[hashKey]; exists {
			for _, leftTuple := range leftTuples {
				if op.predicate != nil && !op.predicate.Evaluate(leftTuple, rightTuple) {
					continue
				}

				// Create joined tuple
				joinedTuple := &types.Tuple{
					Data: append(leftTuple.Data, rightTuple.Data...),
				}

				return joinedTuple, nil
			}
		}
	}
}

// buildHashTable builds the hash table from left relation
func (op *HashJoinOperator) buildHashTable() error {
	for {
		leftTuple, err := op.left.Next()
		if err != nil {
			break
		}

		hashKey := op.getHashKey(leftTuple)
		op.hashTable[hashKey] = append(op.hashTable[hashKey], leftTuple)
	}
	return nil
}

// getHashKey gets hash key from tuple (simplified)
func (op *HashJoinOperator) getHashKey(tuple *types.Tuple) string {
	// Simplified hash key generation
	return fmt.Sprintf("%x", tuple.Data[0:4])
}

// Close closes the hash join
func (op *HashJoinOperator) Close() error {
	op.opened = false
	op.left.Close()
	op.right.Close()
	return nil
}

// GetSchema returns the joined schema
func (op *HashJoinOperator) GetSchema() types.Schema {
	leftSchema := op.left.GetSchema()
	rightSchema := op.right.GetSchema()
	
	joinedSchema := types.Schema{
		Columns: append(leftSchema.Columns, rightSchema.Columns...),
	}
	
	return joinedSchema
}

// Predicate represents a filter predicate
type Predicate struct {
	Column   string
	Operator string
	Value    interface{}
}

// Evaluate evaluates the predicate against a tuple
func (p *Predicate) Evaluate(tuple *types.Tuple) bool {
	// Simplified predicate evaluation
	// In a real implementation, this would parse the tuple data
	// and compare against the column value
	return true
}

// JoinPredicate represents a join predicate
type JoinPredicate struct {
	LeftColumn  string
	RightColumn string
	Operator    string
	LeftSchema  types.Schema
	RightSchema types.Schema
}

// Evaluate evaluates the join predicate
func (jp *JoinPredicate) Evaluate(leftTuple, rightTuple *types.Tuple) bool {
	// Extract values from both tuples
	leftValue := jp.extractColumnValue(leftTuple, jp.LeftColumn, jp.LeftSchema)
	rightValue := jp.extractColumnValue(rightTuple, jp.RightColumn, jp.RightSchema)
	
	// Compare values based on operator
	return jp.compareValues(leftValue, rightValue, jp.Operator)
}

// extractColumnValue extracts a column value from a tuple
func (jp *JoinPredicate) extractColumnValue(tuple *types.Tuple, columnName string, schema types.Schema) interface{} {
	// Deserialize tuple data
	tupleData := jp.deserializeTupleData(tuple.Data)
	
	// Return the column value
	return tupleData[columnName]
}

// deserializeTupleData deserializes tuple data into a map
func (jp *JoinPredicate) deserializeTupleData(data []byte) map[string]any {
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
		
		// Try to convert to appropriate type
		if intVal, err := strconv.Atoi(valueStr); err == nil {
			result[key] = intVal
		} else if boolVal, err := strconv.ParseBool(valueStr); err == nil {
			result[key] = boolVal
		} else if floatVal, err := strconv.ParseFloat(valueStr, 64); err == nil {
			result[key] = floatVal
		} else {
			result[key] = valueStr
		}
	}
	
	return result
}

// compareValues compares two values based on the operator
func (jp *JoinPredicate) compareValues(left, right interface{}, operator string) bool {
	if left == nil || right == nil {
		return operator == "IS NULL" || operator == "IS NOT NULL"
	}
	
	// Convert to strings for comparison (simplified)
	leftStr := fmt.Sprintf("%v", left)
	rightStr := fmt.Sprintf("%v", right)
	
	switch operator {
	case "=", "==":
		return leftStr == rightStr
	case "!=", "<>":
		return leftStr != rightStr
	case "<":
		return leftStr < rightStr
	case ">":
		return leftStr > rightStr
	case "<=":
		return leftStr <= rightStr
	case ">=":
		return leftStr >= rightStr
	default:
		return false
	}
}

// ProjectionOperator performs projection
type ProjectionOperator struct {
	child   Operator
	columns []string
	opened  bool
}

// NewProjectionOperator creates a new projection operator
func NewProjectionOperator(child Operator, columns []string) *ProjectionOperator {
	return &ProjectionOperator{
		child:   child,
		columns: columns,
	}
}

// Open opens the projection
func (op *ProjectionOperator) Open() error {
	if op.opened {
		return fmt.Errorf("operator already opened")
	}
	op.opened = true
	return op.child.Open()
}

// Next returns the next projected tuple
func (op *ProjectionOperator) Next() (*types.Tuple, error) {
	if !op.opened {
		return nil, fmt.Errorf("operator not opened")
	}

	tuple, err := op.child.Next()
	if err != nil {
		return nil, err
	}

	// Apply projection (simplified)
	// In a real implementation, this would select only the requested columns
	return tuple, nil
}

// Close closes the projection
func (op *ProjectionOperator) Close() error {
	op.opened = false
	return op.child.Close()
}

// GetSchema returns the projected schema
func (op *ProjectionOperator) GetSchema() types.Schema {
	childSchema := op.child.GetSchema()
	
	// Filter columns based on projection
	var projectedColumns []types.Column
	for _, colName := range op.columns {
		for _, col := range childSchema.Columns {
			if col.Name == colName {
				projectedColumns = append(projectedColumns, col)
				break
			}
		}
	}
	
	return types.Schema{Columns: projectedColumns}
}

// LeftJoinOperator performs left outer join
type LeftJoinOperator struct {
	left       Operator
	right      Operator
	predicate  *JoinPredicate
	leftTuple  *types.Tuple
	rightOpen  bool
	rightEOF   bool
	foundMatch bool
	opened     bool
}

// NewLeftJoinOperator creates a new left join operator
func NewLeftJoinOperator(left, right Operator, predicate *JoinPredicate) *LeftJoinOperator {
	return &LeftJoinOperator{
		left:      left,
		right:     right,
		predicate: predicate,
	}
}

// Open opens the left join
func (op *LeftJoinOperator) Open() error {
	if op.opened {
		return fmt.Errorf("operator already opened")
	}
	op.opened = true
	
	err := op.left.Open()
	if err != nil {
		return err
	}
	
	return nil
}

// Next returns the next joined tuple
func (op *LeftJoinOperator) Next() (*types.Tuple, error) {
	if !op.opened {
		return nil, fmt.Errorf("operator not opened")
	}

	for {
		// If we don't have a left tuple, get one
		if op.leftTuple == nil {
			leftTuple, err := op.left.Next()
			if err != nil {
				return nil, err
			}
			op.leftTuple = leftTuple
			op.foundMatch = false
			op.rightEOF = false
			
			// Open right operator for this left tuple
			if !op.rightOpen {
				err = op.right.Open()
				if err != nil {
					return nil, err
				}
				op.rightOpen = true
			}
		}

		// Try to get a matching right tuple
		if !op.rightEOF {
			rightTuple, err := op.right.Next()
			if err != nil {
				// No more right tuples
				op.rightEOF = true
				op.right.Close()
				op.rightOpen = false
			} else {
				// Check join predicate
				if op.predicate == nil || op.predicate.Evaluate(op.leftTuple, rightTuple) {
					op.foundMatch = true
					
					// Create joined tuple
					joinedTuple := &types.Tuple{
						Data: append(op.leftTuple.Data, rightTuple.Data...),
					}

					return joinedTuple, nil
				}
				continue
			}
		}

		// If we've exhausted right tuples and found no match, emit left tuple with nulls
		if op.rightEOF && !op.foundMatch {
			// Create null tuple for right side
			rightSchema := op.right.GetSchema()
			nullData := op.createNullTuple(rightSchema)
			
			joinedTuple := &types.Tuple{
				Data: append(op.leftTuple.Data, nullData...),
			}
			
			op.leftTuple = nil // Move to next left tuple
			return joinedTuple, nil
		}

		// Move to next left tuple
		op.leftTuple = nil
	}
}

// createNullTuple creates a tuple with null values for all columns
func (op *LeftJoinOperator) createNullTuple(schema types.Schema) []byte {
	// Create a simple null representation
	return []byte("null;")
}

// Close closes the left join
func (op *LeftJoinOperator) Close() error {
	op.opened = false
	op.left.Close()
	if op.rightOpen {
		op.right.Close()
	}
	return nil
}

// GetSchema returns the joined schema
func (op *LeftJoinOperator) GetSchema() types.Schema {
	leftSchema := op.left.GetSchema()
	rightSchema := op.right.GetSchema()
	
	joinedSchema := types.Schema{
		Columns: append(leftSchema.Columns, rightSchema.Columns...),
	}
	
	return joinedSchema
}

// RightJoinOperator performs right outer join
type RightJoinOperator struct {
	left       Operator
	right      Operator
	predicate  *JoinPredicate
	rightTuple *types.Tuple
	leftOpen   bool
	leftEOF    bool
	foundMatch bool
	opened     bool
}

// NewRightJoinOperator creates a new right join operator
func NewRightJoinOperator(left, right Operator, predicate *JoinPredicate) *RightJoinOperator {
	return &RightJoinOperator{
		left:      left,
		right:     right,
		predicate: predicate,
	}
}

// Open opens the right join
func (op *RightJoinOperator) Open() error {
	if op.opened {
		return fmt.Errorf("operator already opened")
	}
	op.opened = true
	
	err := op.right.Open()
	if err != nil {
		return err
	}
	
	return nil
}

// Next returns the next joined tuple
func (op *RightJoinOperator) Next() (*types.Tuple, error) {
	if !op.opened {
		return nil, fmt.Errorf("operator not opened")
	}

	for {
		// If we don't have a right tuple, get one
		if op.rightTuple == nil {
			rightTuple, err := op.right.Next()
			if err != nil {
				return nil, err
			}
			op.rightTuple = rightTuple
			op.foundMatch = false
			op.leftEOF = false
			
			// Open left operator for this right tuple
			if !op.leftOpen {
				err = op.left.Open()
				if err != nil {
					return nil, err
				}
				op.leftOpen = true
			}
		}

		// Try to get a matching left tuple
		if !op.leftEOF {
			leftTuple, err := op.left.Next()
			if err != nil {
				// No more left tuples
				op.leftEOF = true
				op.left.Close()
				op.leftOpen = false
			} else {
				// Check join predicate
				if op.predicate == nil || op.predicate.Evaluate(leftTuple, op.rightTuple) {
					op.foundMatch = true
					
					// Create joined tuple
					joinedTuple := &types.Tuple{
						Data: append(leftTuple.Data, op.rightTuple.Data...),
					}

					return joinedTuple, nil
				}
				continue
			}
		}

		// If we've exhausted left tuples and found no match, emit right tuple with nulls
		if op.leftEOF && !op.foundMatch {
			// Create null tuple for left side
			leftSchema := op.left.GetSchema()
			nullData := op.createNullTuple(leftSchema)
			
			joinedTuple := &types.Tuple{
				Data: append(nullData, op.rightTuple.Data...),
			}
			
			op.rightTuple = nil // Move to next right tuple
			return joinedTuple, nil
		}

		// Move to next right tuple
		op.rightTuple = nil
	}
}

// createNullTuple creates a tuple with null values for all columns
func (op *RightJoinOperator) createNullTuple(schema types.Schema) []byte {
	// Create a simple null representation
	return []byte("null;")
}

// Close closes the right join
func (op *RightJoinOperator) Close() error {
	op.opened = false
	op.right.Close()
	if op.leftOpen {
		op.left.Close()
	}
	return nil
}

// GetSchema returns the joined schema
func (op *RightJoinOperator) GetSchema() types.Schema {
	leftSchema := op.left.GetSchema()
	rightSchema := op.right.GetSchema()
	
	joinedSchema := types.Schema{
		Columns: append(leftSchema.Columns, rightSchema.Columns...),
	}
	
	return joinedSchema
}

// CrossJoinOperator performs cross join (cartesian product)
type CrossJoinOperator struct {
	left       Operator
	right      Operator
	leftTuple  *types.Tuple
	rightOpen  bool
	opened     bool
}

// NewCrossJoinOperator creates a new cross join operator
func NewCrossJoinOperator(left, right Operator) *CrossJoinOperator {
	return &CrossJoinOperator{
		left:  left,
		right: right,
	}
}

// Open opens the cross join
func (op *CrossJoinOperator) Open() error {
	if op.opened {
		return fmt.Errorf("operator already opened")
	}
	op.opened = true
	
	err := op.left.Open()
	if err != nil {
		return err
	}
	
	return nil
}

// Next returns the next joined tuple
func (op *CrossJoinOperator) Next() (*types.Tuple, error) {
	if !op.opened {
		return nil, fmt.Errorf("operator not opened")
	}

	for {
		// If we don't have a left tuple, get one
		if op.leftTuple == nil {
			leftTuple, err := op.left.Next()
			if err != nil {
				return nil, err
			}
			op.leftTuple = leftTuple
			
			// Open right operator for this left tuple
			if !op.rightOpen {
				err = op.right.Open()
				if err != nil {
					return nil, err
				}
				op.rightOpen = true
			}
		}

		// Try to get a right tuple
		rightTuple, err := op.right.Next()
		if err != nil {
			// No more right tuples, close right and get next left
			op.right.Close()
			op.rightOpen = false
			op.leftTuple = nil
			continue
		}

		// Create joined tuple (cartesian product)
		joinedTuple := &types.Tuple{
			Data: append(op.leftTuple.Data, rightTuple.Data...),
		}

		return joinedTuple, nil
	}
}

// Close closes the cross join
func (op *CrossJoinOperator) Close() error {
	op.opened = false
	op.left.Close()
	if op.rightOpen {
		op.right.Close()
	}
	return nil
}

// GetSchema returns the joined schema
func (op *CrossJoinOperator) GetSchema() types.Schema {
	leftSchema := op.left.GetSchema()
	rightSchema := op.right.GetSchema()
	
	joinedSchema := types.Schema{
		Columns: append(leftSchema.Columns, rightSchema.Columns...),
	}
	
	return joinedSchema
}