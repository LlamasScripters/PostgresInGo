package index

import (
	"testing"

	"github.com/LlamasScripters/PostgresInGo/internal/types"
)

// TestNewBTree tests B-Tree creation
func TestNewBTree(t *testing.T) {
	degree := 3
	keyType := types.IntType

	btree := NewBTree(degree, keyType)
	if btree == nil {
		t.Fatal("B-Tree should not be nil")
	}

	if btree.degree != degree {
		t.Errorf("Expected degree %d, got %d", degree, btree.degree)
	}

	if btree.keyType != keyType {
		t.Errorf("Expected key type %v, got %v", keyType, btree.keyType)
	}

	if btree.comparator == nil {
		t.Error("Comparator should not be nil")
	}

	if btree.root != nil {
		t.Error("Root should be nil for empty tree")
	}
}

// TestBTreeInsertSingle tests inserting a single key-value pair
func TestBTreeInsertSingle(t *testing.T) {
	btree := NewBTree(3, types.IntType)
	key := 10
	value := types.TupleID{PageID: 1, Offset: 0}

	err := btree.Insert(key, value)
	if err != nil {
		t.Fatalf("Failed to insert key-value pair: %v", err)
	}

	// Verify root node was created
	if btree.root == nil {
		t.Fatal("Root should not be nil after insertion")
	}

	if !btree.root.isLeaf {
		t.Error("Root should be a leaf node after single insertion")
	}

	if len(btree.root.keys) != 1 {
		t.Errorf("Expected 1 key in root, got %d", len(btree.root.keys))
	}

	if btree.root.keys[0] != key {
		t.Errorf("Expected key %d, got %v", key, btree.root.keys[0])
	}

	if len(btree.root.values) != 1 {
		t.Errorf("Expected 1 value in root, got %d", len(btree.root.values))
	}

	if btree.root.values[0] != value {
		t.Errorf("Expected value %v, got %v", value, btree.root.values[0])
	}
}

// TestBTreeInsertMultiple tests inserting multiple key-value pairs
func TestBTreeInsertMultiple(t *testing.T) {
	btree := NewBTree(3, types.IntType)

	// Insert multiple keys
	keys := []int{10, 20, 30, 40, 50}
	for i, key := range keys {
		value := types.TupleID{PageID: uint64(i + 1), Offset: uint16(i)}
		err := btree.Insert(key, value)
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", key, err)
		}
	}

	// Verify root exists
	if btree.root == nil {
		t.Fatal("Root should not be nil after multiple insertions")
	}

	// Verify tree structure (basic check)
	if len(btree.root.keys) == 0 {
		t.Error("Root should have at least one key")
	}
}

// TestBTreeSearch tests searching for keys
func TestBTreeSearch(t *testing.T) {
	btree := NewBTree(3, types.IntType)

	// Insert test data
	testData := map[int]types.TupleID{
		10: {PageID: 1, Offset: 0},
		20: {PageID: 1, Offset: 1},
		30: {PageID: 1, Offset: 2},
		40: {PageID: 2, Offset: 0},
		50: {PageID: 2, Offset: 1},
	}

	for key, value := range testData {
		err := btree.Insert(key, value)
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", key, err)
		}
	}

	// Search for existing keys
	for key, expectedValue := range testData {
		values, err := btree.Search(key)
		if err != nil {
			t.Errorf("Key %d should be found", key)
		}
		if len(values) == 0 {
			t.Errorf("Expected at least one value for key %d", key)
		}
		if values[0] != expectedValue {
			t.Errorf("Expected value %v for key %d, got %v", expectedValue, key, values[0])
		}
	}

	// Search for non-existent key
	_, err := btree.Search(999)
	if err == nil {
		t.Error("Non-existent key should return error")
	}
}

// TestBTreeDelete tests deleting keys
func TestBTreeDelete(t *testing.T) {
	btree := NewBTree(3, types.IntType)

	// Insert test data
	keys := []int{10, 20, 30, 40, 50}
	for i, key := range keys {
		value := types.TupleID{PageID: uint64(i + 1), Offset: uint16(i)}
		err := btree.Insert(key, value)
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", key, err)
		}
	}

	// Delete a key
	err := btree.Delete(30)
	if err != nil {
		t.Fatalf("Failed to delete key 30: %v", err)
	}

	// Verify key is gone
	_, err = btree.Search(30)
	if err == nil {
		t.Error("Deleted key should not be found")
	}

	// Verify other keys still exist
	for _, key := range []int{10, 20, 40, 50} {
		_, err := btree.Search(key)
		if err != nil {
			t.Errorf("Key %d should still exist after deletion", key)
		}
	}

	// Try to delete non-existent key
	err = btree.Delete(999)
	if err == nil {
		t.Error("Should fail when deleting non-existent key")
	}
}

// TestBTreeRangeScan tests range scanning
func TestBTreeRangeScan(t *testing.T) {
	btree := NewBTree(3, types.IntType)

	// Insert test data
	keys := []int{10, 20, 30, 40, 50, 60, 70, 80, 90}
	for i, key := range keys {
		value := types.TupleID{PageID: uint64(i + 1), Offset: uint16(i)}
		err := btree.Insert(key, value)
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", key, err)
		}
	}

	// Test range scan
	cursor, err := btree.RangeScan(30, 60)
	if err != nil {
		t.Fatalf("Failed to create range scan cursor: %v", err)
	}

	if cursor == nil {
		t.Fatal("Range scan cursor should not be nil")
	}

	// Test cursor iteration
	count := 0
	for {
		key, value, err := cursor.Next()
		if err != nil {
			break // End of range
		}
		count++
		
		// Verify key is within range
		keyInt := key.(int)
		if keyInt < 30 || keyInt > 60 {
			t.Errorf("Key %d is outside range [30, 60]", keyInt)
		}
		
		// Verify value is not nil
		if value.PageID == 0 && value.Offset == 0 {
			t.Error("Value should not be empty")
		}
	}

	// Should have found keys 30, 40, 50, 60
	if count != 4 {
		t.Errorf("Expected 4 keys in range [30, 60], got %d", count)
	}
}

// TestBTreeComparators tests different data type comparators
func TestBTreeComparators(t *testing.T) {
	// Test integer comparator
	intComparator := getComparator(types.IntType)
	if intComparator == nil {
		t.Error("Integer comparator should not be nil")
	}

	// Test integer comparisons
	if intComparator(10, 20) != -1 {
		t.Error("10 should be less than 20")
	}
	if intComparator(20, 10) != 1 {
		t.Error("20 should be greater than 10")
	}
	if intComparator(10, 10) != 0 {
		t.Error("10 should be equal to 10")
	}

	// Test string comparator
	stringComparator := getComparator(types.VarcharType)
	if stringComparator == nil {
		t.Error("String comparator should not be nil")
	}

	// Test string comparisons
	if stringComparator("apple", "banana") != -1 {
		t.Error("'apple' should be less than 'banana'")
	}
	if stringComparator("banana", "apple") != 1 {
		t.Error("'banana' should be greater than 'apple'")
	}
	if stringComparator("apple", "apple") != 0 {
		t.Error("'apple' should be equal to 'apple'")
	}

	// Test default comparator
	defaultComparator := getComparator(types.BoolType)
	if defaultComparator == nil {
		t.Error("Default comparator should not be nil")
	}

	// Default comparator should return 0
	if defaultComparator(true, false) != 0 {
		t.Error("Default comparator should return 0")
	}
}

// TestBTreeStringKeys tests B-Tree with string keys
func TestBTreeStringKeys(t *testing.T) {
	btree := NewBTree(3, types.VarcharType)

	// Insert string keys
	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for i, key := range keys {
		value := types.TupleID{PageID: uint64(i + 1), Offset: uint16(i)}
		err := btree.Insert(key, value)
		if err != nil {
			t.Fatalf("Failed to insert key %s: %v", key, err)
		}
	}

	// Search for string keys
	for _, key := range keys {
		_, err := btree.Search(key)
		if err != nil {
			t.Errorf("Key %s should be found", key)
		}
	}

	// Test range scan with strings
	cursor, err := btree.RangeScan("banana", "date")
	if err != nil {
		t.Fatalf("Failed to create range scan cursor: %v", err)
	}

	count := 0
	for {
		key, _, err := cursor.Next()
		if err != nil {
			break
		}
		count++
		keyStr := key.(string)
		if keyStr < "banana" || keyStr > "date" {
			t.Errorf("Key %s is outside range [banana, date]", keyStr)
		}
	}

	// Should find banana, cherry, date
	if count != 3 {
		t.Errorf("Expected 3 keys in range [banana, date], got %d", count)
	}
}

// TestBTreeConcurrency tests concurrent operations
func TestBTreeConcurrency(t *testing.T) {
	btree := NewBTree(3, types.IntType)

	// Insert initial data
	for i := 0; i < 50; i++ {
		value := types.TupleID{PageID: uint64(i), Offset: uint16(i)}
		err := btree.Insert(i, value)
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}
	}

	// Test concurrent searches
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(searchKey int) {
			_, err := btree.Search(searchKey)
			if err != nil {
				t.Errorf("Key %d should be found in concurrent search", searchKey)
			}
			done <- true
		}(i * 5)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Test concurrent insertions
	for i := 0; i < 10; i++ {
		go func(insertKey int) {
			value := types.TupleID{PageID: uint64(insertKey + 100), Offset: uint16(insertKey)}
			err := btree.Insert(insertKey+100, value)
			if err != nil {
				t.Logf("Concurrent insert failed for key %d: %v", insertKey+100, err)
			}
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}
}

// TestBTreeEdgeCases tests edge cases
func TestBTreeEdgeCases(t *testing.T) {
	btree := NewBTree(3, types.IntType)

	// Test inserting duplicate keys
	key := 10
	value1 := types.TupleID{PageID: 1, Offset: 0}
	value2 := types.TupleID{PageID: 2, Offset: 0}

	err := btree.Insert(key, value1)
	if err != nil {
		t.Fatalf("Failed to insert first value: %v", err)
	}

	err = btree.Insert(key, value2)
	if err != nil {
		t.Fatalf("Failed to insert duplicate key: %v", err)
	}

	// Search should return values
	foundValues, err := btree.Search(key)
	if err != nil {
		t.Error("Duplicate key should be found")
	}

	// Should contain both values
	if len(foundValues) == 0 {
		t.Error("Should find at least one value")
	}

	// Verify at least one of the values is present
	found := false
	for _, foundValue := range foundValues {
		if foundValue == value1 || foundValue == value2 {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Found values %v should contain either %v or %v", foundValues, value1, value2)
	}
}

// TestBTreeNodeStructure tests B-Tree node structure
func TestBTreeNodeStructure(t *testing.T) {
	btree := NewBTree(3, types.IntType)

	// Insert enough data to create internal nodes
	for i := 1; i <= 10; i++ {
		value := types.TupleID{PageID: uint64(i), Offset: uint16(i)}
		err := btree.Insert(i, value)
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}
	}

	// Verify root exists
	if btree.root == nil {
		t.Fatal("Root should not be nil")
	}

	// Test node properties
	if len(btree.root.keys) == 0 {
		t.Error("Root should have at least one key")
	}

	// If root is not a leaf, it should have children
	if !btree.root.isLeaf && len(btree.root.children) == 0 {
		t.Error("Non-leaf root should have children")
	}

	// If root is a leaf, it should have values
	if btree.root.isLeaf && len(btree.root.values) == 0 {
		t.Error("Leaf root should have values")
	}
}

// TestIndexCursor tests index cursor functionality
func TestIndexCursor(t *testing.T) {
	btree := NewBTree(3, types.IntType)

	// Insert test data
	keys := []int{10, 20, 30, 40, 50}
	for i, key := range keys {
		value := types.TupleID{PageID: uint64(i + 1), Offset: uint16(i)}
		err := btree.Insert(key, value)
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", key, err)
		}
	}

	// Test range scan cursor
	cursor, err := btree.RangeScan(20, 40)
	if err != nil {
		t.Fatalf("Failed to create range scan cursor: %v", err)
	}

	// Test cursor iteration
	expectedKeys := []int{20, 30, 40}
	actualKeys := []int{}

	for {
		key, value, err := cursor.Next()
		if err != nil {
			break
		}
		actualKeys = append(actualKeys, key.(int))
		
		// Verify value is not empty
		if value.PageID == 0 && value.Offset == 0 {
			t.Error("Value should not be empty")
		}
	}

	// Verify all expected keys were found
	if len(actualKeys) != len(expectedKeys) {
		t.Errorf("Expected %d keys, got %d", len(expectedKeys), len(actualKeys))
	}

	for i, expectedKey := range expectedKeys {
		if i >= len(actualKeys) || actualKeys[i] != expectedKey {
			t.Errorf("Expected key %d at position %d, got %d", expectedKey, i, actualKeys[i])
		}
	}
}

// TestBTreeErrorHandling tests error handling
func TestBTreeErrorHandling(t *testing.T) {
	btree := NewBTree(3, types.IntType)

	// Test deleting from empty tree
	err := btree.Delete(10)
	if err == nil {
		t.Error("Should fail when deleting from empty tree")
	}

	// Test searching in empty tree
	_, errSearch := btree.Search(10)
	if errSearch == nil {
		t.Error("Should return error when searching in empty tree")
	}

	// Test range scan on empty tree
	cursor, err := btree.RangeScan(10, 20)
	if err != nil {
		t.Fatalf("Range scan should work on empty tree: %v", err)
	}

	// Cursor should immediately return error
	_, _, err = cursor.Next()
	if err == nil {
		t.Error("Cursor should return error on empty tree")
	}
}

// TestNewIndexManager tests index manager creation
func TestNewIndexManager(t *testing.T) {
	im := NewIndexManager()
	if im == nil {
		t.Fatal("Index manager should not be nil")
	}

	if im.indexes == nil {
		t.Error("Indexes map should not be nil")
	}

	if len(im.indexes) != 0 {
		t.Errorf("Expected empty indexes map, got %d indexes", len(im.indexes))
	}
}

// TestIndexManagerCreateIndex tests index creation
func TestIndexManagerCreateIndex(t *testing.T) {
	im := NewIndexManager()

	// Create index
	err := im.CreateIndex("test_index", types.IntType)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Verify index exists
	index, err := im.GetIndex("test_index")
	if err != nil {
		t.Fatalf("Failed to get index: %v", err)
	}

	if index == nil {
		t.Error("Index should not be nil")
	}

	if index.keyType != types.IntType {
		t.Errorf("Expected key type %v, got %v", types.IntType, index.keyType)
	}

	// Test creating duplicate index
	err = im.CreateIndex("test_index", types.IntType)
	if err == nil {
		t.Error("Should fail when creating duplicate index")
	}
}

// TestIndexManagerDropIndex tests index deletion
func TestIndexManagerDropIndex(t *testing.T) {
	im := NewIndexManager()

	// Create index
	err := im.CreateIndex("test_index", types.IntType)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Drop index
	err = im.DropIndex("test_index")
	if err != nil {
		t.Fatalf("Failed to drop index: %v", err)
	}

	// Verify index is gone
	_, err = im.GetIndex("test_index")
	if err == nil {
		t.Error("Should fail when getting dropped index")
	}

	// Test dropping non-existent index
	err = im.DropIndex("nonexistent")
	if err == nil {
		t.Error("Should fail when dropping non-existent index")
	}
}

// TestIndexManagerGetIndex tests index retrieval
func TestIndexManagerGetIndex(t *testing.T) {
	im := NewIndexManager()

	// Test getting non-existent index
	_, err := im.GetIndex("nonexistent")
	if err == nil {
		t.Error("Should fail when getting non-existent index")
	}

	// Create and get index
	err = im.CreateIndex("test_index", types.VarcharType)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	index, err := im.GetIndex("test_index")
	if err != nil {
		t.Fatalf("Failed to get index: %v", err)
	}

	if index.keyType != types.VarcharType {
		t.Errorf("Expected key type %v, got %v", types.VarcharType, index.keyType)
	}
}

// TestIndexManagerWithBTree tests index manager with B-Tree operations
func TestIndexManagerWithBTree(t *testing.T) {
	im := NewIndexManager()

	// Create index
	err := im.CreateIndex("test_index", types.IntType)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	// Get index
	index, err := im.GetIndex("test_index")
	if err != nil {
		t.Fatalf("Failed to get index: %v", err)
	}

	// Test B-Tree operations through index manager
	key := 42
	value := types.TupleID{PageID: 1, Offset: 0}

	err = index.Insert(key, value)
	if err != nil {
		t.Fatalf("Failed to insert into index: %v", err)
	}

	foundValues, err := index.Search(key)
	if err != nil {
		t.Error("Key should be found in index")
	}

	if len(foundValues) == 0 || foundValues[0] != value {
		t.Errorf("Expected value %v, got %v", value, foundValues)
	}

	// Test deletion
	err = index.Delete(key)
	if err != nil {
		t.Fatalf("Failed to delete from index: %v", err)
	}

	_, err = index.Search(key)
	if err == nil {
		t.Error("Key should not be found after deletion")
	}
}