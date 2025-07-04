package index

import (
	"fmt"
	"sync"

	"github.com/esgi-git/postgres-engine/internal/types"
)

// Comparator defines comparison function for keys
type Comparator func(a, b interface{}) int

// BTreeNode represents a B-Tree node
type BTreeNode struct {
	keys     []interface{}
	values   []types.TupleID // For leaf nodes
	children []*BTreeNode    // For internal nodes
	isLeaf   bool
	parent   *BTreeNode
	next     *BTreeNode // For leaf node linking
}

// BTree represents a B-Tree index
type BTree struct {
	root       *BTreeNode
	degree     int
	keyType    types.DataType
	comparator Comparator
	mu         sync.RWMutex
}

// NewBTree creates a new B-Tree
func NewBTree(degree int, keyType types.DataType) *BTree {
	return &BTree{
		degree:     degree,
		keyType:    keyType,
		comparator: getComparator(keyType),
	}
}

// getComparator returns appropriate comparator for data type
func getComparator(dataType types.DataType) Comparator {
	switch dataType {
	case types.IntType:
		return func(a, b interface{}) int {
			aInt := a.(int)
			bInt := b.(int)
			if aInt < bInt {
				return -1
			} else if aInt > bInt {
				return 1
			}
			return 0
		}
	case types.VarcharType:
		return func(a, b interface{}) int {
			aStr := a.(string)
			bStr := b.(string)
			if aStr < bStr {
				return -1
			} else if aStr > bStr {
				return 1
			}
			return 0
		}
	default:
		return func(a, b interface{}) int { return 0 }
	}
}

// Insert inserts a key-value pair into the B-Tree
func (bt *BTree) Insert(key interface{}, value types.TupleID) error {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	if bt.root == nil {
		bt.root = &BTreeNode{
			keys:   []interface{}{key},
			values: []types.TupleID{value},
			isLeaf: true,
		}
		return nil
	}

	return bt.insertNonFull(bt.root, key, value)
}

// insertNonFull inserts into a non-full node
func (bt *BTree) insertNonFull(node *BTreeNode, key interface{}, value types.TupleID) error {
	i := len(node.keys) - 1

	if node.isLeaf {
		// Insert into leaf node
		node.keys = append(node.keys, nil)
		node.values = append(node.values, types.TupleID{})

		for i >= 0 && bt.comparator(key, node.keys[i]) < 0 {
			node.keys[i+1] = node.keys[i]
			node.values[i+1] = node.values[i]
			i--
		}

		node.keys[i+1] = key
		node.values[i+1] = value
	} else {
		// Find child to insert into
		for i >= 0 && bt.comparator(key, node.keys[i]) < 0 {
			i--
		}
		i++

		// Check if child is full
		if len(node.children[i].keys) == 2*bt.degree-1 {
			bt.splitChild(node, i)
			if bt.comparator(key, node.keys[i]) > 0 {
				i++
			}
		}

		return bt.insertNonFull(node.children[i], key, value)
	}

	return nil
}

// splitChild splits a full child node
func (bt *BTree) splitChild(parent *BTreeNode, index int) {
	fullChild := parent.children[index]
	newChild := &BTreeNode{
		isLeaf: fullChild.isLeaf,
		parent: parent,
	}

	// Move half of the keys to new child
	mid := bt.degree - 1
	newChild.keys = make([]interface{}, len(fullChild.keys[mid+1:]))
	copy(newChild.keys, fullChild.keys[mid+1:])

	if fullChild.isLeaf {
		newChild.values = make([]types.TupleID, len(fullChild.values[mid+1:]))
		copy(newChild.values, fullChild.values[mid+1:])
		fullChild.values = fullChild.values[:mid+1]
	} else {
		newChild.children = make([]*BTreeNode, len(fullChild.children[mid+1:]))
		copy(newChild.children, fullChild.children[mid+1:])
		fullChild.children = fullChild.children[:mid+1]
	}

	// Update parent
	parent.children = append(parent.children[:index+1], append([]*BTreeNode{newChild}, parent.children[index+1:]...)...)
	parent.keys = append(parent.keys[:index], append([]interface{}{fullChild.keys[mid]}, parent.keys[index:]...)...)

	// Truncate full child
	fullChild.keys = fullChild.keys[:mid]
}

// Search searches for a key in the B-Tree
func (bt *BTree) Search(key interface{}) ([]types.TupleID, error) {
	bt.mu.RLock()
	defer bt.mu.RUnlock()

	if bt.root == nil {
		return nil, fmt.Errorf("key not found")
	}

	return bt.searchNode(bt.root, key)
}

// searchNode searches for a key in a node
func (bt *BTree) searchNode(node *BTreeNode, key interface{}) ([]types.TupleID, error) {
	i := 0
	for i < len(node.keys) && bt.comparator(key, node.keys[i]) > 0 {
		i++
	}

	if i < len(node.keys) && bt.comparator(key, node.keys[i]) == 0 {
		if node.isLeaf {
			return []types.TupleID{node.values[i]}, nil
		}
		// For internal nodes, continue search in appropriate child
	}

	if node.isLeaf {
		return nil, fmt.Errorf("key not found")
	}

	return bt.searchNode(node.children[i], key)
}

// Delete removes a key from the B-Tree
func (bt *BTree) Delete(key interface{}) error {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	if bt.root == nil {
		return fmt.Errorf("key not found")
	}

	return bt.deleteFromNode(bt.root, key)
}

// deleteFromNode deletes a key from a node
func (bt *BTree) deleteFromNode(node *BTreeNode, key interface{}) error {
	i := 0
	for i < len(node.keys) && bt.comparator(key, node.keys[i]) > 0 {
		i++
	}

	if i < len(node.keys) && bt.comparator(key, node.keys[i]) == 0 {
		if node.isLeaf {
			// Remove from leaf
			node.keys = append(node.keys[:i], node.keys[i+1:]...)
			node.values = append(node.values[:i], node.values[i+1:]...)
			return nil
		} else {
			// Remove from internal node (simplified)
			return fmt.Errorf("deletion from internal node not implemented")
		}
	}

	if node.isLeaf {
		return fmt.Errorf("key not found")
	}

	return bt.deleteFromNode(node.children[i], key)
}

// IndexCursor represents a cursor for range scans
type IndexCursor struct {
	node     *BTreeNode
	position int
	btree    *BTree
}

// RangeScan performs a range scan on the B-Tree
func (bt *BTree) RangeScan(startKey, endKey interface{}) (*IndexCursor, error) {
	bt.mu.RLock()
	defer bt.mu.RUnlock()

	if bt.root == nil {
		return nil, fmt.Errorf("empty tree")
	}

	node := bt.findLeafNode(startKey)
	position := bt.findKeyPosition(node, startKey)

	return &IndexCursor{
		node:     node,
		position: position,
		btree:    bt,
	}, nil
}

// findLeafNode finds the leaf node for a key
func (bt *BTree) findLeafNode(key interface{}) *BTreeNode {
	node := bt.root
	for !node.isLeaf {
		i := 0
		for i < len(node.keys) && bt.comparator(key, node.keys[i]) > 0 {
			i++
		}
		node = node.children[i]
	}
	return node
}

// findKeyPosition finds the position of a key in a leaf node
func (bt *BTree) findKeyPosition(node *BTreeNode, key interface{}) int {
	i := 0
	for i < len(node.keys) && bt.comparator(key, node.keys[i]) > 0 {
		i++
	}
	return i
}

// Next moves the cursor to the next key-value pair
func (ic *IndexCursor) Next() (interface{}, types.TupleID, error) {
	if ic.node == nil || ic.position >= len(ic.node.keys) {
		return nil, types.TupleID{}, fmt.Errorf("end of cursor")
	}

	key := ic.node.keys[ic.position]
	value := ic.node.values[ic.position]
	ic.position++

	// Move to next node if at end of current node
	if ic.position >= len(ic.node.keys) && ic.node.next != nil {
		ic.node = ic.node.next
		ic.position = 0
	}

	return key, value, nil
}

// IndexManager manages all indexes
type IndexManager struct {
	indexes map[string]*BTree
	mu      sync.RWMutex
}

// NewIndexManager creates a new index manager
func NewIndexManager() *IndexManager {
	return &IndexManager{
		indexes: make(map[string]*BTree),
	}
}

// CreateIndex creates a new index
func (im *IndexManager) CreateIndex(name string, keyType types.DataType) error {
	im.mu.Lock()
	defer im.mu.Unlock()

	if _, exists := im.indexes[name]; exists {
		return fmt.Errorf("index %s already exists", name)
	}

	btree := NewBTree(3, keyType) // Degree 3
	im.indexes[name] = btree

	return nil
}

// GetIndex retrieves an index by name
func (im *IndexManager) GetIndex(name string) (*BTree, error) {
	im.mu.RLock()
	defer im.mu.RUnlock()

	btree, exists := im.indexes[name]
	if !exists {
		return nil, fmt.Errorf("index %s not found", name)
	}

	return btree, nil
}

// DropIndex removes an index
func (im *IndexManager) DropIndex(name string) error {
	im.mu.Lock()
	defer im.mu.Unlock()

	if _, exists := im.indexes[name]; !exists {
		return fmt.Errorf("index %s not found", name)
	}

	delete(im.indexes, name)
	return nil
}