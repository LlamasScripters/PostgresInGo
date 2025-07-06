package storage

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/esgi-git/postgres-engine/internal/types"
)

const (
	PageSize = 8192 // 8KB pages
)

// DiskManager handles disk I/O operations
type DiskManager struct {
	file *os.File
	mu   sync.RWMutex
}

// NewDiskManager creates a new disk manager
func NewDiskManager(filename string) (*DiskManager, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return &DiskManager{file: file}, nil
}

// ReadPage reads a page from disk
func (dm *DiskManager) ReadPage(pageID uint64) (*types.Page, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	offset := int64(pageID * PageSize)
	data := make([]byte, PageSize)

	n, err := dm.file.ReadAt(data, offset)
	if err != nil && err != io.EOF {
		return nil, err
	}

	// If we read less than expected or got EOF, pad with zeros
	if n < PageSize {
		for i := n; i < PageSize; i++ {
			data[i] = 0
		}
	}

	page := &types.Page{
		ID:   pageID,
		Type: types.DataPage,
		Data: data,
	}

	return page, nil
}

// WritePage writes a page to disk
func (dm *DiskManager) WritePage(page *types.Page) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	offset := int64(page.ID * PageSize)
	_, err := dm.file.WriteAt(page.Data, offset)
	if err != nil {
		return err
	}

	return dm.file.Sync()
}

// Close closes the disk manager
func (dm *DiskManager) Close() error {
	return dm.file.Close()
}

// BufferManager manages pages in memory
type BufferManager struct {
	pool     map[uint64]*types.Page
	capacity int
	mu       sync.RWMutex
}

// NewBufferManager creates a new buffer manager
func NewBufferManager(capacity int) *BufferManager {
	return &BufferManager{
		pool:     make(map[uint64]*types.Page),
		capacity: capacity,
	}
}

// GetPage gets a page from buffer or disk
func (bm *BufferManager) GetPage(pageID uint64, dm *DiskManager) (*types.Page, error) {
	bm.mu.RLock()
	if page, exists := bm.pool[pageID]; exists {
		bm.mu.RUnlock()
		return page, nil
	}
	bm.mu.RUnlock()

	// Page not in buffer, load from disk
	page, err := dm.ReadPage(pageID)
	if err != nil {
		return nil, err
	}

	bm.mu.Lock()
	defer bm.mu.Unlock()

	// Check capacity and evict if necessary
	if len(bm.pool) >= bm.capacity {
		// Simple eviction: remove first page
		for id := range bm.pool {
			delete(bm.pool, id)
			break
		}
	}

	bm.pool[pageID] = page
	return page, nil
}

// FlushPage flushes a page to disk
func (bm *BufferManager) FlushPage(pageID uint64, dm *DiskManager) error {
	bm.mu.RLock()
	page, exists := bm.pool[pageID]
	bm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("page %d not in buffer", pageID)
	}

	return dm.WritePage(page)
}

// StorageManager manages storage operations
type StorageManager struct {
	diskManager   *DiskManager
	bufferManager *BufferManager
	tables        map[string]*types.Table
	views         map[string]*types.View    // Storage for views
	tableTuples   map[string][]*types.Tuple // Simple in-memory storage for now
	nextPageID    uint64
	nextTupleID   uint64
	mu            sync.RWMutex
}

// NewStorageManager creates a new storage manager
func NewStorageManager(dataDir string) (*StorageManager, error) {
	dm, err := NewDiskManager(dataDir + "/data.db")
	if err != nil {
		return nil, err
	}

	bm := NewBufferManager(100) // 100 pages in buffer

	sm := &StorageManager{
		diskManager:   dm,
		bufferManager: bm,
		tables:        make(map[string]*types.Table),
		views:         make(map[string]*types.View),
		tableTuples:   make(map[string][]*types.Tuple),
		nextPageID:    1,
		nextTupleID:   1,
	}

	// Try to load existing metadata (commented out for now)
	// sm.loadMetadata(dataDir)

	// Load views metadata (commented out for now)
	// sm.loadViewsMetadata(dataDir)

	// Load views metadata
	sm.loadViewsMetadata(dataDir)

	return sm, nil
}

// CreateTable creates a new table
func (sm *StorageManager) CreateTable(name string, schema types.Schema) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, exists := sm.tables[name]; exists {
		return fmt.Errorf("table %s already exists", name)
	}

	table := &types.Table{
		ID:      uint64(len(sm.tables) + 1),
		Name:    name,
		Schema:  schema,
		Pages:   []uint64{},
		Indexes: make(map[string]*types.Index),
	}

	sm.tables[name] = table

	// Save metadata after creating table (commented out for now)
	// sm.saveMetadata()

	return nil
}

// InsertTuple inserts a tuple into a table
func (sm *StorageManager) InsertTuple(tableName string, tuple *types.Tuple) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	table, exists := sm.tables[tableName]
	if !exists {
		return fmt.Errorf("table %s not found", tableName)
	}

	// Get or create a page for the tuple
	var pageID uint64
	if len(table.Pages) == 0 {
		pageID = sm.nextPageID
		sm.nextPageID++
		table.Pages = append(table.Pages, pageID)
	} else {
		pageID = table.Pages[len(table.Pages)-1]
	}

	// Get the page from buffer manager
	page, err := sm.bufferManager.GetPage(pageID, sm.diskManager)
	if err != nil {
		return err
	}

	// Find free space in the page
	tupleSize := len(tuple.Data)
	if tupleSize == 0 {
		return fmt.Errorf("empty tuple data")
	}

	// Simple allocation: find first available offset
	offset := sm.findFreeOffset(page, tupleSize)
	if offset == 0 && len(page.Data) > 0 {
		// Page is full, create a new page
		pageID = sm.nextPageID
		sm.nextPageID++
		table.Pages = append(table.Pages, pageID)

		// Create new page
		page = &types.Page{
			ID:   pageID,
			Type: types.DataPage,
			Data: make([]byte, PageSize),
		}
		offset = 0
	}

	// Assign TID
	tuple.TID = types.TupleID{
		PageID: pageID,
		Offset: uint16(offset),
	}

	// Write tuple to page
	if offset+tupleSize > len(page.Data) {
		return fmt.Errorf("tuple too large for page")
	}

	copy(page.Data[offset:offset+tupleSize], tuple.Data)

	// Write page to disk
	err = sm.diskManager.WritePage(page)
	if err != nil {
		return err
	}

	// Also store in memory for quick access
	sm.tableTuples[tableName] = append(sm.tableTuples[tableName], tuple)

	// Update table stats
	table.Stats.TupleCount++

	return nil
}

// SelectTuple retrieves a tuple by TID
func (sm *StorageManager) SelectTuple(tableName string, tid types.TupleID) (*types.Tuple, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	_, exists := sm.tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table %s not found", tableName)
	}

	// First try in-memory cache
	tuples, exists := sm.tableTuples[tableName]
	if exists {
		for _, tuple := range tuples {
			if tuple.TID.PageID == tid.PageID && tuple.TID.Offset == tid.Offset {
				return tuple, nil
			}
		}
	}

	// If not in cache, read from disk
	page, err := sm.bufferManager.GetPage(tid.PageID, sm.diskManager)
	if err != nil {
		return nil, err
	}

	// Extract tuple data from page
	offset := int(tid.Offset)
	if offset >= len(page.Data) {
		return nil, fmt.Errorf("tuple offset out of bounds")
	}

	// Find the end of the tuple data (look for delimiter or end of meaningful data)
	end := offset
	for end < len(page.Data) && page.Data[end] != 0 {
		end++
	}

	if end == offset {
		return nil, fmt.Errorf("tuple not found")
	}

	tuple := &types.Tuple{
		TID:  tid,
		Data: make([]byte, end-offset),
	}
	copy(tuple.Data, page.Data[offset:end])

	return tuple, nil
}

// UpdateTuple updates a tuple
func (sm *StorageManager) UpdateTuple(tableName string, tid types.TupleID, tuple *types.Tuple) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	_, exists := sm.tables[tableName]
	if !exists {
		return fmt.Errorf("table %s not found", tableName)
	}

	// Get the page
	page, err := sm.bufferManager.GetPage(tid.PageID, sm.diskManager)
	if err != nil {
		return err
	}

	// Update tuple data on disk
	offset := int(tid.Offset)
	tupleSize := len(tuple.Data)

	if offset+tupleSize > len(page.Data) {
		return fmt.Errorf("updated tuple too large for page")
	}

	// Clear old data first
	oldEnd := offset
	for oldEnd < len(page.Data) && page.Data[oldEnd] != 0 {
		oldEnd++
	}
	for i := offset; i < oldEnd; i++ {
		page.Data[i] = 0
	}

	// Write new data
	copy(page.Data[offset:offset+tupleSize], tuple.Data)

	// Write page to disk
	err = sm.diskManager.WritePage(page)
	if err != nil {
		return err
	}

	// Update in-memory cache
	tuples, exists := sm.tableTuples[tableName]
	if exists {
		for i, existingTuple := range tuples {
			if existingTuple.TID.PageID == tid.PageID && existingTuple.TID.Offset == tid.Offset {
				tuple.TID = tid // Preserve the TID
				sm.tableTuples[tableName][i] = tuple
				break
			}
		}
	}

	return nil
}

// DeleteTuple deletes a tuple
func (sm *StorageManager) DeleteTuple(tableName string, tid types.TupleID) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	table, exists := sm.tables[tableName]
	if !exists {
		return fmt.Errorf("table %s not found", tableName)
	}

	// Get the page
	page, err := sm.bufferManager.GetPage(tid.PageID, sm.diskManager)
	if err != nil {
		return err
	}

	// Mark tuple as deleted on disk by zeroing it out
	offset := int(tid.Offset)
	end := offset
	for end < len(page.Data) && page.Data[end] != 0 {
		end++
	}

	// Clear the tuple data
	for i := offset; i < end; i++ {
		page.Data[i] = 0
	}

	// Write page to disk
	err = sm.diskManager.WritePage(page)
	if err != nil {
		return err
	}

	// Remove from in-memory cache
	tuples, exists := sm.tableTuples[tableName]
	if exists {
		for i, tuple := range tuples {
			if tuple.TID.PageID == tid.PageID && tuple.TID.Offset == tid.Offset {
				// Remove tuple from slice
				sm.tableTuples[tableName] = append(tuples[:i], tuples[i+1:]...)
				break
			}
		}
	}

	// Update table stats
	table.Stats.TupleCount--

	return nil
}

// GetTable returns a table by name
func (sm *StorageManager) GetTable(name string) (*types.Table, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	table, exists := sm.tables[name]
	if !exists {
		return nil, fmt.Errorf("table %s not found", name)
	}

	return table, nil
}

// GetAllTuples returns all tuples for a table
func (sm *StorageManager) GetAllTuples(tableName string) ([]*types.Tuple, error) {
	sm.mu.Lock() // Use write lock to potentially rebuild cache
	defer sm.mu.Unlock()

	table, exists := sm.tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table %s not found", tableName)
	}

	// Check if we have cached tuples
	tuples, exists := sm.tableTuples[tableName]
	if !exists || len(tuples) == 0 {
		// Rebuild cache from disk
		tuples = []*types.Tuple{}

		for _, pageID := range table.Pages {
			page, err := sm.bufferManager.GetPage(pageID, sm.diskManager)
			if err != nil {
				continue // Skip pages that can't be read
			}

			// Scan through the page to find tuples
			for offset := 0; offset < len(page.Data); {
				// Skip empty space
				if page.Data[offset] == 0 {
					offset++
					continue
				}

				// Find end of tuple
				start := offset
				for offset < len(page.Data) && page.Data[offset] != 0 {
					offset++
				}

				if offset > start {
					// Found a tuple
					tupleData := make([]byte, offset-start)
					copy(tupleData, page.Data[start:offset])

					tuple := &types.Tuple{
						TID: types.TupleID{
							PageID: pageID,
							Offset: uint16(start),
						},
						Data: tupleData,
					}
					tuples = append(tuples, tuple)
				}
			}
		}

		// Cache the tuples
		sm.tableTuples[tableName] = tuples
	}

	// Return a copy to avoid concurrent modification issues
	result := make([]*types.Tuple, len(tuples))
	copy(result, tuples)
	return result, nil
}

// findFreeOffset finds a free offset in a page for a tuple of given size
func (sm *StorageManager) findFreeOffset(page *types.Page, tupleSize int) int {
	// Simple implementation: find first free space
	// In a real implementation, this would use a more sophisticated allocation strategy

	// Scan through the page looking for free space
	for i := 0; i <= len(page.Data)-tupleSize; i += 32 { // Align to 32-byte boundaries
		// Check if this space is free (all zeros)
		free := true
		for j := i; j < i+tupleSize && j < len(page.Data); j++ {
			if page.Data[j] != 0 {
				free = false
				break
			}
		}
		if free {
			return i
		}
	}

	// No free space found
	return len(page.Data)
}

// ==================== View Management ====================

// CreateView stores a view definition
func (sm *StorageManager) CreateView(view *types.View) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, exists := sm.views[view.Name]; exists {
		return fmt.Errorf("view %s already exists", view.Name)
	}

	sm.views[view.Name] = view

	// Save metadata to disk
	return sm.saveViewMetadata(view)
}

// DropView removes a view
func (sm *StorageManager) DropView(name string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, exists := sm.views[name]; !exists {
		return fmt.Errorf("view %s does not exist", name)
	}

	delete(sm.views, name)

	// Remove metadata from disk
	return sm.removeViewMetadata(name)
}

// GetView retrieves a view by name
func (sm *StorageManager) GetView(name string) (*types.View, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	view, exists := sm.views[name]
	if !exists {
		return nil, fmt.Errorf("view %s does not exist", name)
	}

	return view, nil
}

// GetAllViews returns all views
func (sm *StorageManager) GetAllViews() ([]*types.View, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	views := make([]*types.View, 0, len(sm.views))
	for _, view := range sm.views {
		views = append(views, view)
	}

	return views, nil
}

// saveViewMetadata saves view metadata to disk
func (sm *StorageManager) saveViewMetadata(view *types.View) error {
	// Create views directory if it doesn't exist
	viewsDir := filepath.Dir(sm.diskManager.file.Name()) + "/views"
	if err := os.MkdirAll(viewsDir, 0755); err != nil {
		return err
	}

	// Save view metadata as JSON
	viewFile := filepath.Join(viewsDir, view.Name+".json")
	data, err := json.MarshalIndent(view, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(viewFile, data, 0644)
}

// removeViewMetadata removes view metadata from disk
func (sm *StorageManager) removeViewMetadata(name string) error {
	viewsDir := filepath.Dir(sm.diskManager.file.Name()) + "/views"
	viewFile := filepath.Join(viewsDir, name+".json")

	if err := os.Remove(viewFile); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

// loadViewsMetadata loads all view metadata from disk
func (sm *StorageManager) loadViewsMetadata(dataDir string) error {
	viewsDir := filepath.Join(dataDir, "views")

	// Check if views directory exists
	if _, err := os.Stat(viewsDir); os.IsNotExist(err) {
		return nil // No views to load
	}

	entries, err := os.ReadDir(viewsDir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			viewFile := filepath.Join(viewsDir, entry.Name())
			data, err := os.ReadFile(viewFile)
			if err != nil {
				continue // Skip corrupted files
			}

			var view types.View
			if err := json.Unmarshal(data, &view); err != nil {
				continue // Skip corrupted files
			}

			sm.views[view.Name] = &view
		}
	}

	return nil
}

// Close closes the storage manager
func (sm *StorageManager) Close() error {
	// Save metadata before closing (commented out for now)
	// sm.saveMetadata()
	return sm.diskManager.Close()
}

// Constraint validation functions

// ValidatePrimaryKey validates primary key constraints
func (sm *StorageManager) ValidatePrimaryKey(tableName string, data map[string]any) error {
	table, err := sm.GetTable(tableName)
	if err != nil {
		return err
	}

	pkColumns := table.GetPrimaryKeyColumns()
	if len(pkColumns) == 0 {
		return nil // No primary key constraint
	}

	// Check that all primary key columns have values
	for _, col := range pkColumns {
		if value, exists := data[col]; !exists || value == nil {
			return fmt.Errorf("primary key column '%s' cannot be null", col)
		}
	}

	// Check for duplicate primary key values
	return sm.checkPrimaryKeyUniqueness(tableName, pkColumns, data)
}

// checkPrimaryKeyUniqueness verifies primary key values are unique
func (sm *StorageManager) checkPrimaryKeyUniqueness(tableName string, pkColumns []string, data map[string]any) error {
	// Get all existing tuples
	tuples, err := sm.GetAllTuples(tableName)
	if err != nil {
		return err
	}

	// Extract primary key values from new data
	newPKValues := make([]interface{}, len(pkColumns))
	for i, col := range pkColumns {
		newPKValues[i] = data[col]
	}

	// Check against existing tuples
	for _, tuple := range tuples {
		existingData := sm.deserializeTupleData(tuple.Data)
		existingPKValues := make([]interface{}, len(pkColumns))

		for i, col := range pkColumns {
			existingPKValues[i] = existingData[col]
		}

		// Compare primary key values
		if sm.compareValues(newPKValues, existingPKValues) {
			return fmt.Errorf("duplicate primary key value")
		}
	}

	return nil
}

// ValidateForeignKey validates foreign key constraints
func (sm *StorageManager) ValidateForeignKey(tableName string, data map[string]any) error {
	table, err := sm.GetTable(tableName)
	if err != nil {
		return err
	}

	// Check all foreign key constraints
	for _, fk := range table.ForeignKeys {
		if err := sm.validateSingleForeignKey(fk, data); err != nil {
			return err
		}
	}

	return nil
}

// validateSingleForeignKey validates a single foreign key constraint
func (sm *StorageManager) validateSingleForeignKey(fk *types.Constraint, data map[string]any) error {
	// Extract foreign key values
	fkValues := make([]interface{}, len(fk.Columns))
	hasNonNullValue := false

	for i, col := range fk.Columns {
		value, exists := data[col]
		if exists && value != nil {
			hasNonNullValue = true
		}
		fkValues[i] = value
	}

	// If all FK columns are null, constraint is satisfied
	if !hasNonNullValue {
		return nil
	}

	// Check that referenced table exists
	_, err := sm.GetTable(fk.RefTable)
	if err != nil {
		return fmt.Errorf("referenced table '%s' does not exist", fk.RefTable)
	}

	// Get all tuples from referenced table
	refTuples, err := sm.GetAllTuples(fk.RefTable)
	if err != nil {
		return err
	}

	// Check if foreign key values exist in referenced table
	for _, tuple := range refTuples {
		refData := sm.deserializeTupleData(tuple.Data)
		refValues := make([]interface{}, len(fk.RefColumns))

		for i, col := range fk.RefColumns {
			refValues[i] = refData[col]
		}

		if sm.compareValues(fkValues, refValues) {
			return nil // Found matching reference
		}
	}

	return fmt.Errorf("foreign key constraint violation: referenced values not found in table '%s'", fk.RefTable)
}

// ValidateUniqueConstraints validates unique constraints
func (sm *StorageManager) ValidateUniqueConstraints(tableName string, data map[string]any) error {
	table, err := sm.GetTable(tableName)
	if err != nil {
		return err
	}

	// Check unique constraints
	for _, uk := range table.UniqueKeys {
		if err := sm.validateUniqueConstraint(tableName, uk, data); err != nil {
			return err
		}
	}

	// Check unique columns
	for _, col := range table.Schema.Columns {
		if col.IsUnique && !col.IsPrimaryKey { // PK uniqueness already checked
			value, exists := data[col.Name]
			if exists && value != nil {
				if err := sm.checkColumnUniqueness(tableName, col.Name, value); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// validateUniqueConstraint validates a unique constraint
func (sm *StorageManager) validateUniqueConstraint(tableName string, uk *types.Constraint, data map[string]any) error {
	// Extract unique key values
	ukValues := make([]interface{}, len(uk.Columns))
	hasNonNullValue := false

	for i, col := range uk.Columns {
		value, exists := data[col]
		if exists && value != nil {
			hasNonNullValue = true
		}
		ukValues[i] = value
	}

	// If all columns are null, uniqueness constraint is satisfied
	if !hasNonNullValue {
		return nil
	}

	// Get all existing tuples
	tuples, err := sm.GetAllTuples(tableName)
	if err != nil {
		return err
	}

	// Check against existing tuples
	for _, tuple := range tuples {
		existingData := sm.deserializeTupleData(tuple.Data)
		existingValues := make([]interface{}, len(uk.Columns))

		for i, col := range uk.Columns {
			existingValues[i] = existingData[col]
		}

		if sm.compareValues(ukValues, existingValues) {
			return fmt.Errorf("unique constraint violation on columns %v", uk.Columns)
		}
	}

	return nil
}

// checkColumnUniqueness validates column uniqueness
func (sm *StorageManager) checkColumnUniqueness(tableName, columnName string, value interface{}) error {
	tuples, err := sm.GetAllTuples(tableName)
	if err != nil {
		return err
	}

	for _, tuple := range tuples {
		existingData := sm.deserializeTupleData(tuple.Data)
		if existingValue, exists := existingData[columnName]; exists {
			if fmt.Sprintf("%v", value) == fmt.Sprintf("%v", existingValue) {
				return fmt.Errorf("unique constraint violation on column '%s'", columnName)
			}
		}
	}

	return nil
}

// compareValues compares two slices of values for equality
func (sm *StorageManager) compareValues(vals1, vals2 []interface{}) bool {
	if len(vals1) != len(vals2) {
		return false
	}

	for i := range vals1 {
		if fmt.Sprintf("%v", vals1[i]) != fmt.Sprintf("%v", vals2[i]) {
			return false
		}
	}

	return true
}

// deserializeTupleData deserializes tuple data into a map
func (sm *StorageManager) deserializeTupleData(data []byte) map[string]any {
	// This should use the same logic as the engine's DeserializeDataForTesting
	// For now, simplified implementation
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
		} else if boolVal, err := strconv.ParseBool(valueStr); err == nil {
			result[key] = boolVal
		} else {
			result[key] = valueStr
		}
	}

	return result
}

// EnforceReferentialIntegrity enforces referential integrity on delete/update
func (sm *StorageManager) EnforceReferentialIntegrity(tableName string, oldData map[string]any, operation string) error {
	// Find all tables that reference this table
	referencingTables, err := sm.findReferencingTables(tableName)
	if err != nil {
		return err
	}

	for _, refInfo := range referencingTables {
		if err := sm.enforceReferentialAction(refInfo, oldData, operation); err != nil {
			return err
		}
	}

	return nil
}

// ReferencingTableInfo holds information about referencing tables
type ReferencingTableInfo struct {
	TableName  string
	Constraint *types.Constraint
}

// findReferencingTables finds all tables that reference the given table
func (sm *StorageManager) findReferencingTables(tableName string) ([]ReferencingTableInfo, error) {
	var referencingTables []ReferencingTableInfo

	// Iterate through all tables to find foreign key references
	for name, table := range sm.tables {
		for _, fk := range table.ForeignKeys {
			if fk.RefTable == tableName {
				referencingTables = append(referencingTables, ReferencingTableInfo{
					TableName:  name,
					Constraint: fk,
				})
			}
		}
	}

	return referencingTables, nil
}

// enforceReferentialAction enforces referential actions (CASCADE, RESTRICT, etc.)
func (sm *StorageManager) enforceReferentialAction(refInfo ReferencingTableInfo, oldData map[string]any, operation string) error {
	constraint := refInfo.Constraint

	// For RESTRICT, check if any referencing rows exist
	if constraint.OnDeleteRule == "RESTRICT" || constraint.OnUpdateRule == "RESTRICT" {
		return sm.checkRestrictConstraint(refInfo, oldData)
	}

	// For CASCADE, delete/update referencing rows
	if (operation == "DELETE" && constraint.OnDeleteRule == "CASCADE") ||
		(operation == "UPDATE" && constraint.OnUpdateRule == "CASCADE") {
		return sm.cascadeOperation(refInfo, oldData, operation)
	}

	// For SET NULL, set referencing columns to null
	if (operation == "DELETE" && constraint.OnDeleteRule == "SET NULL") ||
		(operation == "UPDATE" && constraint.OnUpdateRule == "SET NULL") {
		return sm.setNullOperation(refInfo, oldData)
	}

	return nil
}

// checkRestrictConstraint checks if RESTRICT constraint would be violated
func (sm *StorageManager) checkRestrictConstraint(refInfo ReferencingTableInfo, oldData map[string]any) error {
	// Get referenced values
	refValues := make([]interface{}, len(refInfo.Constraint.RefColumns))
	for i, col := range refInfo.Constraint.RefColumns {
		refValues[i] = oldData[col]
	}

	// Check if any referencing tuples exist
	tuples, err := sm.GetAllTuples(refInfo.TableName)
	if err != nil {
		return err
	}

	for _, tuple := range tuples {
		tupleData := sm.deserializeTupleData(tuple.Data)
		fkValues := make([]interface{}, len(refInfo.Constraint.Columns))

		for i, col := range refInfo.Constraint.Columns {
			fkValues[i] = tupleData[col]
		}

		if sm.compareValues(fkValues, refValues) {
			return fmt.Errorf("foreign key constraint violation: cannot delete/update, referencing rows exist in table '%s'", refInfo.TableName)
		}
	}

	return nil
}

// cascadeOperation performs cascade delete/update
func (sm *StorageManager) cascadeOperation(refInfo ReferencingTableInfo, oldData map[string]any, operation string) error {
	// This is a simplified implementation
	// In a full implementation, this would recursively handle cascades
	return fmt.Errorf("cascade operations not fully implemented")
}

// setNullOperation sets referencing foreign key columns to null
func (sm *StorageManager) setNullOperation(refInfo ReferencingTableInfo, oldData map[string]any) error {
	// This is a simplified implementation
	// In a full implementation, this would update referencing tuples
	return fmt.Errorf("set null operations not fully implemented")
}
