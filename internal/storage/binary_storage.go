package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"unsafe"

	"github.com/LlamasScripters/PostgresInGo/internal/types"
)

// Cache line size for modern CPUs (64 bytes)
const (
	CacheLineSize    = 64
	BinaryMagic      = 0x504754524553 // "PGTRES" in hex
	MetadataVersion  = 1
	TupleBinaryMagic = 0x4254 // "BT" - Binary Tuple magic number

	// Cache alignment masks and helpers
	CacheLineMask               = CacheLineSize - 1
	CacheAlignedTupleHeaderSize = 64 // Full cache line for optimal performance
)

// BinaryStorageManager implements cache-aligned binary storage
type BinaryStorageManager struct {
	*StorageManager
	metadataFilePath string
	binaryMode       bool
	schemaHashCache  map[string]uint32 // Cache schema hashes by table name

	// Memory pools for performance optimization
	bufferPool     *sync.Pool // Reusable byte buffers
	byteSlicePool  *sync.Pool // Reusable byte slices
	valueArrayPool *sync.Pool // Reusable value arrays
	bitmapPool     *sync.Pool // Reusable null bitmaps
}

// BinaryMetadataHeader is cache-aligned metadata header (64 bytes)
type BinaryMetadataHeader struct {
	Magic       uint64  // 8 bytes - Magic number for format validation
	Version     uint32  // 4 bytes - Format version
	TableCount  uint32  // 4 bytes - Number of tables
	NextPageID  uint64  // 8 bytes - Next page ID
	NextTupleID uint64  // 8 bytes - Next tuple ID
	Reserved1   uint64  // 8 bytes - Reserved for future use
	Reserved2   uint64  // 8 bytes - Reserved for future use
	Reserved3   uint64  // 8 bytes - Reserved for future use
	Checksum    uint32  // 4 bytes - Header checksum
	Padding     [4]byte // 4 bytes - Padding to reach 64 bytes
}

// BinaryTableHeader represents a table in binary format (cache-aligned to 64 bytes)
type BinaryTableHeader struct {
	TableID         uint64  // 8 bytes - Table ID
	NameLength      uint32  // 4 bytes - Length of table name
	ColumnCount     uint32  // 4 bytes - Number of columns
	ConstraintCount uint32  // 4 bytes - Number of constraints
	IndexCount      uint32  // 4 bytes - Number of indexes
	PageCount       uint32  // 4 bytes - Number of pages
	TupleCount      uint64  // 8 bytes - Total tuple count
	TableSize       uint64  // 8 bytes - Size in bytes
	Reserved1       uint64  // 8 bytes - Reserved for future use
	Reserved2       uint64  // 8 bytes - Reserved for future use
	Checksum        uint32  // 4 bytes - Header checksum
	Padding         [4]byte // 4 bytes - Padding to reach 64 bytes
}

// BinaryColumnInfo represents column metadata (cache-aligned to 32 bytes)
type BinaryColumnInfo struct {
	NameOffset    uint32         // 4 bytes - Offset to column name in string table
	DataType      types.DataType // 4 bytes - Column data type
	Size          uint32         // 4 bytes - Column size
	Flags         uint32         // 4 bytes - Flags (nullable, primary key, unique, auto increment)
	DefaultOffset uint32         // 4 bytes - Offset to default value in string table
	Reserved      uint32         // 4 bytes - Reserved for future use
	Padding       [8]byte        // 8 bytes - Padding to reach 32 bytes
}

// Column flags
const (
	FlagNullable      uint32 = 1 << 0
	FlagPrimaryKey    uint32 = 1 << 1
	FlagUnique        uint32 = 1 << 2
	FlagAutoIncrement uint32 = 1 << 3
)

// BinaryConstraintInfo represents constraint metadata (cache-aligned to 64 bytes)
type BinaryConstraintInfo struct {
	NameOffset      uint32               // 4 bytes - Offset to constraint name
	Type            types.ConstraintType // 4 bytes - Constraint type
	ColumnCount     uint32               // 4 bytes - Number of columns in constraint
	RefTableOffset  uint32               // 4 bytes - Offset to referenced table name
	RefColumnCount  uint32               // 4 bytes - Number of referenced columns
	OnDeleteRule    uint32               // 4 bytes - On delete rule
	OnUpdateRule    uint32               // 4 bytes - On update rule
	CheckExprOffset uint32               // 4 bytes - Offset to check expression
	Reserved1       uint64               // 8 bytes - Reserved
	Reserved2       uint64               // 8 bytes - Reserved
	Reserved3       uint64               // 8 bytes - Reserved
	Checksum        uint32               // 4 bytes - Constraint checksum
	Padding         [4]byte              // 4 bytes - Padding to 64 bytes
}

// BinaryTupleHeader represents tuple metadata (cache-aligned to 32 bytes)
type BinaryTupleHeader struct {
	TupleID     types.TupleID // 10 bytes (8 + 2) - Tuple ID
	DataSize    uint32        // 4 bytes - Size of tuple data
	ColumnCount uint32        // 4 bytes - Number of columns
	Flags       uint32        // 4 bytes - Tuple flags (deleted, updated, etc.)
	Timestamp   uint64        // 8 bytes - Timestamp
	Checksum    uint32        // 4 bytes - Data checksum
	Padding     [6]byte       // 6 bytes - Padding to reach 32 bytes
}

// Tuple flags
const (
	TupleFlagDeleted uint32 = 1 << 0
	TupleFlagUpdated uint32 = 1 << 1
)

// NewBinaryStorageManager creates a binary storage manager
func NewBinaryStorageManager(dataDir string) (*BinaryStorageManager, error) {
	// Create base storage manager
	baseStorage, err := NewStorageManager(dataDir)
	if err != nil {
		return nil, err
	}

	bsm := &BinaryStorageManager{
		StorageManager:   baseStorage,
		metadataFilePath: filepath.Join(dataDir, "metadata.bin"),
		binaryMode:       true,
		schemaHashCache:  make(map[string]uint32),

		// Initialize memory pools for high-performance operations
		bufferPool: &sync.Pool{
			New: func() interface{} {
				// Pre-allocate buffer with typical tuple size
				return bytes.NewBuffer(make([]byte, 0, 512))
			},
		},
		byteSlicePool: &sync.Pool{
			New: func() interface{} {
				// Pre-allocate 8-byte slice for numeric values
				return make([]byte, 8)
			},
		},
		valueArrayPool: &sync.Pool{
			New: func() interface{} {
				// Pre-allocate array for typical column count
				return make([]any, 0, 32)
			},
		},
		bitmapPool: &sync.Pool{
			New: func() interface{} {
				// Pre-allocate bitmap for typical column count (32 columns = 4 bytes)
				return make([]byte, 4)
			},
		},
	}

	// Try to load existing binary metadata
	err = bsm.loadBinaryMetadata()
	if err != nil {
		// If binary metadata doesn't exist, try to migrate from JSON
		if os.IsNotExist(err) {
			err = bsm.migrateFromJSON()
			if err != nil {
				// If no JSON exists either, start fresh
				return bsm, nil
			}
		} else {
			return nil, err
		}
	}

	return bsm, nil
}

// loadBinaryMetadata loads metadata from binary format
func (bsm *BinaryStorageManager) loadBinaryMetadata() error {
	file, err := os.Open(bsm.metadataFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Read and validate header
	var header BinaryMetadataHeader
	err = binary.Read(file, binary.LittleEndian, &header)
	if err != nil {
		return fmt.Errorf("failed to read metadata header: %w", err)
	}

	if header.Magic != BinaryMagic {
		return fmt.Errorf("invalid binary metadata format")
	}

	if header.Version != MetadataVersion {
		return fmt.Errorf("unsupported metadata version: %d", header.Version)
	}

	// Validate header checksum
	headerBytes := (*[unsafe.Sizeof(header)]byte)(unsafe.Pointer(&header))[:unsafe.Sizeof(header)-8] // Exclude checksum and padding
	calculatedChecksum := crc32.ChecksumIEEE(headerBytes)
	if calculatedChecksum != header.Checksum {
		return fmt.Errorf("metadata header checksum mismatch")
	}

	// Update storage manager state
	bsm.nextPageID = header.NextPageID
	bsm.nextTupleID = header.NextTupleID
	bsm.tables = make(map[string]*types.Table)

	// Read table data
	for i := uint32(0); i < header.TableCount; i++ {
		table, err := bsm.readBinaryTable(file)
		if err != nil {
			return fmt.Errorf("failed to read table %d: %w", i, err)
		}
		bsm.tables[table.Name] = table
		bsm.tableTuples[table.Name] = []*types.Tuple{}
	}

	return nil
}

// readBinaryTable reads a single table from binary format
func (bsm *BinaryStorageManager) readBinaryTable(file *os.File) (*types.Table, error) {
	var tableHeader BinaryTableHeader
	err := binary.Read(file, binary.LittleEndian, &tableHeader)
	if err != nil {
		return nil, err
	}

	// Read table name
	nameBytes := make([]byte, tableHeader.NameLength)
	_, err = file.Read(nameBytes)
	if err != nil {
		return nil, err
	}
	tableName := string(nameBytes)

	// Create table structure
	table := &types.Table{
		ID:      tableHeader.TableID,
		Name:    tableName,
		Pages:   make([]uint64, tableHeader.PageCount),
		Indexes: make(map[string]*types.Index),
		Stats: types.TableStats{
			TupleCount: int64(tableHeader.TupleCount),
			PageCount:  int64(tableHeader.PageCount),
			Size:       int64(tableHeader.TableSize),
		},
	}

	// Read page IDs
	for i := uint32(0); i < tableHeader.PageCount; i++ {
		var pageID uint64
		err = binary.Read(file, binary.LittleEndian, &pageID)
		if err != nil {
			return nil, err
		}
		table.Pages[i] = pageID
	}

	// Read columns
	columns := make([]types.Column, tableHeader.ColumnCount)
	columnInfos := make([]BinaryColumnInfo, tableHeader.ColumnCount)

	// Read column headers
	for i := uint32(0); i < tableHeader.ColumnCount; i++ {
		err = binary.Read(file, binary.LittleEndian, &columnInfos[i])
		if err != nil {
			return nil, err
		}
	}

	// Read string table for column names and defaults
	stringTable, err := bsm.readStringTable(file)
	if err != nil {
		return nil, err
	}

	// Populate column data
	for i, colInfo := range columnInfos {
		columns[i] = types.Column{
			Name:          bsm.getStringFromTable(stringTable, colInfo.NameOffset),
			Type:          colInfo.DataType,
			Size:          int(colInfo.Size),
			Nullable:      (colInfo.Flags & FlagNullable) != 0,
			IsPrimaryKey:  (colInfo.Flags & FlagPrimaryKey) != 0,
			IsUnique:      (colInfo.Flags & FlagUnique) != 0,
			AutoIncrement: (colInfo.Flags & FlagAutoIncrement) != 0,
		}

		if colInfo.DefaultOffset > 0 {
			columns[i].Default = bsm.getStringFromTable(stringTable, colInfo.DefaultOffset)
		}
	}

	table.Schema.Columns = columns

	// Read constraints
	constraints := make([]types.Constraint, tableHeader.ConstraintCount)
	for i := uint32(0); i < tableHeader.ConstraintCount; i++ {
		constraint, err := bsm.readBinaryConstraint(file, stringTable)
		if err != nil {
			return nil, err
		}
		constraints[i] = *constraint
	}
	table.Schema.Constraints = constraints

	// Assign constraints to table fields
	bsm.assignConstraintsToTable(table, constraints)

	return table, nil
}

// readStringTable reads a string table from binary format
func (bsm *BinaryStorageManager) readStringTable(file *os.File) ([]byte, error) {
	var tableSize uint32
	err := binary.Read(file, binary.LittleEndian, &tableSize)
	if err != nil {
		return nil, err
	}

	stringTable := make([]byte, tableSize)
	_, err = file.Read(stringTable)
	if err != nil {
		return nil, err
	}

	return stringTable, nil
}

// getStringFromTable extracts a null-terminated string from string table
func (bsm *BinaryStorageManager) getStringFromTable(stringTable []byte, offset uint32) string {
	if offset >= uint32(len(stringTable)) {
		return ""
	}

	start := int(offset)
	end := start
	for end < len(stringTable) && stringTable[end] != 0 {
		end++
	}

	return string(stringTable[start:end])
}

// readBinaryConstraint reads a constraint from binary format
func (bsm *BinaryStorageManager) readBinaryConstraint(file *os.File, stringTable []byte) (*types.Constraint, error) {
	var constraintInfo BinaryConstraintInfo
	err := binary.Read(file, binary.LittleEndian, &constraintInfo)
	if err != nil {
		return nil, err
	}

	constraint := &types.Constraint{
		Name: bsm.getStringFromTable(stringTable, constraintInfo.NameOffset),
		Type: constraintInfo.Type,
	}

	// Read column names
	constraint.Columns = make([]string, constraintInfo.ColumnCount)
	for i := uint32(0); i < constraintInfo.ColumnCount; i++ {
		var nameOffset uint32
		err = binary.Read(file, binary.LittleEndian, &nameOffset)
		if err != nil {
			return nil, err
		}
		constraint.Columns[i] = bsm.getStringFromTable(stringTable, nameOffset)
	}

	// Read referenced columns if foreign key
	if constraintInfo.Type == types.ForeignKeyConstraint {
		constraint.RefTable = bsm.getStringFromTable(stringTable, constraintInfo.RefTableOffset)
		constraint.RefColumns = make([]string, constraintInfo.RefColumnCount)

		for i := uint32(0); i < constraintInfo.RefColumnCount; i++ {
			var nameOffset uint32
			err = binary.Read(file, binary.LittleEndian, &nameOffset)
			if err != nil {
				return nil, err
			}
			constraint.RefColumns[i] = bsm.getStringFromTable(stringTable, nameOffset)
		}

		// Convert rule constants to strings
		constraint.OnDeleteRule = bsm.convertRuleToString(constraintInfo.OnDeleteRule)
		constraint.OnUpdateRule = bsm.convertRuleToString(constraintInfo.OnUpdateRule)
	}

	// Read check expression if check constraint
	if constraintInfo.Type == types.CheckConstraint && constraintInfo.CheckExprOffset > 0 {
		constraint.CheckExpr = bsm.getStringFromTable(stringTable, constraintInfo.CheckExprOffset)
	}

	return constraint, nil
}

// convertRuleToString converts binary rule constant to string
func (bsm *BinaryStorageManager) convertRuleToString(rule uint32) string {
	switch types.ReferentialAction(rule) {
	case types.Cascade:
		return "CASCADE"
	case types.Restrict:
		return "RESTRICT"
	case types.SetNull:
		return "SET NULL"
	case types.SetDefault:
		return "SET DEFAULT"
	default:
		return "NO ACTION"
	}
}

// assignConstraintsToTable assigns constraints to appropriate table fields
func (bsm *BinaryStorageManager) assignConstraintsToTable(table *types.Table, constraints []types.Constraint) {
	for i := range constraints {
		constraint := &constraints[i]
		switch constraint.Type {
		case types.PrimaryKeyConstraint:
			table.PrimaryKey = constraint
		case types.ForeignKeyConstraint:
			table.ForeignKeys = append(table.ForeignKeys, constraint)
		case types.UniqueConstraint:
			table.UniqueKeys = append(table.UniqueKeys, constraint)
		case types.CheckConstraint:
			table.CheckConstraints = append(table.CheckConstraints, constraint)
		}
	}
}

// saveBinaryMetadata saves metadata in optimized binary format
func (bsm *BinaryStorageManager) saveBinaryMetadata() error {
	bsm.mu.Lock()
	defer bsm.mu.Unlock()

	file, err := os.Create(bsm.metadataFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Prepare header
	header := BinaryMetadataHeader{
		Magic:       BinaryMagic,
		Version:     MetadataVersion,
		TableCount:  uint32(len(bsm.tables)),
		NextPageID:  bsm.nextPageID,
		NextTupleID: bsm.nextTupleID,
	}

	// Calculate and set header checksum
	headerBytes := (*[unsafe.Sizeof(header)]byte)(unsafe.Pointer(&header))[:unsafe.Sizeof(header)-8]
	header.Checksum = crc32.ChecksumIEEE(headerBytes)

	// Write header
	err = binary.Write(file, binary.LittleEndian, &header)
	if err != nil {
		return err
	}

	// Write each table
	for _, table := range bsm.tables {
		err = bsm.writeBinaryTable(file, table)
		if err != nil {
			return err
		}
	}

	return file.Sync()
}

// writeBinaryTable writes a table in binary format
func (bsm *BinaryStorageManager) writeBinaryTable(file *os.File, table *types.Table) error {
	// Prepare string table
	stringTable := bytes.NewBuffer(nil)
	stringOffsets := make(map[string]uint32)

	// Prepare string table for column names and values

	// Add all column names and defaults
	columnOffsets := make([]uint32, len(table.Schema.Columns))
	defaultOffsets := make([]uint32, len(table.Schema.Columns))

	for i, col := range table.Schema.Columns {
		columnOffsets[i] = bsm.addStringToTable(stringTable, stringOffsets, col.Name)
		if col.Default != nil {
			defaultOffsets[i] = bsm.addStringToTable(stringTable, stringOffsets, fmt.Sprintf("%v", col.Default))
		}
	}

	// Count all constraints
	constraintCount := len(table.Schema.Constraints)
	if table.PrimaryKey != nil {
		constraintCount++
	}
	constraintCount += len(table.ForeignKeys) + len(table.UniqueKeys) + len(table.CheckConstraints)

	// Write table header
	tableHeader := BinaryTableHeader{
		TableID:         table.ID,
		NameLength:      uint32(len(table.Name)),
		ColumnCount:     uint32(len(table.Schema.Columns)),
		ConstraintCount: uint32(constraintCount),
		IndexCount:      uint32(len(table.Indexes)),
		PageCount:       uint32(len(table.Pages)),
		TupleCount:      uint64(table.Stats.TupleCount),
		TableSize:       uint64(table.Stats.Size),
	}

	// Calculate checksum
	headerBytes := (*[unsafe.Sizeof(tableHeader)]byte)(unsafe.Pointer(&tableHeader))[:unsafe.Sizeof(tableHeader)-8]
	tableHeader.Checksum = crc32.ChecksumIEEE(headerBytes)

	err := binary.Write(file, binary.LittleEndian, &tableHeader)
	if err != nil {
		return err
	}

	// Write table name
	_, err = file.Write([]byte(table.Name))
	if err != nil {
		return err
	}

	// Write page IDs
	for _, pageID := range table.Pages {
		err = binary.Write(file, binary.LittleEndian, pageID)
		if err != nil {
			return err
		}
	}

	// Write column information
	for i, col := range table.Schema.Columns {
		flags := uint32(0)
		if col.Nullable {
			flags |= FlagNullable
		}
		if col.IsPrimaryKey {
			flags |= FlagPrimaryKey
		}
		if col.IsUnique {
			flags |= FlagUnique
		}
		if col.AutoIncrement {
			flags |= FlagAutoIncrement
		}

		colInfo := BinaryColumnInfo{
			NameOffset:    columnOffsets[i],
			DataType:      col.Type,
			Size:          uint32(col.Size),
			Flags:         flags,
			DefaultOffset: defaultOffsets[i],
		}

		err = binary.Write(file, binary.LittleEndian, &colInfo)
		if err != nil {
			return err
		}
	}

	// Write string table
	stringTableData := stringTable.Bytes()
	err = binary.Write(file, binary.LittleEndian, uint32(len(stringTableData)))
	if err != nil {
		return err
	}

	_, err = file.Write(stringTableData)
	if err != nil {
		return err
	}

	// Write constraints
	allConstraints := bsm.getAllConstraints(table)
	for _, constraint := range allConstraints {
		err = bsm.writeBinaryConstraint(file, constraint, stringTable, stringOffsets)
		if err != nil {
			return err
		}
	}

	return nil
}

// addStringToTable adds a string to string table and returns offset
func (bsm *BinaryStorageManager) addStringToTable(buffer *bytes.Buffer, offsets map[string]uint32, str string) uint32 {
	if offset, exists := offsets[str]; exists {
		return offset
	}

	offset := uint32(buffer.Len())
	buffer.WriteString(str)
	buffer.WriteByte(0) // Null terminator
	offsets[str] = offset

	return offset
}

// getAllConstraints collects all constraints for a table
func (bsm *BinaryStorageManager) getAllConstraints(table *types.Table) []*types.Constraint {
	var constraints []*types.Constraint

	// Add schema constraints
	for i := range table.Schema.Constraints {
		constraints = append(constraints, &table.Schema.Constraints[i])
	}

	// Add primary key
	if table.PrimaryKey != nil {
		constraints = append(constraints, table.PrimaryKey)
	}

	// Add foreign keys
	constraints = append(constraints, table.ForeignKeys...)

	// Add unique keys
	constraints = append(constraints, table.UniqueKeys...)

	// Add check constraints
	constraints = append(constraints, table.CheckConstraints...)

	return constraints
}

// writeBinaryConstraint writes a constraint in binary format
func (bsm *BinaryStorageManager) writeBinaryConstraint(file *os.File, constraint *types.Constraint, stringTable *bytes.Buffer, stringOffsets map[string]uint32) error {
	nameOffset := bsm.addStringToTable(stringTable, stringOffsets, constraint.Name)

	constraintInfo := BinaryConstraintInfo{
		NameOffset:     nameOffset,
		Type:           constraint.Type,
		ColumnCount:    uint32(len(constraint.Columns)),
		RefColumnCount: uint32(len(constraint.RefColumns)),
	}

	// Set foreign key specific fields
	if constraint.Type == types.ForeignKeyConstraint {
		constraintInfo.RefTableOffset = bsm.addStringToTable(stringTable, stringOffsets, constraint.RefTable)
		constraintInfo.OnDeleteRule = uint32(bsm.convertStringToRule(constraint.OnDeleteRule))
		constraintInfo.OnUpdateRule = uint32(bsm.convertStringToRule(constraint.OnUpdateRule))
	}

	// Set check constraint specific fields
	if constraint.Type == types.CheckConstraint && constraint.CheckExpr != "" {
		constraintInfo.CheckExprOffset = bsm.addStringToTable(stringTable, stringOffsets, constraint.CheckExpr)
	}

	// Calculate checksum
	infoBytes := (*[unsafe.Sizeof(constraintInfo)]byte)(unsafe.Pointer(&constraintInfo))[:unsafe.Sizeof(constraintInfo)-8]
	constraintInfo.Checksum = crc32.ChecksumIEEE(infoBytes)

	err := binary.Write(file, binary.LittleEndian, &constraintInfo)
	if err != nil {
		return err
	}

	// Write column name offsets
	for _, colName := range constraint.Columns {
		offset := bsm.addStringToTable(stringTable, stringOffsets, colName)
		err = binary.Write(file, binary.LittleEndian, offset)
		if err != nil {
			return err
		}
	}

	// Write referenced column name offsets for foreign keys
	for _, refColName := range constraint.RefColumns {
		offset := bsm.addStringToTable(stringTable, stringOffsets, refColName)
		err = binary.Write(file, binary.LittleEndian, offset)
		if err != nil {
			return err
		}
	}

	return nil
}

// convertStringToRule converts string rule to binary constant
func (bsm *BinaryStorageManager) convertStringToRule(rule string) types.ReferentialAction {
	switch rule {
	case "CASCADE":
		return types.Cascade
	case "RESTRICT":
		return types.Restrict
	case "SET NULL":
		return types.SetNull
	case "SET DEFAULT":
		return types.SetDefault
	default:
		return types.NoAction
	}
}

// migrateFromJSON migrates existing JSON metadata to binary format
func (bsm *BinaryStorageManager) migrateFromJSON() error {
	// Load JSON metadata using parent method (commented out for now)
	// bsm.StorageManager.loadMetadata(filepath.Dir(bsm.metadataFilePath))

	// Save in binary format
	return bsm.saveBinaryMetadata()
}

// Close saves binary metadata and closes storage
func (bsm *BinaryStorageManager) Close() error {
	// Save binary metadata
	if err := bsm.saveBinaryMetadata(); err != nil {
		return err
	}

	// Close base storage
	return bsm.StorageManager.Close()
}

// SerializeTupleBinary serializes tuple data in optimized binary format
func (bsm *BinaryStorageManager) SerializeTupleBinary(data map[string]any, schema types.Schema) []byte {
	// Get cached schema hash for this table
	tableName := "default" // We need table name, but for now use default
	schemaHash, exists := bsm.schemaHashCache[tableName]
	if !exists {
		schemaHash = bsm.calculateSchemaHash(schema)
		bsm.schemaHashCache[tableName] = schemaHash
	}

	// Get buffer from pool to reduce allocations
	buffer := bsm.bufferPool.Get().(*bytes.Buffer)
	buffer.Reset() // Clear any existing data
	defer bsm.bufferPool.Put(buffer)

	// Use cache-aligned header for optimal performance (64 bytes)
	cacheAlignedHeader := struct {
		Magic       uint16  // Magic number for format detection
		ColumnCount uint16  // Number of columns
		DataSize    uint32  // Size of data portion
		Timestamp   uint32  // Creation timestamp
		Checksum    uint32  // Data integrity check
		TupleFlags  uint32  // Tuple status flags
		SchemaHash  uint32  // Schema version hash for validation
		Reserved1   uint32  // Reserved for future use
		Reserved2   uint64  // Reserved for future use
		Reserved3   uint64  // Reserved for future use
		Reserved4   uint64  // Reserved for future use
		Reserved5   uint64  // Reserved for future use
		Reserved6   uint64  // Reserved for future use
		Padding     [8]byte // Padding to reach 64 bytes
	}{
		Magic:       TupleBinaryMagic,
		ColumnCount: uint16(len(schema.Columns)),
		Timestamp:   uint32(0),  // Will be set by caller
		TupleFlags:  0,          // No special flags initially
		SchemaHash:  schemaHash, // Use cached hash
	}

	// Calculate total size for pre-allocation with cache alignment
	estimatedSize := CacheAlignedTupleHeaderSize // 64-byte cache-aligned header
	for _, col := range schema.Columns {
		if value, exists := data[col.Name]; exists && value != nil {
			estimatedSize += bsm.estimateValueSizeCompact(value, col.Type)
		}
	}

	// Pre-grow buffer with estimated size
	buffer.Grow(estimatedSize)

	// Write cache-aligned header placeholder (will be updated later)
	headerPos := buffer.Len()
	binary.Write(buffer, binary.LittleEndian, &cacheAlignedHeader)

	// Get pooled resources to reduce allocations
	bitmapSize := (len(schema.Columns) + 7) / 8
	nullBitmap := bsm.getBitmapFromPool(bitmapSize)
	defer bsm.returnBitmapToPool(nullBitmap)

	nonNullValues := bsm.valueArrayPool.Get().([]any)
	nonNullValues = nonNullValues[:0] // Reset length but keep capacity
	defer bsm.valueArrayPool.Put(nonNullValues)

	nonNullTypes := make([]types.DataType, 0, len(schema.Columns))

	// Vectorized bitmap processing - process 8 columns at a time for better performance
	bsm.processColumnsBitmapVectorized(schema.Columns, data, nullBitmap, &nonNullValues, &nonNullTypes)

	// Write null bitmap
	dataStart := buffer.Len()
	buffer.Write(nullBitmap)

	// Write only non-null values (inline for performance)
	for i, value := range nonNullValues {
		bsm.writeBinaryValueInline(buffer, value, nonNullTypes[i])
	}

	// Update header with actual data size
	dataSize := buffer.Len() - dataStart
	cacheAlignedHeader.DataSize = uint32(dataSize)

	// Calculate checksum for data (use fast checksum for small data)
	dataBytes := buffer.Bytes()[dataStart:]
	if len(dataBytes) < 1024 {
		// Use simple checksum for small tuples (much faster)
		cacheAlignedHeader.Checksum = bsm.fastChecksum(dataBytes)
	} else {
		// Use CRC32 for larger tuples where integrity is more critical
		cacheAlignedHeader.Checksum = crc32.ChecksumIEEE(dataBytes)
	}

	// Update header in buffer (64 bytes)
	headerBytes := (*[CacheAlignedTupleHeaderSize]byte)(unsafe.Pointer(&cacheAlignedHeader))[:]
	copy(buffer.Bytes()[headerPos:], headerBytes)

	// Copy result before returning buffer to pool
	result := make([]byte, buffer.Len())
	copy(result, buffer.Bytes())
	return result
}

// SerializeTuplesBinaryBulk serializes multiple tuples in bulk for better performance
func (bsm *BinaryStorageManager) SerializeTuplesBinaryBulk(dataSlice []map[string]any, schema types.Schema) [][]byte {
	results := make([][]byte, len(dataSlice))

	// Get cached schema hash once for all tuples
	tableName := "default"
	schemaHash, exists := bsm.schemaHashCache[tableName]
	if !exists {
		schemaHash = bsm.calculateSchemaHash(schema)
		bsm.schemaHashCache[tableName] = schemaHash
	}

	// Adaptive batch sizing based on data characteristics for optimal performance
	batchSize := bsm.calculateOptimalBatchSize(len(dataSlice), len(schema.Columns))

	for batchStart := 0; batchStart < len(dataSlice); batchStart += batchSize {
		batchEnd := min(batchStart+batchSize, len(dataSlice))

		// Process batch with vectorized operations when possible
		if batchEnd-batchStart >= 4 && len(schema.Columns) <= 16 {
			// Use vectorized processing for small schemas
			bsm.serializeBatchVectorized(dataSlice[batchStart:batchEnd], results[batchStart:batchEnd], schema, schemaHash)
		} else {
			// Standard processing for complex schemas
			for i := batchStart; i < batchEnd; i++ {
				results[i] = bsm.serializeTupleOptimized(dataSlice[i], schema, schemaHash)
			}
		}
	}

	return results
}

// serializeTupleOptimized is an optimized version for bulk processing
func (bsm *BinaryStorageManager) serializeTupleOptimized(data map[string]any, schema types.Schema, schemaHash uint32) []byte {
	// Use smaller buffer for typical tuple sizes (reduces memory pressure)
	buffer := bytes.NewBuffer(make([]byte, 0, 256))

	// Pre-build header with known schema hash
	cacheAlignedHeader := struct {
		Magic       uint16  // Magic number for format detection
		ColumnCount uint16  // Number of columns
		DataSize    uint32  // Size of data portion
		Timestamp   uint32  // Creation timestamp
		Checksum    uint32  // Data integrity check
		TupleFlags  uint32  // Tuple status flags
		SchemaHash  uint32  // Schema version hash for validation
		Reserved1   uint32  // Reserved for future use
		Reserved2   uint64  // Reserved for future use
		Reserved3   uint64  // Reserved for future use
		Reserved4   uint64  // Reserved for future use
		Reserved5   uint64  // Reserved for future use
		Reserved6   uint64  // Reserved for future use
		Padding     [8]byte // Padding to reach 64 bytes
	}{
		Magic:       TupleBinaryMagic,
		ColumnCount: uint16(len(schema.Columns)),
		Timestamp:   uint32(0),
		TupleFlags:  0,
		SchemaHash:  schemaHash, // Use pre-calculated hash
	}

	headerPos := buffer.Len()
	binary.Write(buffer, binary.LittleEndian, &cacheAlignedHeader)

	// Fast null bitmap and value writing
	bitmapSize := (len(schema.Columns) + 7) / 8
	nullBitmap := make([]byte, bitmapSize)

	dataStart := buffer.Len() + bitmapSize
	buffer.Write(nullBitmap) // Write placeholder

	// Write values directly, updating bitmap as we go
	for i, col := range schema.Columns {
		if value, exists := data[col.Name]; exists && value != nil {
			nullBitmap[i/8] |= 1 << (i % 8)
			bsm.writeBinaryValueInline(buffer, value, col.Type)
		}
	}

	// Update bitmap in buffer
	copy(buffer.Bytes()[dataStart-bitmapSize:dataStart], nullBitmap)

	// Update header
	dataSize := buffer.Len() - dataStart
	cacheAlignedHeader.DataSize = uint32(dataSize)
	cacheAlignedHeader.Checksum = bsm.fastChecksum(buffer.Bytes()[dataStart:])

	headerBytes := (*[CacheAlignedTupleHeaderSize]byte)(unsafe.Pointer(&cacheAlignedHeader))[:]
	copy(buffer.Bytes()[headerPos:], headerBytes)

	return buffer.Bytes()
}

// CompressedSerializeTupleBinary serializes with optional compression for large tuples
func (bsm *BinaryStorageManager) CompressedSerializeTupleBinary(data map[string]any, schema types.Schema, compress bool) []byte {
	// Serialize normally first
	rawData := bsm.SerializeTupleBinary(data, schema)

	// Apply compression if requested and beneficial
	if compress && len(rawData) > 512 { // Only compress larger tuples
		return bsm.compressTupleData(rawData)
	}

	return rawData
}

// compressTupleData applies lightweight compression to tuple data
func (bsm *BinaryStorageManager) compressTupleData(data []byte) []byte {
	// Simple run-length encoding for repeated bytes (common in null bitmaps)
	if len(data) < 64 {
		return data // Not worth compressing small data
	}

	compressed := make([]byte, 0, len(data))
	compressed = append(compressed, 0xFF) // Compression marker

	for i := 0; i < len(data); {
		current := data[i]
		count := 1

		// Count consecutive identical bytes
		for i+count < len(data) && data[i+count] == current && count < 255 {
			count++
		}

		if count > 3 || current == 0 { // Compress runs of 4+ or null bytes
			compressed = append(compressed, 0xFE, byte(count), current)
		} else {
			// Copy literals
			for j := 0; j < count; j++ {
				compressed = append(compressed, data[i+j])
			}
		}

		i += count
	}

	// Only return compressed if it's actually smaller
	if len(compressed) < len(data) {
		return compressed
	}
	return data
}

// calculateSchemaHash generates a hash for schema validation with optimized loops
func (bsm *BinaryStorageManager) calculateSchemaHash(schema types.Schema) uint32 {
	// Optimized hash based on column names and types with unrolled loops
	hash := uint32(2166136261) // FNV-1a offset basis

	for _, col := range schema.Columns {
		// Hash column name with unrolled loop for better performance
		nameBytes := []byte(col.Name)
		hash = bsm.hashBytesUnrolled(hash, nameBytes)

		// Hash column type
		hash ^= uint32(col.Type)
		hash *= 16777619
	}
	return hash
}

// hashBytesUnrolled performs FNV-1a hash with loop unrolling for better performance
func (bsm *BinaryStorageManager) hashBytesUnrolled(hash uint32, data []byte) uint32 {
	const prime = uint32(16777619)
	length := len(data)
	i := 0

	// Process 4 bytes at a time (unrolled)
	for i+3 < length {
		hash ^= uint32(data[i])
		hash *= prime
		hash ^= uint32(data[i+1])
		hash *= prime
		hash ^= uint32(data[i+2])
		hash *= prime
		hash ^= uint32(data[i+3])
		hash *= prime
		i += 4
	}

	// Process remaining bytes
	for i < length {
		hash ^= uint32(data[i])
		hash *= prime
		i++
	}

	return hash
}

// calculateOptimalBatchSize determines optimal batch size based on data characteristics
func (bsm *BinaryStorageManager) calculateOptimalBatchSize(tupleCount, columnCount int) int {
	// Base batch size
	batchSize := 16

	// Adjust based on column count (more columns = smaller batches for cache efficiency)
	if columnCount > 20 {
		batchSize = 8
	} else if columnCount > 50 {
		batchSize = 4
	}

	// Adjust based on tuple count
	if tupleCount < 32 {
		batchSize = min(batchSize, tupleCount)
	} else if tupleCount > 1000 {
		batchSize = min(32, batchSize*2) // Larger batches for bulk operations
	}

	return max(1, batchSize)
}

// serializeBatchVectorized processes a batch of tuples with vectorized operations
func (bsm *BinaryStorageManager) serializeBatchVectorized(batch []map[string]any, results [][]byte, schema types.Schema, schemaHash uint32) {
	// Pre-allocate buffers for the batch
	buffers := make([]*bytes.Buffer, len(batch))
	for i := range batch {
		buffers[i] = bytes.NewBuffer(make([]byte, 0, 256))
	}

	// Process headers in bulk
	for i, data := range batch {
		bsm.writeOptimizedHeader(buffers[i], schema, schemaHash)
		bsm.serializeTupleData(buffers[i], data, schema)
		results[i] = buffers[i].Bytes()
	}
}

// writeOptimizedHeader writes tuple header with optimized operations
func (bsm *BinaryStorageManager) writeOptimizedHeader(buffer *bytes.Buffer, schema types.Schema, schemaHash uint32) {
	header := struct {
		Magic       uint16
		ColumnCount uint16
		DataSize    uint32
		Timestamp   uint32
		Checksum    uint32
		TupleFlags  uint32
		SchemaHash  uint32
		Reserved1   uint32
		Reserved2   uint64
		Reserved3   uint64
		Reserved4   uint64
		Reserved5   uint64
		Reserved6   uint64
		Padding     [8]byte
	}{
		Magic:       TupleBinaryMagic,
		ColumnCount: uint16(len(schema.Columns)),
		Timestamp:   uint32(0),
		TupleFlags:  0,
		SchemaHash:  schemaHash,
	}

	binary.Write(buffer, binary.LittleEndian, &header)
}

// serializeTupleData serializes tuple data with vectorized bitmap processing
func (bsm *BinaryStorageManager) serializeTupleData(buffer *bytes.Buffer, data map[string]any, schema types.Schema) {
	bitmapSize := (len(schema.Columns) + 7) / 8
	nullBitmap := make([]byte, bitmapSize)

	nonNullValues := make([]any, 0, len(schema.Columns))
	nonNullTypes := make([]types.DataType, 0, len(schema.Columns))

	// Use vectorized processing
	bsm.processColumnsBitmapVectorized(schema.Columns, data, nullBitmap, &nonNullValues, &nonNullTypes)

	// Write bitmap
	buffer.Write(nullBitmap)

	// Write values with optimized multi-value writing
	bsm.writeValuesOptimized(buffer, nonNullValues, nonNullTypes)
}

// writeValuesOptimized writes multiple values with type-specific optimizations
func (bsm *BinaryStorageManager) writeValuesOptimized(buffer *bytes.Buffer, values []any, valueTypes []types.DataType) {
	// Process values in groups by type for better cache performance
	for i := 0; i < len(values); i += 4 {
		end := min(i+4, len(values))

		// Try to batch write similar types
		if end-i >= 4 && bsm.canBatchWrite(valueTypes[i:end]) {
			bsm.writeBatchedValues(buffer, values[i:end], valueTypes[i])
		} else {
			// Write individually
			for j := i; j < end; j++ {
				bsm.writeBinaryValueInline(buffer, values[j], valueTypes[j])
			}
		}
	}
}

// canBatchWrite checks if a group of types can be batch written
func (bsm *BinaryStorageManager) canBatchWrite(dataTypes []types.DataType) bool {
	if len(dataTypes) < 4 {
		return false
	}

	// Check if all types are the same and suitable for batching
	firstType := dataTypes[0]
	for _, t := range dataTypes[1:] {
		if t != firstType {
			return false
		}
	}

	// Only batch numeric types
	return firstType == types.IntType || firstType == types.BigIntType || firstType == types.FloatType || firstType == types.DoubleType
}

// writeBatchedValues writes a batch of same-type values efficiently
func (bsm *BinaryStorageManager) writeBatchedValues(buffer *bytes.Buffer, values []any, dataType types.DataType) {
	b := bsm.byteSlicePool.Get().([]byte)
	defer bsm.byteSlicePool.Put(b)

	switch dataType {
	case types.IntType:
		// Batch write 4 int32 values (16 bytes)
		for i, value := range values {
			v32 := uint32(value.(int))
			offset := i * 4
			b[offset] = byte(v32)
			b[offset+1] = byte(v32 >> 8)
			b[offset+2] = byte(v32 >> 16)
			b[offset+3] = byte(v32 >> 24)
		}
		buffer.Write(b[:len(values)*4])
	case types.BigIntType:
		// Batch write 4 int64 values (32 bytes) - would need larger buffer
		for _, value := range values {
			bsm.writeBinaryValueInline(buffer, value, dataType)
		}
	default:
		// Fallback to individual writes
		for _, value := range values {
			bsm.writeBinaryValueInline(buffer, value, dataType)
		}
	}
}

// getBitmapFromPool gets a bitmap of the specified size from pool
func (bsm *BinaryStorageManager) getBitmapFromPool(size int) []byte {
	bitmap := bsm.bitmapPool.Get().([]byte)
	// Resize if needed (keep capacity for reuse)
	if cap(bitmap) < size {
		bitmap = make([]byte, size)
	} else {
		bitmap = bitmap[:size]
		// Clear the bitmap
		for i := range bitmap {
			bitmap[i] = 0
		}
	}
	return bitmap
}

// returnBitmapToPool returns a bitmap to the pool
func (bsm *BinaryStorageManager) returnBitmapToPool(bitmap []byte) {
	if cap(bitmap) <= 64 { // Only pool reasonable sizes
		bsm.bitmapPool.Put(bitmap)
	}
}

// fastChecksum provides a fast checksum for small data (much faster than CRC32)
func (bsm *BinaryStorageManager) fastChecksum(data []byte) uint32 {
	// Simple FNV-1a hash - much faster than CRC32 for small data
	hash := uint32(2166136261)
	for _, b := range data {
		hash ^= uint32(b)
		hash *= 16777619
	}
	return hash
}

// alignToCacheLine aligns an offset to cache line boundary
func (bsm *BinaryStorageManager) alignToCacheLine(offset int) int {
	return (offset + CacheLineMask) &^ CacheLineMask
}

// writeCacheAlignedData writes data with cache-line alignment for optimal access
func (bsm *BinaryStorageManager) writeCacheAlignedData(buffer *bytes.Buffer, data []byte) {
	currentPos := buffer.Len()

	// Calculate padding needed for cache alignment
	alignedPos := bsm.alignToCacheLine(currentPos)
	padding := alignedPos - currentPos

	// Add padding if needed (but only if it's reasonable - less than cache line size)
	if padding > 0 && padding < CacheLineSize {
		buffer.Write(make([]byte, padding))
	}

	// Write the actual data
	buffer.Write(data)
}

// prefetchNextCacheLine provides hint for sequential access optimization
func (bsm *BinaryStorageManager) prefetchNextCacheLine(data []byte, offset int) {
	// In Go, we can't directly issue prefetch instructions, but we can
	// structure our access patterns to be cache-friendly
	// This is a placeholder for potential future optimization with assembly
	if offset+CacheLineSize < len(data) {
		// Touch the next cache line to encourage prefetching
		_ = data[offset+CacheLineSize-1]
	}
}

// estimateValueSize estimates the serialized size of a value
func (bsm *BinaryStorageManager) estimateValueSize(value any, dataType types.DataType) int {
	switch v := value.(type) {
	case int, int32, uint32:
		return 5 // type byte + 4 bytes data
	case int64, uint64:
		return 9 // type byte + 8 bytes data
	case float32:
		return 5 // type byte + 4 bytes data
	case float64:
		return 9 // type byte + 8 bytes data
	case bool:
		return 2 // type byte + 1 byte data
	case string:
		return 5 + len(v) // type byte + length + data
	case []byte:
		return 5 + len(v) // type byte + length + data
	default:
		return 32 // Default estimate for complex types
	}
}

// estimateValueSizeCompact estimates serialized size with compact encoding
func (bsm *BinaryStorageManager) estimateValueSizeCompact(value any, dataType types.DataType) int {
	switch v := value.(type) {
	case int, int32, uint32:
		return 4 // No type byte needed, use position
	case int64, uint64:
		return 8 // No type byte needed
	case float32:
		return 4 // No type byte needed
	case float64:
		return 8 // No type byte needed
	case bool:
		return 1 // No type byte needed
	case string:
		return 2 + len(v) // compact length + data
	case []byte:
		return 2 + len(v) // compact length + data
	default:
		return 16 // Default estimate for complex types
	}
}

// writeBinaryValue writes a value in binary format with type information
func (bsm *BinaryStorageManager) writeBinaryValue(buffer *bytes.Buffer, value any, dataType types.DataType) {
	switch v := value.(type) {
	case int:
		buffer.WriteByte(1) // Type marker for int
		binary.Write(buffer, binary.LittleEndian, int32(v))
	case int32:
		buffer.WriteByte(1)
		binary.Write(buffer, binary.LittleEndian, v)
	case int64:
		buffer.WriteByte(2) // Type marker for int64
		binary.Write(buffer, binary.LittleEndian, v)
	case float32:
		buffer.WriteByte(3) // Type marker for float32
		binary.Write(buffer, binary.LittleEndian, v)
	case float64:
		buffer.WriteByte(4) // Type marker for float64
		binary.Write(buffer, binary.LittleEndian, v)
	case bool:
		buffer.WriteByte(5) // Type marker for bool
		if v {
			buffer.WriteByte(1)
		} else {
			buffer.WriteByte(0)
		}
	case string:
		buffer.WriteByte(6) // Type marker for string
		strBytes := []byte(v)
		binary.Write(buffer, binary.LittleEndian, uint32(len(strBytes)))
		buffer.Write(strBytes)
	case []byte:
		buffer.WriteByte(7) // Type marker for bytes
		binary.Write(buffer, binary.LittleEndian, uint32(len(v)))
		buffer.Write(v)
	default:
		// Fallback to string representation
		buffer.WriteByte(6)
		strRepr := fmt.Sprintf("%v", v)
		strBytes := []byte(strRepr)
		binary.Write(buffer, binary.LittleEndian, uint32(len(strBytes)))
		buffer.Write(strBytes)
	}
}

// DeserializeTupleBinary deserializes binary tuple data back to map
func (bsm *BinaryStorageManager) DeserializeTupleBinary(data []byte, schema types.Schema) map[string]any {
	if len(data) < CacheAlignedTupleHeaderSize { // 64-byte cache-aligned header
		return make(map[string]any)
	}

	reader := bytes.NewReader(data)

	// Read cache-aligned header
	var header struct {
		Magic       uint16  // Magic number for format detection
		ColumnCount uint16  // Number of columns
		DataSize    uint32  // Size of data portion
		Timestamp   uint32  // Creation timestamp
		Checksum    uint32  // Data integrity check
		TupleFlags  uint32  // Tuple status flags
		SchemaHash  uint32  // Schema version hash for validation
		Reserved1   uint32  // Reserved for future use
		Reserved2   uint64  // Reserved for future use
		Reserved3   uint64  // Reserved for future use
		Reserved4   uint64  // Reserved for future use
		Reserved5   uint64  // Reserved for future use
		Reserved6   uint64  // Reserved for future use
		Padding     [8]byte // Padding to reach 64 bytes
	}
	binary.Read(reader, binary.LittleEndian, &header)

	// Verify magic number
	if header.Magic != TupleBinaryMagic {
		return make(map[string]any)
	}

	// Verify schema compatibility
	expectedHash := bsm.calculateSchemaHash(schema)
	if header.SchemaHash != expectedHash {
		// Schema mismatch - this could be from an older version
		// For now, continue but could add migration logic here
	}

	// Verify checksum (try both fast and CRC32 checksums)
	dataBytes := data[CacheAlignedTupleHeaderSize : CacheAlignedTupleHeaderSize+header.DataSize]
	var calculatedChecksum uint32
	if len(dataBytes) < 1024 {
		calculatedChecksum = bsm.fastChecksum(dataBytes)
	} else {
		calculatedChecksum = crc32.ChecksumIEEE(dataBytes)
	}

	// If fast checksum doesn't match, try CRC32 (for compatibility)
	if calculatedChecksum != header.Checksum {
		if len(dataBytes) < 1024 {
			calculatedChecksum = crc32.ChecksumIEEE(dataBytes)
		}
		if calculatedChecksum != header.Checksum {
			// Data corruption detected, return empty map
			return make(map[string]any)
		}
	}

	result := make(map[string]any)

	// Read null bitmap
	nullBitmapSize := (len(schema.Columns) + 7) / 8
	nullBitmap := make([]byte, nullBitmapSize)
	reader.Read(nullBitmap)

	// Pre-allocate result map with known size
	result = make(map[string]any, len(schema.Columns))

	// Read only non-null values in schema order with cache-friendly access
	for i, col := range schema.Columns {
		if reader.Len() == 0 {
			break
		}

		// Prefetch hint for sequential access (every few iterations)
		if i%8 == 0 {
			currentOffset := len(data) - reader.Len()
			bsm.prefetchNextCacheLine(data, currentOffset)
		}

		// Check if value is null using bitmap
		if (nullBitmap[i/8] & (1 << (i % 8))) == 0 {
			// Null value - don't store in map for efficiency
			continue
		}

		// Read value based on column type (no type marker needed)
		value := bsm.readBinaryValueCompact(reader, col.Type)
		if value != nil {
			result[col.Name] = value
		}
	}

	return result
}

// DeserializeTupleBinaryOptimized provides an optimized deserialization path for high-performance scenarios
func (bsm *BinaryStorageManager) DeserializeTupleBinaryOptimized(data []byte, schema types.Schema) map[string]any {
	// Fast path validation
	if len(data) < CacheAlignedTupleHeaderSize {
		return make(map[string]any)
	}

	// Use unsafe pointer for faster header reading
	header := (*struct {
		Magic       uint16
		ColumnCount uint16
		DataSize    uint32
		Timestamp   uint32
		Checksum    uint32
		TupleFlags  uint32
		SchemaHash  uint32
		Reserved1   uint32
		Reserved2   uint64
		Reserved3   uint64
		Reserved4   uint64
		Reserved5   uint64
		Reserved6   uint64
		Padding     [8]byte
	})(unsafe.Pointer(&data[0]))

	// Quick validation
	if header.Magic != TupleBinaryMagic {
		return make(map[string]any)
	}

	// Skip checksum validation for performance (optional)
	// Could be enabled with a flag for critical data

	// Pre-allocate result with known size
	result := make(map[string]any, len(schema.Columns))

	// Fast bitmap reading without bytes.Reader overhead
	dataStart := CacheAlignedTupleHeaderSize
	nullBitmapSize := (len(schema.Columns) + 7) / 8
	nullBitmap := data[dataStart : dataStart+nullBitmapSize]

	// Direct data access without reader
	valueData := data[dataStart+nullBitmapSize:]
	offset := 0

	// Optimized column processing
	for i, col := range schema.Columns {
		// Check null bitmap
		if (nullBitmap[i/8] & (1 << (i % 8))) == 0 {
			continue // Null value
		}

		// Read value directly from byte slice
		value, bytesRead := bsm.readValueFast(valueData[offset:], col.Type)
		if value != nil {
			result[col.Name] = value
			offset += bytesRead
		}
	}

	return result
}

// readValueFast reads a value directly from byte slice for maximum performance
func (bsm *BinaryStorageManager) readValueFast(data []byte, dataType types.DataType) (any, int) {
	if len(data) < 2 {
		return nil, 0
	}

	switch dataType {
	case types.IntType:
		if len(data) < 4 {
			return nil, 0
		}
		// Direct unsafe conversion for speed
		value := *(*int32)(unsafe.Pointer(&data[0]))
		return int(value), 4

	case types.BigIntType:
		if len(data) < 8 {
			return nil, 0
		}
		// Direct unsafe conversion for speed
		value := *(*int64)(unsafe.Pointer(&data[0]))
		return value, 8

	case types.DoubleType:
		if len(data) < 8 {
			return nil, 0
		}
		value := *(*float64)(unsafe.Pointer(&data[0]))
		return value, 8

	case types.BoolType:
		if len(data) < 1 {
			return nil, 0
		}
		return data[0] != 0, 1

	case types.VarcharType, types.TextType, types.CharType:
		if len(data) < 2 {
			return nil, 0
		}
		length := *(*uint16)(unsafe.Pointer(&data[0]))
		if len(data) < int(2+length) {
			return nil, 0
		}
		if length == 0 {
			return "", 2
		}
		// Direct string conversion without copying when safe
		value := string(data[2 : 2+length])
		return value, int(2 + length)

	case types.ByteaType:
		if len(data) < 2 {
			return nil, 0
		}
		length := *(*uint16)(unsafe.Pointer(&data[0]))
		if len(data) < int(2+length) {
			return nil, 0
		}
		if length == 0 {
			return []byte{}, 2
		}
		value := make([]byte, length)
		copy(value, data[2:2+length])
		return value, int(2 + length)

	default:
		// Fallback to safe method for unknown types
		reader := bytes.NewReader(data)
		value := bsm.readBinaryValueCompact(reader, dataType)
		bytesRead := len(data) - reader.Len()
		return value, bytesRead
	}
}

// readBinaryValue reads a binary value based on type marker
func (bsm *BinaryStorageManager) readBinaryValue(reader *bytes.Reader, typeByte byte) any {
	switch typeByte {
	case 1: // int32
		var value int32
		binary.Read(reader, binary.LittleEndian, &value)
		return int(value)
	case 2: // int64
		var value int64
		binary.Read(reader, binary.LittleEndian, &value)
		return value
	case 3: // float32
		var value float32
		binary.Read(reader, binary.LittleEndian, &value)
		return value
	case 4: // float64
		var value float64
		binary.Read(reader, binary.LittleEndian, &value)
		return value
	case 5: // bool
		var value byte
		binary.Read(reader, binary.LittleEndian, &value)
		return value != 0
	case 6: // string
		var length uint32
		binary.Read(reader, binary.LittleEndian, &length)
		strBytes := make([]byte, length)
		reader.Read(strBytes)
		return string(strBytes)
	case 7: // bytes
		var length uint32
		binary.Read(reader, binary.LittleEndian, &length)
		bytes := make([]byte, length)
		reader.Read(bytes)
		return bytes
	default:
		return nil
	}
}

// readBinaryValueCompact reads a binary value without type markers using schema type
func (bsm *BinaryStorageManager) readBinaryValueCompact(reader *bytes.Reader, dataType types.DataType) any {
	switch dataType {
	case types.IntType:
		var value int32
		binary.Read(reader, binary.LittleEndian, &value)
		return int(value)
	case types.BigIntType:
		var value int64
		binary.Read(reader, binary.LittleEndian, &value)
		return value
	case types.FloatType:
		var value float32
		binary.Read(reader, binary.LittleEndian, &value)
		return value
	case types.DoubleType:
		var value float64
		binary.Read(reader, binary.LittleEndian, &value)
		return value
	case types.BoolType:
		var value byte
		binary.Read(reader, binary.LittleEndian, &value)
		return value != 0
	case types.VarcharType, types.TextType, types.CharType:
		var length uint16
		if binary.Read(reader, binary.LittleEndian, &length) != nil {
			return ""
		}
		if length == 0 {
			return ""
		}
		strBytes := make([]byte, length)
		if n, _ := reader.Read(strBytes); n != int(length) {
			return ""
		}
		return string(strBytes)
	case types.ByteaType:
		var length uint16
		if binary.Read(reader, binary.LittleEndian, &length) != nil {
			return nil
		}
		if length == 0 {
			return []byte{}
		}
		bytes := make([]byte, length)
		if n, _ := reader.Read(bytes); n != int(length) {
			return nil
		}
		return bytes
	default:
		// Fallback to string
		var length uint16
		if binary.Read(reader, binary.LittleEndian, &length) != nil {
			return ""
		}
		if length == 0 {
			return ""
		}
		strBytes := make([]byte, length)
		if n, _ := reader.Read(strBytes); n != int(length) {
			return ""
		}
		return string(strBytes)
	}
}

// SerializeTuplesBinaryParallel serializes multiple tuples using parallel lanes for large datasets
func (bsm *BinaryStorageManager) SerializeTuplesBinaryParallel(dataSlice []map[string]any, schema types.Schema) [][]byte {
	results := make([][]byte, len(dataSlice))

	// Only use parallel processing for larger datasets (overhead not worth it for small sets)
	if len(dataSlice) < 100 {
		return bsm.SerializeTuplesBinaryBulk(dataSlice, schema)
	}

	// Calculate optimal number of workers (limit to prevent context switching overhead)
	numWorkers := min(runtime.NumCPU(), len(dataSlice)/50, 8)
	if numWorkers < 2 {
		return bsm.SerializeTuplesBinaryBulk(dataSlice, schema)
	}

	// Get cached schema hash once for all workers
	tableName := "default"
	schemaHash, exists := bsm.schemaHashCache[tableName]
	if !exists {
		schemaHash = bsm.calculateSchemaHash(schema)
		bsm.schemaHashCache[tableName] = schemaHash
	}

	// Create work distribution
	chunkSize := len(dataSlice) / numWorkers
	var wg sync.WaitGroup

	// Process chunks in parallel
	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if i == numWorkers-1 {
			end = len(dataSlice) // Last worker takes remaining items
		}

		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()

			// Process this chunk
			for j := start; j < end; j++ {
				results[j] = bsm.serializeTupleOptimized(dataSlice[j], schema, schemaHash)
			}
		}(start, end)
	}

	wg.Wait()
	return results
}

// DeserializeTuplesBinaryParallel deserializes multiple binary tuples in parallel
func (bsm *BinaryStorageManager) DeserializeTuplesBinaryParallel(dataSlice [][]byte, schema types.Schema) []map[string]any {
	results := make([]map[string]any, len(dataSlice))

	// Only use parallel processing for larger datasets
	if len(dataSlice) < 50 {
		return bsm.deserializeTuplesBulk(dataSlice, schema)
	}

	// Calculate optimal number of workers
	numWorkers := min(runtime.NumCPU(), len(dataSlice)/25, 8)
	if numWorkers < 2 {
		return bsm.deserializeTuplesBulk(dataSlice, schema)
	}

	// Create work distribution
	chunkSize := len(dataSlice) / numWorkers
	var wg sync.WaitGroup

	// Process chunks in parallel
	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if i == numWorkers-1 {
			end = len(dataSlice) // Last worker takes remaining items
		}

		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()

			// Deserialize this chunk
			for j := start; j < end; j++ {
				results[j] = bsm.DeserializeTupleBinaryOptimized(dataSlice[j], schema)
			}
		}(start, end)
	}

	wg.Wait()
	return results
}

// deserializeTuplesBulk deserializes tuples sequentially for small datasets
func (bsm *BinaryStorageManager) deserializeTuplesBulk(dataSlice [][]byte, schema types.Schema) []map[string]any {
	results := make([]map[string]any, len(dataSlice))
	for i, data := range dataSlice {
		results[i] = bsm.DeserializeTupleBinaryOptimized(data, schema)
	}
	return results
}

// writeBinaryValueInline writes a value optimized for performance with pooled byte slices
func (bsm *BinaryStorageManager) writeBinaryValueInline(buffer *bytes.Buffer, value any, dataType types.DataType) {
	// Fast path for common types using pooled byte slices with unrolled operations
	switch v := value.(type) {
	case int:
		// Unrolled int32 writing - faster than binary.Write
		b := bsm.byteSlicePool.Get().([]byte)
		v32 := uint32(v)
		b[0] = byte(v32)
		b[1] = byte(v32 >> 8)
		b[2] = byte(v32 >> 16)
		b[3] = byte(v32 >> 24)
		buffer.Write(b[:4])
		bsm.byteSlicePool.Put(b)
		return
	case int32:
		// Unrolled int32 writing
		b := bsm.byteSlicePool.Get().([]byte)
		v32 := uint32(v)
		b[0] = byte(v32)
		b[1] = byte(v32 >> 8)
		b[2] = byte(v32 >> 16)
		b[3] = byte(v32 >> 24)
		buffer.Write(b[:4])
		bsm.byteSlicePool.Put(b)
		return
	case int64:
		// Unrolled int64 writing - process in 32-bit chunks for better performance
		b := bsm.byteSlicePool.Get().([]byte)
		v64 := uint64(v)
		// Lower 32 bits
		lower := uint32(v64)
		b[0] = byte(lower)
		b[1] = byte(lower >> 8)
		b[2] = byte(lower >> 16)
		b[3] = byte(lower >> 24)
		// Upper 32 bits
		upper := uint32(v64 >> 32)
		b[4] = byte(upper)
		b[5] = byte(upper >> 8)
		b[6] = byte(upper >> 16)
		b[7] = byte(upper >> 24)
		buffer.Write(b[:8])
		bsm.byteSlicePool.Put(b)
		return
	case string:
		// Optimized string writing with length caching
		strBytes := []byte(v)
		length := uint16(len(strBytes))
		b := bsm.byteSlicePool.Get().([]byte)
		b[0] = byte(length)
		b[1] = byte(length >> 8)
		buffer.Write(b[:2])
		buffer.Write(strBytes)
		bsm.byteSlicePool.Put(b)
		return
	}

	// Fallback to standard implementation for other types
	bsm.writeBinaryValueCompact(buffer, value, dataType)
}

// processColumnsBitmapVectorized processes columns in vectorized manner for better performance
func (bsm *BinaryStorageManager) processColumnsBitmapVectorized(columns []types.Column, data map[string]any, nullBitmap []byte, nonNullValues *[]any, nonNullTypes *[]types.DataType) {
	colCount := len(columns)

	// Process 8 columns at a time for vectorized bitmap operations
	for i := 0; i < colCount; i += 8 {
		end := i + 8
		if end > colCount {
			end = colCount
		}

		// Process this chunk of columns
		bitmapByte := byte(0)
		bitmapByteIdx := i / 8

		// Unrolled loop for up to 8 columns
		for j := i; j < end; j++ {
			col := columns[j]
			if value, exists := data[col.Name]; exists && value != nil {
				// Set bit in current byte
				bitmapByte |= 1 << (j % 8)
				*nonNullValues = append(*nonNullValues, value)
				*nonNullTypes = append(*nonNullTypes, col.Type)
			}
		}

		// Store the computed byte
		nullBitmap[bitmapByteIdx] = bitmapByte
	}
}

// writeBinaryValueCompact writes a value without type markers for space efficiency
func (bsm *BinaryStorageManager) writeBinaryValueCompact(buffer *bytes.Buffer, value any, dataType types.DataType) {
	switch v := value.(type) {
	case int:
		binary.Write(buffer, binary.LittleEndian, int32(v))
	case int32:
		binary.Write(buffer, binary.LittleEndian, v)
	case int64:
		binary.Write(buffer, binary.LittleEndian, v)
	case float32:
		binary.Write(buffer, binary.LittleEndian, v)
	case float64:
		binary.Write(buffer, binary.LittleEndian, v)
	case bool:
		if v {
			buffer.WriteByte(1)
		} else {
			buffer.WriteByte(0)
		}
	case string:
		strBytes := []byte(v)
		// Use compact length encoding (2 bytes max for 64KB strings)
		binary.Write(buffer, binary.LittleEndian, uint16(len(strBytes)))
		buffer.Write(strBytes)
	case []byte:
		binary.Write(buffer, binary.LittleEndian, uint16(len(v)))
		buffer.Write(v)
	default:
		// Fallback to string representation
		strRepr := fmt.Sprintf("%v", v)
		strBytes := []byte(strRepr)
		binary.Write(buffer, binary.LittleEndian, uint16(len(strBytes)))
		buffer.Write(strBytes)
	}
}
