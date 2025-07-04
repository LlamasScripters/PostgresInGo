package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"unsafe"

	"github.com/esgi-git/postgres-engine/internal/types"
)

// Cache line size for modern CPUs (64 bytes)
const (
	CacheLineSize = 64
	BinaryMagic   = 0x504754524553 // "PGTRES" in hex
	MetadataVersion = 1
)

// BinaryStorageManager implements cache-aligned binary storage
type BinaryStorageManager struct {
	*StorageManager
	metadataFilePath string
	binaryMode       bool
}

// BinaryMetadataHeader is cache-aligned metadata header (64 bytes)
type BinaryMetadataHeader struct {
	Magic       uint64 // 8 bytes - Magic number for format validation
	Version     uint32 // 4 bytes - Format version
	TableCount  uint32 // 4 bytes - Number of tables
	NextPageID  uint64 // 8 bytes - Next page ID
	NextTupleID uint64 // 8 bytes - Next tuple ID
	Reserved1   uint64 // 8 bytes - Reserved for future use
	Reserved2   uint64 // 8 bytes - Reserved for future use
	Reserved3   uint64 // 8 bytes - Reserved for future use
	Checksum    uint32 // 4 bytes - Header checksum
	Padding     [4]byte // 4 bytes - Padding to reach 64 bytes
}

// BinaryTableHeader represents a table in binary format (cache-aligned to 64 bytes)
type BinaryTableHeader struct {
	TableID        uint64    // 8 bytes - Table ID
	NameLength     uint32    // 4 bytes - Length of table name
	ColumnCount    uint32    // 4 bytes - Number of columns
	ConstraintCount uint32   // 4 bytes - Number of constraints
	IndexCount     uint32    // 4 bytes - Number of indexes
	PageCount      uint32    // 4 bytes - Number of pages
	TupleCount     uint64    // 8 bytes - Total tuple count
	TableSize      uint64    // 8 bytes - Size in bytes
	Reserved1      uint64    // 8 bytes - Reserved for future use
	Reserved2      uint64    // 8 bytes - Reserved for future use
	Checksum       uint32    // 4 bytes - Header checksum
	Padding        [4]byte   // 4 bytes - Padding to reach 64 bytes
}

// BinaryColumnInfo represents column metadata (cache-aligned to 32 bytes)
type BinaryColumnInfo struct {
	NameOffset    uint32       // 4 bytes - Offset to column name in string table
	DataType      types.DataType // 4 bytes - Column data type
	Size          uint32       // 4 bytes - Column size
	Flags         uint32       // 4 bytes - Flags (nullable, primary key, unique, auto increment)
	DefaultOffset uint32       // 4 bytes - Offset to default value in string table
	Reserved      uint32       // 4 bytes - Reserved for future use
	Padding       [8]byte      // 8 bytes - Padding to reach 32 bytes
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
	NameOffset      uint32                 // 4 bytes - Offset to constraint name
	Type            types.ConstraintType   // 4 bytes - Constraint type
	ColumnCount     uint32                 // 4 bytes - Number of columns in constraint
	RefTableOffset  uint32                 // 4 bytes - Offset to referenced table name
	RefColumnCount  uint32                 // 4 bytes - Number of referenced columns
	OnDeleteRule    uint32                 // 4 bytes - On delete rule
	OnUpdateRule    uint32                 // 4 bytes - On update rule
	CheckExprOffset uint32                 // 4 bytes - Offset to check expression
	Reserved1       uint64                 // 8 bytes - Reserved
	Reserved2       uint64                 // 8 bytes - Reserved
	Reserved3       uint64                 // 8 bytes - Reserved
	Checksum        uint32                 // 4 bytes - Constraint checksum
	Padding         [4]byte                // 4 bytes - Padding to 64 bytes
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
		ID:          tableHeader.TableID,
		Name:        tableName,
		Pages:       make([]uint64, tableHeader.PageCount),
		Indexes:     make(map[string]*types.Index),
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
	// Load JSON metadata using parent method
	bsm.StorageManager.loadMetadata(filepath.Dir(bsm.metadataFilePath))
	
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

// SerializeTupleBinary serializes tuple data in cache-aligned binary format
func (bsm *BinaryStorageManager) SerializeTupleBinary(data map[string]any, schema types.Schema) []byte {
	buffer := bytes.NewBuffer(nil)
	
	// Calculate total size for pre-allocation
	estimatedSize := int(unsafe.Sizeof(BinaryTupleHeader{}))
	for _, col := range schema.Columns {
		if value, exists := data[col.Name]; exists && value != nil {
			estimatedSize += bsm.estimateValueSize(value, col.Type)
		}
	}
	
	// Pre-grow buffer with estimated size (no alignment for now to avoid size issues)
	buffer.Grow(estimatedSize)
	
	// Write tuple header placeholder (will be updated later)
	header := BinaryTupleHeader{
		ColumnCount: uint32(len(schema.Columns)),
		Timestamp:   uint64(0), // Will be set by caller
	}
	
	headerPos := buffer.Len()
	binary.Write(buffer, binary.LittleEndian, &header)
	
	// Write column data in schema order for predictable layout
	dataStart := buffer.Len()
	for _, col := range schema.Columns {
		value, exists := data[col.Name]
		if !exists || value == nil {
			// Write null marker
			buffer.WriteByte(0)
			continue
		}
		
		// Write value type and data
		bsm.writeBinaryValue(buffer, value, col.Type)
	}
	
	// Update header with actual data size
	dataSize := buffer.Len() - dataStart
	header.DataSize = uint32(dataSize)
	
	// Calculate checksum for data
	dataBytes := buffer.Bytes()[dataStart:]
	header.Checksum = crc32.ChecksumIEEE(dataBytes)
	
	// Update header in buffer
	headerBytes := (*[unsafe.Sizeof(header)]byte)(unsafe.Pointer(&header))[:]
	copy(buffer.Bytes()[headerPos:], headerBytes)
	
	return buffer.Bytes()
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
	if len(data) < int(unsafe.Sizeof(BinaryTupleHeader{})) {
		return make(map[string]any)
	}
	
	reader := bytes.NewReader(data)
	
	// Read tuple header
	var header BinaryTupleHeader
	binary.Read(reader, binary.LittleEndian, &header)
	
	// Verify checksum
	dataBytes := data[unsafe.Sizeof(header):unsafe.Sizeof(header)+uintptr(header.DataSize)]
	calculatedChecksum := crc32.ChecksumIEEE(dataBytes)
	if calculatedChecksum != header.Checksum {
		// Data corruption detected, return empty map
		return make(map[string]any)
	}
	
	result := make(map[string]any)
	
	// Read column values in schema order
	for _, col := range schema.Columns {
		if reader.Len() == 0 {
			break
		}
		
		// Read type marker
		var typeByte byte
		binary.Read(reader, binary.LittleEndian, &typeByte)
		
		if typeByte == 0 {
			// Null value
			result[col.Name] = nil
			continue
		}
		
		// Read value based on type
		value := bsm.readBinaryValue(reader, typeByte)
		if value != nil {
			result[col.Name] = value
		}
	}
	
	return result
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