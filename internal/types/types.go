package types

import (
	"fmt"
	"net"
	"slices"
	"time"
)

// DataType represents column data types
type DataType int

const (
	// Integer Types
	SmallIntType  DataType = iota // 16-bit integer (-32,768 to 32,767)
	IntType                       // 32-bit integer
	BigIntType                    // 64-bit integer
	SerialType                    // Auto-incrementing 32-bit integer
	BigSerialType                 // Auto-incrementing 64-bit integer

	// Numeric Types
	NumericType // Arbitrary precision decimal
	DecimalType // Alias for Numeric
	RealType    // Single precision floating point
	DoubleType  // Double precision floating point
	FloatType   // Legacy alias for Real
	MoneyType   // Currency amount

	// Character Types
	CharType    // Fixed-length character string
	VarcharType // Variable-length character string
	TextType    // Variable unlimited length text

	// Boolean Type
	BoolType

	// Date/Time Types
	DateType        // Date (year, month, day)
	TimeType        // Time of day
	TimestampType   // Date and time
	TimestampTzType // Date and time with timezone
	IntervalType    // Time interval

	// Binary Types
	ByteaType // Binary data ("byte array")

	// Network Address Types
	InetType    // IPv4 or IPv6 host address
	CidrType    // IPv4 or IPv6 network address
	MacAddrType // MAC address

	// UUID Type
	UuidType // Universally unique identifier

	// JSON Types
	JsonType  // JSON data
	JsonbType // Binary JSON data

	// Geometric Types
	PointType   // Point on a plane
	LineType    // Infinite line
	LsegType    // Line segment
	BoxType     // Rectangular box
	PathType    // Geometric path
	PolygonType // Closed geometric path
	CircleType  // Circle

	// Array Types
	IntArrayType     // Array of integers
	TextArrayType    // Array of text
	VarcharArrayType // Array of varchar
	BoolArrayType    // Array of booleans
	FloatArrayType   // Array of floats
	DateArrayType    // Array of dates

	// Enumeration Type
	EnumType // User-defined enumeration

	// Range Types
	Int4RangeType // Range of integers
	Int8RangeType // Range of bigints
	NumRangeType  // Range of numerics
	TsRangeType   // Range of timestamps
	TsTzRangeType // Range of timestamps with timezone
	DateRangeType // Range of dates

	// Full Text Search Types
	TsVectorType // Text search vector
	TsQueryType  // Text search query

	// XML Type
	XmlType // XML data

	// Composite Type
	CompositeType // User-defined composite type

	// Unknown/Untyped
	UnknownType // For untyped literals

	// Internal types for type system
	AnyType     // Represents any type for polymorphic functions
	VoidType    // Represents void/no return type
	RecordType  // Generic record type
	TriggerType // Trigger function type
)

// ConstraintType represents different constraint types
type ConstraintType int

const (
	PrimaryKeyConstraint ConstraintType = iota
	ForeignKeyConstraint
	UniqueConstraint
	CheckConstraint
	NotNullConstraint
)

// Constraint represents a table constraint
type Constraint struct {
	Name         string
	Type         ConstraintType
	Columns      []string
	RefTable     string   // For foreign keys
	RefColumns   []string // For foreign keys
	CheckExpr    string   // For check constraints
	OnDeleteRule string   // CASCADE, RESTRICT, SET NULL, etc.
	OnUpdateRule string   // CASCADE, RESTRICT, SET NULL, etc.
}

// Column represents a table column
type Column struct {
	Name          string
	Type          DataType
	Size          int
	Nullable      bool
	Default       any
	IsPrimaryKey  bool
	IsUnique      bool
	AutoIncrement bool
}

// Schema defines table structure
type Schema struct {
	Columns     []Column
	Constraints []Constraint
}

// TupleID uniquely identifies a tuple
type TupleID struct {
	PageID uint64
	Offset uint16
}

// Tuple represents a row in a table
type Tuple struct {
	TID      TupleID
	Data     []byte
	Metadata TupleMetadata
}

// TupleMetadata contains tuple metadata
type TupleMetadata struct {
	Size      uint16
	Timestamp time.Time
}

// PageType represents different page types
type PageType int

const (
	DataPage PageType = iota
	IndexPage
	MetaPage
)

// Page represents a disk page
type Page struct {
	ID       uint64
	Type     PageType
	Data     []byte
	Checksum uint32
	LSN      uint64
}

// ReferentialAction represents referential integrity actions
type ReferentialAction int

const (
	NoAction ReferentialAction = iota
	Restrict
	Cascade
	SetNull
	SetDefault
)

// Table represents a database table
type Table struct {
	ID               uint64
	Name             string
	Schema           Schema
	Pages            []uint64
	Indexes          map[string]*Index
	Stats            TableStats
	PrimaryKey       *Constraint
	ForeignKeys      []*Constraint
	UniqueKeys       []*Constraint
	CheckConstraints []*Constraint
}

// TableStats contains table statistics
type TableStats struct {
	TupleCount int64
	PageCount  int64
	Size       int64
}

// Index represents a database index
type Index struct {
	ID       uint64
	Name     string
	Table    string
	Columns  []string
	Type     IndexType
	Metadata IndexMetadata
}

// IndexType represents index types
type IndexType int

const (
	BTreeIndex IndexType = iota
	HashIndex
)

// IndexMetadata contains index metadata
type IndexMetadata struct {
	Height int
	Size   int64
}

// Result represents query execution result
type Result struct {
	Rows     [][]any
	Columns  []string
	Affected int64
	Error    error
}

// TransactionState represents transaction states
type TransactionState int

const (
	TxnActive TransactionState = iota
	TxnCommitted
	TxnAborted
)

// IsolationLevel represents transaction isolation levels
type IsolationLevel int

const (
	ReadUncommitted IsolationLevel = iota
	ReadCommitted
	RepeatableRead
	Serializable
)

// Transaction represents a database transaction
type Transaction struct {
	ID        uint64
	State     TransactionState
	StartTime time.Time
	LSN       uint64
	ReadSet   map[TupleID]bool
	WriteSet  map[TupleID]*Tuple
	LockSet   map[TupleID]LockType
	Isolation IsolationLevel
}

// LockType represents lock types
type LockType int

const (
	SharedLock LockType = iota
	ExclusiveLock
	IntentionSharedLock
	IntentionExclusiveLock
)

// LogRecordType represents WAL record types
type LogRecordType int

const (
	LogBegin LogRecordType = iota
	LogCommit
	LogAbort
	LogUpdate
	LogInsert
	LogDelete
)

// LogRecord represents a WAL record
type LogRecord struct {
	LSN       uint64
	TxnID     uint64
	Type      LogRecordType
	Data      []byte
	Timestamp time.Time
}

// View represents a database view
type View struct {
	ID           uint64
	Name         string
	Definition   string       // The original SQL query defining the view
	Columns      []ViewColumn // Column metadata for the view
	Dependencies []string     // Tables this view depends on
	CreatedAt    time.Time
	Schema       string // Database schema name
}

// ViewColumn represents a column in a view
type ViewColumn struct {
	Name         string
	Type         DataType
	SourceTable  string // Which table this column comes from
	SourceColumn string // Original column name if different
	IsComputed   bool   // True for computed/aggregate columns
}

// Helper functions for constraint management

// GetPrimaryKeyColumns returns the primary key column names
func (t *Table) GetPrimaryKeyColumns() []string {
	if t.PrimaryKey != nil {
		return t.PrimaryKey.Columns
	}

	// Look for columns marked as primary key
	var pkColumns []string
	for _, col := range t.Schema.Columns {
		if col.IsPrimaryKey {
			pkColumns = append(pkColumns, col.Name)
		}
	}
	return pkColumns
}

// HasPrimaryKey checks if table has a primary key
func (t *Table) HasPrimaryKey() bool {
	return t.PrimaryKey != nil || len(t.GetPrimaryKeyColumns()) > 0
}

// GetForeignKeyFor returns foreign key constraint for given column
func (t *Table) GetForeignKeyFor(column string) *Constraint {
	for _, fk := range t.ForeignKeys {
		if slices.Contains(fk.Columns, column) {
			return fk
		}
	}
	return nil
}

// GetColumnByName returns column by name
func (s *Schema) GetColumnByName(name string) *Column {
	for i := range s.Columns {
		if s.Columns[i].Name == name {
			return &s.Columns[i]
		}
	}
	return nil
}

// HasColumn checks if schema has a column with given name
func (s *Schema) HasColumn(name string) bool {
	return s.GetColumnByName(name) != nil
}

// GetColumnIndex returns the index of a column by name
func (s *Schema) GetColumnIndex(name string) int {
	for i, col := range s.Columns {
		if col.Name == name {
			return i
		}
	}
	return -1
}

// JoinType represents different types of joins
type JoinType int

const (
	InnerJoin JoinType = iota
	LeftJoin
	RightJoin
	FullOuterJoin
	CrossJoin
)

// JoinCondition represents a join condition
type JoinCondition struct {
	LeftColumn  string
	RightColumn string
	Operator    string // =, <>, <, >, <=, >=
}

// QueryPlan represents a query execution plan
type QueryPlan struct {
	Tables         []*Table
	JoinType       JoinType
	JoinConditions []JoinCondition
	SelectColumns  []string
}

// TypeInfo provides metadata about data types
type TypeInfo struct {
	Name        string
	Size        int    // Size in bytes, -1 for variable
	Alignment   int    // Alignment requirement
	Category    string // Category (numeric, string, datetime, etc.)
	Description string
	IsArray     bool
	ElementType DataType // For array types
}

// GetTypeInfo returns metadata for a data type
func GetTypeInfo(dataType DataType) TypeInfo {
	switch dataType {
	// Integer Types
	case SmallIntType:
		return TypeInfo{"smallint", 2, 2, "numeric", "16-bit signed integer", false, UnknownType}
	case IntType:
		return TypeInfo{"integer", 4, 4, "numeric", "32-bit signed integer", false, UnknownType}
	case BigIntType:
		return TypeInfo{"bigint", 8, 8, "numeric", "64-bit signed integer", false, UnknownType}
	case SerialType:
		return TypeInfo{"serial", 4, 4, "numeric", "auto-incrementing 32-bit integer", false, UnknownType}
	case BigSerialType:
		return TypeInfo{"bigserial", 8, 8, "numeric", "auto-incrementing 64-bit integer", false, UnknownType}

	// Numeric Types
	case NumericType:
		return TypeInfo{"numeric", -1, 4, "numeric", "arbitrary precision decimal", false, UnknownType}
	case DecimalType:
		return TypeInfo{"decimal", -1, 4, "numeric", "arbitrary precision decimal", false, UnknownType}
	case RealType:
		return TypeInfo{"real", 4, 4, "numeric", "single precision floating point", false, UnknownType}
	case DoubleType:
		return TypeInfo{"double precision", 8, 8, "numeric", "double precision floating point", false, UnknownType}
	case FloatType:
		return TypeInfo{"float", 8, 8, "numeric", "double precision floating point", false, UnknownType}
	case MoneyType:
		return TypeInfo{"money", 8, 8, "numeric", "currency amount", false, UnknownType}

	// Character Types
	case CharType:
		return TypeInfo{"char", -1, 1, "string", "fixed-length character string", false, UnknownType}
	case VarcharType:
		return TypeInfo{"varchar", -1, 1, "string", "variable-length character string", false, UnknownType}
	case TextType:
		return TypeInfo{"text", -1, 1, "string", "variable unlimited length text", false, UnknownType}

	// Boolean Type
	case BoolType:
		return TypeInfo{"boolean", 1, 1, "boolean", "logical boolean", false, UnknownType}

	// Date/Time Types
	case DateType:
		return TypeInfo{"date", 4, 4, "datetime", "calendar date (year, month, day)", false, UnknownType}
	case TimeType:
		return TypeInfo{"time", 8, 8, "datetime", "time of day", false, UnknownType}
	case TimestampType:
		return TypeInfo{"timestamp", 8, 8, "datetime", "date and time", false, UnknownType}
	case TimestampTzType:
		return TypeInfo{"timestamptz", 8, 8, "datetime", "date and time with timezone", false, UnknownType}
	case IntervalType:
		return TypeInfo{"interval", 16, 8, "timespan", "time interval", false, UnknownType}

	// Binary Types
	case ByteaType:
		return TypeInfo{"bytea", -1, 1, "binary", "binary data", false, UnknownType}

	// Network Address Types
	case InetType:
		return TypeInfo{"inet", -1, 1, "network", "IPv4 or IPv6 host address", false, UnknownType}
	case CidrType:
		return TypeInfo{"cidr", -1, 1, "network", "IPv4 or IPv6 network address", false, UnknownType}
	case MacAddrType:
		return TypeInfo{"macaddr", 6, 1, "network", "MAC address", false, UnknownType}

	// UUID Type
	case UuidType:
		return TypeInfo{"uuid", 16, 4, "uuid", "universally unique identifier", false, UnknownType}

	// JSON Types
	case JsonType:
		return TypeInfo{"json", -1, 1, "json", "textual JSON data", false, UnknownType}
	case JsonbType:
		return TypeInfo{"jsonb", -1, 1, "json", "binary JSON data", false, UnknownType}

	// Geometric Types
	case PointType:
		return TypeInfo{"point", 16, 8, "geometric", "point on a plane", false, UnknownType}
	case LineType:
		return TypeInfo{"line", 24, 8, "geometric", "infinite line", false, UnknownType}
	case LsegType:
		return TypeInfo{"lseg", 32, 8, "geometric", "line segment", false, UnknownType}
	case BoxType:
		return TypeInfo{"box", 32, 8, "geometric", "rectangular box", false, UnknownType}
	case PathType:
		return TypeInfo{"path", -1, 8, "geometric", "geometric path", false, UnknownType}
	case PolygonType:
		return TypeInfo{"polygon", -1, 8, "geometric", "closed geometric path", false, UnknownType}
	case CircleType:
		return TypeInfo{"circle", 24, 8, "geometric", "circle", false, UnknownType}

	// Array Types
	case IntArrayType:
		return TypeInfo{"integer[]", -1, 4, "array", "array of integers", true, IntType}
	case TextArrayType:
		return TypeInfo{"text[]", -1, 1, "array", "array of text", true, TextType}
	case VarcharArrayType:
		return TypeInfo{"varchar[]", -1, 1, "array", "array of varchar", true, VarcharType}
	case BoolArrayType:
		return TypeInfo{"boolean[]", -1, 1, "array", "array of booleans", true, BoolType}
	case FloatArrayType:
		return TypeInfo{"float[]", -1, 8, "array", "array of floats", true, FloatType}
	case DateArrayType:
		return TypeInfo{"date[]", -1, 4, "array", "array of dates", true, DateType}

	// Other Types
	case EnumType:
		return TypeInfo{"enum", 4, 4, "enum", "user-defined enumeration", false, UnknownType}
	case XmlType:
		return TypeInfo{"xml", -1, 1, "xml", "XML data", false, UnknownType}
	case CompositeType:
		return TypeInfo{"composite", -1, 8, "composite", "user-defined composite type", false, UnknownType}

	default:
		return TypeInfo{"unknown", -1, 1, "unknown", "unknown type", false, UnknownType}
	}
}

// IsNumericType checks if a type is numeric
func IsNumericType(dataType DataType) bool {
	return GetTypeInfo(dataType).Category == "numeric"
}

// IsStringType checks if a type is string-based
func IsStringType(dataType DataType) bool {
	return GetTypeInfo(dataType).Category == "string"
}

// IsDateTimeType checks if a type is date/time related
func IsDateTimeType(dataType DataType) bool {
	return GetTypeInfo(dataType).Category == "datetime" || GetTypeInfo(dataType).Category == "timespan"
}

// IsArrayType checks if a type is an array type
func IsArrayType(dataType DataType) bool {
	return GetTypeInfo(dataType).IsArray
}

// GetArrayElementType returns the element type for array types
func GetArrayElementType(dataType DataType) DataType {
	info := GetTypeInfo(dataType)
	if info.IsArray {
		return info.ElementType
	}
	return UnknownType
}

// ValidateTypeCompatibility checks if two types are compatible for operations
func ValidateTypeCompatibility(type1, type2 DataType) bool {
	// Same types are always compatible
	if type1 == type2 {
		return true
	}

	// Numeric types are generally compatible with each other
	if IsNumericType(type1) && IsNumericType(type2) {
		return true
	}

	// String types are compatible with each other
	if IsStringType(type1) && IsStringType(type2) {
		return true
	}

	// Date/time types have limited compatibility
	if IsDateTimeType(type1) && IsDateTimeType(type2) {
		return true
	}

	// Special compatibility rules
	compatiblePairs := map[DataType][]DataType{
		IntType:      {SmallIntType, BigIntType, RealType, DoubleType, NumericType},
		SmallIntType: {IntType, BigIntType, RealType, DoubleType, NumericType},
		BigIntType:   {IntType, SmallIntType, RealType, DoubleType, NumericType},
		CharType:     {VarcharType, TextType},
		VarcharType:  {CharType, TextType},
		TextType:     {CharType, VarcharType},
	}

	if compatible, exists := compatiblePairs[type1]; exists {
		return slices.Contains(compatible, type2)
	}

	return false
}

// GetDefaultValue returns the default value for a data type
func GetDefaultValue(dataType DataType) any {
	switch dataType {
	case SmallIntType, IntType, BigIntType, SerialType, BigSerialType:
		return 0
	case NumericType, DecimalType, RealType, DoubleType, FloatType:
		return 0.0
	case MoneyType:
		return "$0.00"
	case CharType, VarcharType, TextType:
		return ""
	case BoolType:
		return false
	case DateType:
		return time.Now().Format("2006-01-02")
	case TimeType:
		return time.Now().Format("15:04:05")
	case TimestampType, TimestampTzType:
		return time.Now()
	case IntervalType:
		return time.Duration(0)
	case ByteaType:
		return []byte{}
	case InetType:
		return net.IPv4(0, 0, 0, 0)
	case CidrType:
		return "0.0.0.0/0"
	case MacAddrType:
		return "00:00:00:00:00:00"
	case UuidType:
		return "00000000-0000-0000-0000-000000000000"
	case JsonType, JsonbType:
		return "{}"
	case XmlType:
		return "<root></root>"
	default:
		return nil
	}
}

// ParseTypeString converts a string representation to DataType
func ParseTypeString(typeStr string) (DataType, error) {
	typeMap := map[string]DataType{
		"smallint":          SmallIntType,
		"int2":              SmallIntType,
		"integer":           IntType,
		"int":               IntType,
		"int4":              IntType,
		"bigint":            BigIntType,
		"int8":              BigIntType,
		"serial":            SerialType,
		"serial4":           SerialType,
		"bigserial":         BigSerialType,
		"serial8":           BigSerialType,
		"numeric":           NumericType,
		"decimal":           DecimalType,
		"real":              RealType,
		"float4":            RealType,
		"double precision":  DoubleType,
		"float8":            DoubleType,
		"float":             FloatType,
		"money":             MoneyType,
		"char":              CharType,
		"character":         CharType,
		"varchar":           VarcharType,
		"character varying": VarcharType,
		"text":              TextType,
		"boolean":           BoolType,
		"bool":              BoolType,
		"date":              DateType,
		"time":              TimeType,
		"timestamp":         TimestampType,
		"timestamptz":       TimestampTzType,
		"interval":          IntervalType,
		"bytea":             ByteaType,
		"inet":              InetType,
		"cidr":              CidrType,
		"macaddr":           MacAddrType,
		"uuid":              UuidType,
		"json":              JsonType,
		"jsonb":             JsonbType,
		"point":             PointType,
		"line":              LineType,
		"lseg":              LsegType,
		"box":               BoxType,
		"path":              PathType,
		"polygon":           PolygonType,
		"circle":            CircleType,
		"xml":               XmlType,
	}

	if dataType, exists := typeMap[typeStr]; exists {
		return dataType, nil
	}

	return UnknownType, fmt.Errorf("unknown data type: %s", typeStr)
}

// TypeCategory represents categories of data types
type TypeCategory string

const (
	NumericCategory   TypeCategory = "numeric"
	StringCategory    TypeCategory = "string"
	BooleanCategory   TypeCategory = "boolean"
	DateTimeCategory  TypeCategory = "datetime"
	TimespanCategory  TypeCategory = "timespan"
	BinaryCategory    TypeCategory = "binary"
	NetworkCategory   TypeCategory = "network"
	UuidCategory      TypeCategory = "uuid"
	JsonCategory      TypeCategory = "json"
	GeometricCategory TypeCategory = "geometric"
	ArrayCategory     TypeCategory = "array"
	EnumCategory      TypeCategory = "enum"
	XmlCategory       TypeCategory = "xml"
	CompositeCategory TypeCategory = "composite"
	UnknownCategory   TypeCategory = "unknown"
)
