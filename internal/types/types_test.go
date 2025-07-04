package types

import (
	"testing"
)

func TestGetTypeInfo(t *testing.T) {
	tests := []struct {
		dataType DataType
		expected TypeInfo
	}{
		{IntType, TypeInfo{"integer", 4, 4, "numeric", "32-bit signed integer", false, UnknownType}},
		{VarcharType, TypeInfo{"varchar", -1, 1, "string", "variable-length character string", false, UnknownType}},
		{BoolType, TypeInfo{"boolean", 1, 1, "boolean", "logical boolean", false, UnknownType}},
		{DateType, TypeInfo{"date", 4, 4, "datetime", "calendar date (year, month, day)", false, UnknownType}},
		{JsonType, TypeInfo{"json", -1, 1, "json", "textual JSON data", false, UnknownType}},
		{UuidType, TypeInfo{"uuid", 16, 4, "uuid", "universally unique identifier", false, UnknownType}},
		{IntArrayType, TypeInfo{"integer[]", -1, 4, "array", "array of integers", true, IntType}},
	}

	for _, test := range tests {
		info := GetTypeInfo(test.dataType)
		if info.Name != test.expected.Name {
			t.Errorf("Expected name %s, got %s for type %d", test.expected.Name, info.Name, test.dataType)
		}
		if info.Category != test.expected.Category {
			t.Errorf("Expected category %s, got %s for type %d", test.expected.Category, info.Category, test.dataType)
		}
		if info.IsArray != test.expected.IsArray {
			t.Errorf("Expected IsArray %v, got %v for type %d", test.expected.IsArray, info.IsArray, test.dataType)
		}
	}
}

func TestIsNumericType(t *testing.T) {
	numericTypes := []DataType{SmallIntType, IntType, BigIntType, RealType, DoubleType, NumericType}
	nonNumericTypes := []DataType{VarcharType, BoolType, DateType, JsonType}

	for _, dataType := range numericTypes {
		if !IsNumericType(dataType) {
			t.Errorf("Expected %d to be numeric type", dataType)
		}
	}

	for _, dataType := range nonNumericTypes {
		if IsNumericType(dataType) {
			t.Errorf("Expected %d to not be numeric type", dataType)
		}
	}
}

func TestIsStringType(t *testing.T) {
	stringTypes := []DataType{CharType, VarcharType, TextType}
	nonStringTypes := []DataType{IntType, BoolType, DateType, JsonType}

	for _, dataType := range stringTypes {
		if !IsStringType(dataType) {
			t.Errorf("Expected %d to be string type", dataType)
		}
	}

	for _, dataType := range nonStringTypes {
		if IsStringType(dataType) {
			t.Errorf("Expected %d to not be string type", dataType)
		}
	}
}

func TestIsArrayType(t *testing.T) {
	arrayTypes := []DataType{IntArrayType, TextArrayType, BoolArrayType}
	nonArrayTypes := []DataType{IntType, VarcharType, BoolType}

	for _, dataType := range arrayTypes {
		if !IsArrayType(dataType) {
			t.Errorf("Expected %d to be array type", dataType)
		}
	}

	for _, dataType := range nonArrayTypes {
		if IsArrayType(dataType) {
			t.Errorf("Expected %d to not be array type", dataType)
		}
	}
}

func TestValidateTypeCompatibility(t *testing.T) {
	// Same types should be compatible
	if !ValidateTypeCompatibility(IntType, IntType) {
		t.Error("Same types should be compatible")
	}

	// Numeric types should be compatible
	if !ValidateTypeCompatibility(IntType, BigIntType) {
		t.Error("Numeric types should be compatible")
	}

	// String types should be compatible
	if !ValidateTypeCompatibility(VarcharType, TextType) {
		t.Error("String types should be compatible")
	}

	// Incompatible types
	if ValidateTypeCompatibility(IntType, BoolType) {
		t.Error("Int and Bool should not be compatible")
	}
}

func TestParseTypeString(t *testing.T) {
	tests := []struct {
		input    string
		expected DataType
		hasError bool
	}{
		{"integer", IntType, false},
		{"int", IntType, false},
		{"varchar", VarcharType, false},
		{"text", TextType, false},
		{"boolean", BoolType, false},
		{"json", JsonType, false},
		{"uuid", UuidType, false},
		{"unknown_type", UnknownType, true},
	}

	for _, test := range tests {
		result, err := ParseTypeString(test.input)
		if test.hasError {
			if err == nil {
				t.Errorf("Expected error for input %s", test.input)
			}
		} else {
			if err != nil {
				t.Errorf("Unexpected error for input %s: %v", test.input, err)
			}
			if result != test.expected {
				t.Errorf("Expected %d, got %d for input %s", test.expected, result, test.input)
			}
		}
	}
}

func TestGetDefaultValue(t *testing.T) {
	tests := []struct {
		dataType DataType
		expected any
	}{
		{IntType, 0},
		{VarcharType, ""},
		{BoolType, false},
		{ByteaType, []byte{}},
	}

	for _, test := range tests {
		result := GetDefaultValue(test.dataType)
		switch expected := test.expected.(type) {
		case int:
			if result.(int) != expected {
				t.Errorf("Expected %v, got %v for type %d", expected, result, test.dataType)
			}
		case string:
			if result.(string) != expected {
				t.Errorf("Expected %v, got %v for type %d", expected, result, test.dataType)
			}
		case bool:
			if result.(bool) != expected {
				t.Errorf("Expected %v, got %v for type %d", expected, result, test.dataType)
			}
		case []byte:
			if len(result.([]byte)) != len(expected) {
				t.Errorf("Expected %v, got %v for type %d", expected, result, test.dataType)
			}
		}
	}
}

func TestGetArrayElementType(t *testing.T) {
	if GetArrayElementType(IntArrayType) != IntType {
		t.Error("IntArrayType should have IntType as element type")
	}

	if GetArrayElementType(TextArrayType) != TextType {
		t.Error("TextArrayType should have TextType as element type")
	}

	if GetArrayElementType(IntType) != UnknownType {
		t.Error("Non-array type should return UnknownType")
	}
}

func TestTableHelperMethods(t *testing.T) {
	// Test table with primary key constraint
	table := &Table{
		PrimaryKey: &Constraint{
			Columns: []string{"id", "name"},
		},
		Schema: Schema{
			Columns: []Column{
				{Name: "id", IsPrimaryKey: true},
				{Name: "name", IsPrimaryKey: false},
			},
		},
		ForeignKeys: []*Constraint{
			{
				Columns:  []string{"user_id"},
				RefTable: "users",
			},
		},
	}

	// Test GetPrimaryKeyColumns
	pkCols := table.GetPrimaryKeyColumns()
	if len(pkCols) != 2 || pkCols[0] != "id" || pkCols[1] != "name" {
		t.Errorf("Expected [id, name], got %v", pkCols)
	}

	// Test HasPrimaryKey
	if !table.HasPrimaryKey() {
		t.Error("Table should have primary key")
	}

	// Test GetForeignKeyFor
	fk := table.GetForeignKeyFor("user_id")
	if fk == nil {
		t.Error("Should find foreign key for user_id")
	}
	if fk.RefTable != "users" {
		t.Errorf("Expected ref table 'users', got '%s'", fk.RefTable)
	}

	// Test non-existent foreign key
	fk = table.GetForeignKeyFor("non_existent")
	if fk != nil {
		t.Error("Should not find foreign key for non-existent column")
	}
}

func TestSchemaHelperMethods(t *testing.T) {
	schema := Schema{
		Columns: []Column{
			{Name: "id", Type: IntType},
			{Name: "name", Type: VarcharType},
			{Name: "created_at", Type: TimestampType},
		},
	}

	// Test GetColumnByName
	col := schema.GetColumnByName("name")
	if col == nil {
		t.Error("Should find column 'name'")
	}
	if col.Type != VarcharType {
		t.Errorf("Expected VarcharType, got %d", col.Type)
	}

	// Test non-existent column
	col = schema.GetColumnByName("non_existent")
	if col != nil {
		t.Error("Should not find non-existent column")
	}

	// Test HasColumn
	if !schema.HasColumn("id") {
		t.Error("Schema should have column 'id'")
	}
	if schema.HasColumn("non_existent") {
		t.Error("Schema should not have non-existent column")
	}

	// Test GetColumnIndex
	index := schema.GetColumnIndex("created_at")
	if index != 2 {
		t.Errorf("Expected index 2, got %d", index)
	}

	index = schema.GetColumnIndex("non_existent")
	if index != -1 {
		t.Errorf("Expected index -1 for non-existent column, got %d", index)
	}
}

func TestDateTimeTypes(t *testing.T) {
	dateTimeTypes := []DataType{DateType, TimeType, TimestampType, TimestampTzType}
	for _, dataType := range dateTimeTypes {
		if !IsDateTimeType(dataType) {
			t.Errorf("Expected %d to be datetime type", dataType)
		}
	}

	// Test interval as timespan
	if !IsDateTimeType(IntervalType) {
		t.Error("IntervalType should be considered datetime type")
	}
}

func BenchmarkGetTypeInfo(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = GetTypeInfo(IntType)
	}
}

func BenchmarkValidateTypeCompatibility(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = ValidateTypeCompatibility(IntType, BigIntType)
	}
}
