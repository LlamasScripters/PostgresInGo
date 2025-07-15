package parser

import (
	"fmt"
	"strings"

	"github.com/LlamasScripters/PostgresInGo/internal/types"
)

// Node represents any node in the AST
type Node interface {
	String() string
	Type() string
}

// Statement represents a SQL statement
type Statement interface {
	Node
	statementNode()
}

// Expression represents a SQL expression
type Expression interface {
	Node
	expressionNode()
}

// SQLStatement represents the root of a SQL AST
type SQLStatement struct {
	Statements []Statement
}

func (s *SQLStatement) String() string {
	var out strings.Builder
	for i, stmt := range s.Statements {
		if i > 0 {
			out.WriteString(";\n")
		}
		out.WriteString(stmt.String())
	}
	return out.String()
}

func (s *SQLStatement) Type() string { return "SQLStatement" }

// ==================== DDL Statements ====================

// CreateDatabaseStatement represents CREATE DATABASE statement
type CreateDatabaseStatement struct {
	Name string
}

func (cds *CreateDatabaseStatement) statementNode() {}
func (cds *CreateDatabaseStatement) String() string {
	return fmt.Sprintf("CREATE DATABASE %s", cds.Name)
}
func (cds *CreateDatabaseStatement) Type() string { return "CreateDatabaseStatement" }

// DropDatabaseStatement represents DROP DATABASE statement
type DropDatabaseStatement struct {
	Name string
}

func (dds *DropDatabaseStatement) statementNode() {}
func (dds *DropDatabaseStatement) String() string {
	return fmt.Sprintf("DROP DATABASE %s", dds.Name)
}
func (dds *DropDatabaseStatement) Type() string { return "DropDatabaseStatement" }

// CreateTableStatement represents CREATE TABLE statement
type CreateTableStatement struct {
	Name        string
	Columns     []*ColumnDefinition
	Constraints []*ConstraintDefinition
}

func (cts *CreateTableStatement) statementNode() {}
func (cts *CreateTableStatement) String() string {
	var out strings.Builder
	out.WriteString(fmt.Sprintf("CREATE TABLE %s (", cts.Name))

	var parts []string
	for _, col := range cts.Columns {
		parts = append(parts, col.String())
	}
	for _, constraint := range cts.Constraints {
		parts = append(parts, constraint.String())
	}

	out.WriteString(strings.Join(parts, ", "))
	out.WriteString(")")
	return out.String()
}
func (cts *CreateTableStatement) Type() string { return "CreateTableStatement" }

// DropTableStatement represents DROP TABLE statement
type DropTableStatement struct {
	Name string
}

func (dts *DropTableStatement) statementNode() {}
func (dts *DropTableStatement) String() string {
	return fmt.Sprintf("DROP TABLE %s", dts.Name)
}
func (dts *DropTableStatement) Type() string { return "DropTableStatement" }

// CreateIndexStatement represents CREATE INDEX statement
type CreateIndexStatement struct {
	Name    string
	Table   string
	Columns []string
	Unique  bool
}

func (cis *CreateIndexStatement) statementNode() {}
func (cis *CreateIndexStatement) String() string {
	unique := ""
	if cis.Unique {
		unique = "UNIQUE "
	}
	return fmt.Sprintf("CREATE %sINDEX %s ON %s (%s)",
		unique, cis.Name, cis.Table, strings.Join(cis.Columns, ", "))
}
func (cis *CreateIndexStatement) Type() string { return "CreateIndexStatement" }

// DropIndexStatement represents DROP INDEX statement
type DropIndexStatement struct {
	Name string
}

func (dis *DropIndexStatement) statementNode() {}
func (dis *DropIndexStatement) String() string {
	return fmt.Sprintf("DROP INDEX %s", dis.Name)
}
func (dis *DropIndexStatement) Type() string { return "DropIndexStatement" }

// ==================== DML Statements ====================

// SelectStatement represents SELECT statement
type SelectStatement struct {
	Distinct bool
	Columns  []Expression
	From     *FromClause
	Where    Expression
	GroupBy  []Expression
	Having   Expression
	OrderBy  []*OrderByClause
	Limit    Expression
	Offset   Expression
	Joins    []*JoinClause
}

func (ss *SelectStatement) statementNode() {}
func (ss *SelectStatement) String() string {
	var out strings.Builder

	out.WriteString("SELECT ")
	if ss.Distinct {
		out.WriteString("DISTINCT ")
	}

	// Columns
	var colStrs []string
	for _, col := range ss.Columns {
		colStrs = append(colStrs, col.String())
	}
	out.WriteString(strings.Join(colStrs, ", "))

	// FROM clause
	if ss.From != nil {
		out.WriteString(" ")
		out.WriteString(ss.From.String())
	}

	// JOINs
	for _, join := range ss.Joins {
		out.WriteString(" ")
		out.WriteString(join.String())
	}

	// WHERE clause
	if ss.Where != nil {
		out.WriteString(" WHERE ")
		out.WriteString(ss.Where.String())
	}

	// GROUP BY clause
	if len(ss.GroupBy) > 0 {
		out.WriteString(" GROUP BY ")
		var groupStrs []string
		for _, expr := range ss.GroupBy {
			groupStrs = append(groupStrs, expr.String())
		}
		out.WriteString(strings.Join(groupStrs, ", "))
	}

	// HAVING clause
	if ss.Having != nil {
		out.WriteString(" HAVING ")
		out.WriteString(ss.Having.String())
	}

	// ORDER BY clause
	if len(ss.OrderBy) > 0 {
		out.WriteString(" ORDER BY ")
		var orderStrs []string
		for _, order := range ss.OrderBy {
			orderStrs = append(orderStrs, order.String())
		}
		out.WriteString(strings.Join(orderStrs, ", "))
	}

	// LIMIT clause
	if ss.Limit != nil {
		out.WriteString(" LIMIT ")
		out.WriteString(ss.Limit.String())
	}

	// OFFSET clause
	if ss.Offset != nil {
		out.WriteString(" OFFSET ")
		out.WriteString(ss.Offset.String())
	}

	return out.String()
}
func (ss *SelectStatement) Type() string { return "SelectStatement" }

// InsertStatement represents INSERT statement
type InsertStatement struct {
	Table   string
	Columns []string
	Values  [][]Expression
}

func (is *InsertStatement) statementNode() {}
func (is *InsertStatement) String() string {
	var out strings.Builder
	out.WriteString(fmt.Sprintf("INSERT INTO %s", is.Table))

	if len(is.Columns) > 0 {
		out.WriteString(" (")
		out.WriteString(strings.Join(is.Columns, ", "))
		out.WriteString(")")
	}

	out.WriteString(" VALUES ")

	var valueStrs []string
	for _, valueRow := range is.Values {
		var rowStrs []string
		for _, value := range valueRow {
			rowStrs = append(rowStrs, value.String())
		}
		valueStrs = append(valueStrs, "("+strings.Join(rowStrs, ", ")+")")
	}
	out.WriteString(strings.Join(valueStrs, ", "))

	return out.String()
}
func (is *InsertStatement) Type() string { return "InsertStatement" }

// UpdateStatement represents UPDATE statement
type UpdateStatement struct {
	Table       string
	Assignments []*AssignmentExpression
	Where       Expression
}

func (us *UpdateStatement) statementNode() {}
func (us *UpdateStatement) String() string {
	var out strings.Builder
	out.WriteString(fmt.Sprintf("UPDATE %s SET ", us.Table))

	var assignStrs []string
	for _, assign := range us.Assignments {
		assignStrs = append(assignStrs, assign.String())
	}
	out.WriteString(strings.Join(assignStrs, ", "))

	if us.Where != nil {
		out.WriteString(" WHERE ")
		out.WriteString(us.Where.String())
	}

	return out.String()
}
func (us *UpdateStatement) Type() string { return "UpdateStatement" }

// DeleteStatement represents DELETE statement
type DeleteStatement struct {
	Table string
	Where Expression
}

func (ds *DeleteStatement) statementNode() {}
func (ds *DeleteStatement) String() string {
	var out strings.Builder
	out.WriteString(fmt.Sprintf("DELETE FROM %s", ds.Table))

	if ds.Where != nil {
		out.WriteString(" WHERE ")
		out.WriteString(ds.Where.String())
	}

	return out.String()
}
func (ds *DeleteStatement) Type() string { return "DeleteStatement" }

// CreateViewStatement represents CREATE VIEW statement
type CreateViewStatement struct {
	Name       string
	Columns    []string // Optional column list
	Query      *SelectStatement
	Definition string // Raw SQL for the view
}

func (cvs *CreateViewStatement) statementNode() {}
func (cvs *CreateViewStatement) String() string {
	if len(cvs.Columns) > 0 {
		return fmt.Sprintf("CREATE VIEW %s (%s) AS %s",
			cvs.Name, strings.Join(cvs.Columns, ", "), cvs.Query.String())
	}
	return fmt.Sprintf("CREATE VIEW %s AS %s", cvs.Name, cvs.Query.String())
}
func (cvs *CreateViewStatement) Type() string { return "CreateViewStatement" }

// DropViewStatement represents DROP VIEW statement
type DropViewStatement struct {
	Name string
}

func (dvs *DropViewStatement) statementNode() {}
func (dvs *DropViewStatement) String() string {
	return fmt.Sprintf("DROP VIEW %s", dvs.Name)
}
func (dvs *DropViewStatement) Type() string { return "DropViewStatement" }

// ==================== Expressions ====================

// Identifier represents an identifier (table name, column name, etc.)
type Identifier struct {
	Value string
}

func (i *Identifier) expressionNode() {}
func (i *Identifier) String() string  { return i.Value }
func (i *Identifier) Type() string    { return "Identifier" }

// QualifiedIdentifier represents a qualified identifier (table.column)
type QualifiedIdentifier struct {
	Table  string
	Column string
}

func (qi *QualifiedIdentifier) expressionNode() {}
func (qi *QualifiedIdentifier) String() string  { return fmt.Sprintf("%s.%s", qi.Table, qi.Column) }
func (qi *QualifiedIdentifier) Type() string    { return "QualifiedIdentifier" }

// Literal represents a literal value
type Literal struct {
	Value interface{}
}

func (l *Literal) expressionNode() {}
func (l *Literal) String() string {
	switch v := l.Value.(type) {
	case string:
		return fmt.Sprintf("'%s'", v)
	case nil:
		return "NULL"
	default:
		return fmt.Sprintf("%v", v)
	}
}
func (l *Literal) Type() string { return "Literal" }

// BinaryExpression represents binary operations (=, <, >, AND, OR, etc.)
type BinaryExpression struct {
	Left     Expression
	Operator string
	Right    Expression
}

func (be *BinaryExpression) expressionNode() {}
func (be *BinaryExpression) String() string {
	return fmt.Sprintf("(%s %s %s)", be.Left.String(), be.Operator, be.Right.String())
}
func (be *BinaryExpression) Type() string { return "BinaryExpression" }

// UnaryExpression represents unary operations (NOT, -, etc.)
type UnaryExpression struct {
	Operator string
	Operand  Expression
}

func (ue *UnaryExpression) expressionNode() {}
func (ue *UnaryExpression) String() string {
	return fmt.Sprintf("%s %s", ue.Operator, ue.Operand.String())
}
func (ue *UnaryExpression) Type() string { return "UnaryExpression" }

// FunctionCall represents function calls (COUNT, SUM, etc.)
type FunctionCall struct {
	Name      string
	Arguments []Expression
	Distinct  bool
}

func (fc *FunctionCall) expressionNode() {}
func (fc *FunctionCall) String() string {
	var out strings.Builder
	out.WriteString(fc.Name)
	out.WriteString("(")

	if fc.Distinct {
		out.WriteString("DISTINCT ")
	}

	var argStrs []string
	for _, arg := range fc.Arguments {
		argStrs = append(argStrs, arg.String())
	}
	out.WriteString(strings.Join(argStrs, ", "))
	out.WriteString(")")

	return out.String()
}
func (fc *FunctionCall) Type() string { return "FunctionCall" }

// AssignmentExpression represents column = value assignments in UPDATE
type AssignmentExpression struct {
	Column string
	Value  Expression
}

func (ae *AssignmentExpression) expressionNode() {}
func (ae *AssignmentExpression) String() string {
	return fmt.Sprintf("%s = %s", ae.Column, ae.Value.String())
}
func (ae *AssignmentExpression) Type() string { return "AssignmentExpression" }

// ==================== Clauses ====================

// FromClause represents FROM table
type FromClause struct {
	Table string
	Alias string
}

func (fc *FromClause) String() string {
	if fc.Alias != "" {
		return fmt.Sprintf("FROM %s AS %s", fc.Table, fc.Alias)
	}
	return fmt.Sprintf("FROM %s", fc.Table)
}

// JoinClause represents JOIN operations
type JoinClause struct {
	Type      string // INNER, LEFT, RIGHT, FULL OUTER
	Table     string
	Alias     string
	Condition Expression
}

func (jc *JoinClause) String() string {
	var out strings.Builder
	out.WriteString(jc.Type)
	out.WriteString(" JOIN ")
	out.WriteString(jc.Table)

	if jc.Alias != "" {
		out.WriteString(" AS ")
		out.WriteString(jc.Alias)
	}

	if jc.Condition != nil {
		out.WriteString(" ON ")
		out.WriteString(jc.Condition.String())
	}

	return out.String()
}

// OrderByClause represents ORDER BY items
type OrderByClause struct {
	Expression Expression
	Direction  string // ASC or DESC
}

func (obc *OrderByClause) String() string {
	if obc.Direction != "" {
		return fmt.Sprintf("%s %s", obc.Expression.String(), obc.Direction)
	}
	return obc.Expression.String()
}

// ==================== Column and Constraint Definitions ====================

// ColumnDefinition represents a column definition in CREATE TABLE
type ColumnDefinition struct {
	Name          string
	DataType      DataTypeDefinition
	Nullable      *bool // nil means not specified, true/false for NULL/NOT NULL
	Default       Expression
	AutoIncrement bool
	PrimaryKey    bool
	Unique        bool
}

func (cd *ColumnDefinition) String() string {
	var out strings.Builder
	out.WriteString(cd.Name)
	out.WriteString(" ")
	out.WriteString(cd.DataType.String())

	if cd.Nullable != nil {
		if *cd.Nullable {
			out.WriteString(" NULL")
		} else {
			out.WriteString(" NOT NULL")
		}
	}

	if cd.Default != nil {
		out.WriteString(" DEFAULT ")
		out.WriteString(cd.Default.String())
	}

	if cd.AutoIncrement {
		out.WriteString(" AUTO_INCREMENT")
	}

	if cd.PrimaryKey {
		out.WriteString(" PRIMARY KEY")
	}

	if cd.Unique {
		out.WriteString(" UNIQUE")
	}

	return out.String()
}

// DataTypeDefinition represents a data type with optional size/precision
type DataTypeDefinition struct {
	Type      string
	Size      *int
	Precision *int
	Scale     *int
}

func (dtd *DataTypeDefinition) String() string {
	var out strings.Builder
	out.WriteString(dtd.Type)

	if dtd.Size != nil {
		out.WriteString(fmt.Sprintf("(%d", *dtd.Size))
		if dtd.Scale != nil {
			out.WriteString(fmt.Sprintf(",%d", *dtd.Scale))
		}
		out.WriteString(")")
	} else if dtd.Precision != nil {
		out.WriteString(fmt.Sprintf("(%d", *dtd.Precision))
		if dtd.Scale != nil {
			out.WriteString(fmt.Sprintf(",%d", *dtd.Scale))
		}
		out.WriteString(")")
	}

	return out.String()
}

// ConstraintDefinition represents table constraints
type ConstraintDefinition struct {
	Name       string
	Type       string // PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK
	Columns    []string
	RefTable   string
	RefColumns []string
	OnDelete   string
	OnUpdate   string
	CheckExpr  Expression
}

func (cd *ConstraintDefinition) String() string {
	var out strings.Builder

	if cd.Name != "" {
		out.WriteString("CONSTRAINT ")
		out.WriteString(cd.Name)
		out.WriteString(" ")
	}

	switch cd.Type {
	case "PRIMARY KEY":
		out.WriteString("PRIMARY KEY (")
		out.WriteString(strings.Join(cd.Columns, ", "))
		out.WriteString(")")
	case "FOREIGN KEY":
		out.WriteString("FOREIGN KEY (")
		out.WriteString(strings.Join(cd.Columns, ", "))
		out.WriteString(") REFERENCES ")
		out.WriteString(cd.RefTable)
		out.WriteString(" (")
		out.WriteString(strings.Join(cd.RefColumns, ", "))
		out.WriteString(")")
		if cd.OnDelete != "" {
			out.WriteString(" ON DELETE ")
			out.WriteString(cd.OnDelete)
		}
		if cd.OnUpdate != "" {
			out.WriteString(" ON UPDATE ")
			out.WriteString(cd.OnUpdate)
		}
	case "UNIQUE":
		out.WriteString("UNIQUE (")
		out.WriteString(strings.Join(cd.Columns, ", "))
		out.WriteString(")")
	case "CHECK":
		out.WriteString("CHECK (")
		if cd.CheckExpr != nil {
			out.WriteString(cd.CheckExpr.String())
		}
		out.WriteString(")")
	}

	return out.String()
}

// ==================== Helper Functions ====================

// ConvertDataType converts AST data type to internal type system
func ConvertDataType(dtd DataTypeDefinition) types.DataType {
	switch strings.ToUpper(dtd.Type) {
	case "INT", "INTEGER":
		return types.IntType
	case "VARCHAR":
		return types.VarcharType
	case "TEXT":
		return types.TextType
	case "BOOLEAN", "BOOL":
		return types.BoolType
	case "DATE":
		return types.DateType
	case "TIME":
		return types.TimeType
	case "TIMESTAMP":
		return types.TimestampType
	case "FLOAT", "REAL":
		return types.FloatType
	case "DECIMAL", "NUMERIC":
		return types.NumericType
	default:
		return types.UnknownType
	}
}

// ConvertColumnDefinition converts AST column to internal column
func ConvertColumnDefinition(cd *ColumnDefinition) types.Column {
	column := types.Column{
		Name:          cd.Name,
		Type:          ConvertDataType(cd.DataType),
		Nullable:      cd.Nullable == nil || *cd.Nullable,
		IsPrimaryKey:  cd.PrimaryKey,
		IsUnique:      cd.Unique,
		AutoIncrement: cd.AutoIncrement,
	}

	if cd.DataType.Size != nil {
		column.Size = *cd.DataType.Size
	}

	if cd.Default != nil {
		// Convert default value expression to actual value
		if lit, ok := cd.Default.(*Literal); ok {
			column.Default = lit.Value
		}
	}

	return column
}

// ConvertConstraintDefinition converts AST constraint to internal constraint
func ConvertConstraintDefinition(cd *ConstraintDefinition) types.Constraint {
	constraint := types.Constraint{
		Name:         cd.Name,
		Columns:      cd.Columns,
		RefTable:     cd.RefTable,
		RefColumns:   cd.RefColumns,
		OnDeleteRule: cd.OnDelete,
		OnUpdateRule: cd.OnUpdate,
	}

	switch cd.Type {
	case "PRIMARY KEY":
		constraint.Type = types.PrimaryKeyConstraint
	case "FOREIGN KEY":
		constraint.Type = types.ForeignKeyConstraint
	case "UNIQUE":
		constraint.Type = types.UniqueConstraint
	case "CHECK":
		constraint.Type = types.CheckConstraint
		if cd.CheckExpr != nil {
			constraint.CheckExpr = cd.CheckExpr.String()
		}
	}

	return constraint
}

// ==================== Visitor Pattern ====================

// Visitor interface for traversing the AST
type Visitor interface {
	VisitStatement(stmt Statement) interface{}
	VisitExpression(expr Expression) interface{}
}

// Accept method for statements
func (s *CreateTableStatement) Accept(v Visitor) interface{} {
	return v.VisitStatement(s)
}

func (s *SelectStatement) Accept(v Visitor) interface{} {
	return v.VisitStatement(s)
}

func (s *InsertStatement) Accept(v Visitor) interface{} {
	return v.VisitStatement(s)
}

func (s *UpdateStatement) Accept(v Visitor) interface{} {
	return v.VisitStatement(s)
}

func (s *DeleteStatement) Accept(v Visitor) interface{} {
	return v.VisitStatement(s)
}

func (s *CreateViewStatement) Accept(v Visitor) interface{} {
	return v.VisitStatement(s)
}

func (s *DropViewStatement) Accept(v Visitor) interface{} {
	return v.VisitStatement(s)
}

// Accept method for expressions
func (e *Identifier) Accept(v Visitor) interface{} {
	return v.VisitExpression(e)
}

func (e *Literal) Accept(v Visitor) interface{} {
	return v.VisitExpression(e)
}

func (e *BinaryExpression) Accept(v Visitor) interface{} {
	return v.VisitExpression(e)
}

func (e *FunctionCall) Accept(v Visitor) interface{} {
	return v.VisitExpression(e)
}

// ==================== Utility Functions ====================

// GetTableNames extracts all table names referenced in a statement
func GetTableNames(stmt Statement) []string {
	var tables []string

	switch s := stmt.(type) {
	case *CreateTableStatement:
		tables = append(tables, s.Name)
	case *SelectStatement:
		if s.From != nil {
			tables = append(tables, s.From.Table)
		}
		for _, join := range s.Joins {
			tables = append(tables, join.Table)
		}
	case *InsertStatement:
		tables = append(tables, s.Table)
	case *UpdateStatement:
		tables = append(tables, s.Table)
	case *DeleteStatement:
		tables = append(tables, s.Table)
	}

	return tables
}

// GetColumnNames extracts all column names referenced in a statement
func GetColumnNames(stmt Statement) []string {
	var columns []string

	switch s := stmt.(type) {
	case *CreateTableStatement:
		for _, col := range s.Columns {
			columns = append(columns, col.Name)
		}
	case *InsertStatement:
		columns = append(columns, s.Columns...)
	case *UpdateStatement:
		for _, assign := range s.Assignments {
			columns = append(columns, assign.Column)
		}
	}

	return columns
}

// IsReadOnlyStatement checks if a statement only reads data
func IsReadOnlyStatement(stmt Statement) bool {
	switch stmt.(type) {
	case *SelectStatement:
		return true
	default:
		return false
	}
}

// IsDDLStatement checks if a statement is a DDL statement
func IsDDLStatement(stmt Statement) bool {
	switch stmt.(type) {
	case *CreateTableStatement, *DropTableStatement, *CreateIndexStatement, *DropIndexStatement,
		*CreateDatabaseStatement, *DropDatabaseStatement, *CreateViewStatement, *DropViewStatement:
		return true
	default:
		return false
	}
}

// IsDMLStatement checks if a statement is a DML statement
func IsDMLStatement(stmt Statement) bool {
	switch stmt.(type) {
	case *SelectStatement, *InsertStatement, *UpdateStatement, *DeleteStatement:
		return true
	default:
		return false
	}
}
