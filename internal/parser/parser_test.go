package parser

import (
	"testing"
)

// TestParserDDLStatements tests parsing of DDL statements
func TestParserDDLStatements(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "CreateDatabase",
			sql:  "CREATE DATABASE testdb;",
		},
		{
			name: "CreateTable",
			sql:  "CREATE TABLE users (id INT NOT NULL, name VARCHAR(50), PRIMARY KEY (id));",
		},
		{
			name: "CreateIndex",
			sql:  "CREATE INDEX idx_name ON users (name);",
		},
		{
			name: "CreateUniqueIndex",
			sql:  "CREATE UNIQUE INDEX idx_unique ON users (email);",
		},
		{
			name: "DropTable",
			sql:  "DROP TABLE users;",
		},
		{
			name: "DropDatabase",
			sql:  "DROP DATABASE testdb;",
		},
		{
			name: "DropIndex",
			sql:  "DROP INDEX idx_name;",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parser := NewParser(test.sql)
			stmt, err := parser.Parse()

			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			if len(stmt.Statements) != 1 {
				t.Fatalf("Expected 1 statement, got %d", len(stmt.Statements))
			}

			// Basic check that parsing succeeded
			if stmt.Statements[0].String() == "" {
				t.Error("Statement string representation is empty")
			}
		})
	}
}

// TestParserDMLStatements tests parsing of DML statements
func TestParserDMLStatements(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		expectedType interface{}
	}{
		{
			name:         "Select",
			sql:          "SELECT * FROM users;",
			expectedType: &SelectStatement{},
		},
		{
			name:         "SelectWithWhere",
			sql:          "SELECT id, name FROM users WHERE id = 1;",
			expectedType: &SelectStatement{},
		},
		{
			name:         "Insert",
			sql:          "INSERT INTO users (id, name) VALUES (1, 'Alice');",
			expectedType: &InsertStatement{},
		},
		{
			name:         "Update",
			sql:          "UPDATE users SET name = 'Bob' WHERE id = 1;",
			expectedType: &UpdateStatement{},
		},
		{
			name:         "Delete",
			sql:          "DELETE FROM users WHERE id = 1;",
			expectedType: &DeleteStatement{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parser := NewParser(test.sql)
			stmt, err := parser.Parse()

			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			if len(stmt.Statements) != 1 {
				t.Fatalf("Expected 1 statement, got %d", len(stmt.Statements))
			}

			// Check statement type
			statement := stmt.Statements[0]
			switch test.expectedType.(type) {
			case *SelectStatement:
				if _, ok := statement.(*SelectStatement); !ok {
					t.Errorf("Expected SelectStatement, got %T", statement)
				}
			case *InsertStatement:
				if _, ok := statement.(*InsertStatement); !ok {
					t.Errorf("Expected InsertStatement, got %T", statement)
				}
			case *UpdateStatement:
				if _, ok := statement.(*UpdateStatement); !ok {
					t.Errorf("Expected UpdateStatement, got %T", statement)
				}
			case *DeleteStatement:
				if _, ok := statement.(*DeleteStatement); !ok {
					t.Errorf("Expected DeleteStatement, got %T", statement)
				}
			}
		})
	}
}

// TestParserComplexQueries tests parsing of complex queries
func TestParserComplexQueries(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "SelectWithJoin",
			sql:  "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id;",
		},
		{
			name: "SelectWithGroupBy",
			sql:  "SELECT category, COUNT(*) FROM products GROUP BY category;",
		},
		{
			name: "SelectWithOrderBy",
			sql:  "SELECT * FROM users ORDER BY name ASC, id DESC;",
		},
		{
			name: "SelectWithLimit",
			sql:  "SELECT * FROM users LIMIT 10;",
		},
		{
			name: "SelectWithOffset",
			sql:  "SELECT * FROM users LIMIT 10 OFFSET 5;",
		},
		{
			name: "SelectWithDistinct",
			sql:  "SELECT DISTINCT category FROM products;",
		},
		{
			name: "SelectWithHaving",
			sql:  "SELECT category, COUNT(*) FROM products GROUP BY category HAVING COUNT(*) > 5;",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parser := NewParser(test.sql)
			stmt, err := parser.Parse()

			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			if len(stmt.Statements) != 1 {
				t.Fatalf("Expected 1 statement, got %d", len(stmt.Statements))
			}

			selectStmt, ok := stmt.Statements[0].(*SelectStatement)
			if !ok {
				t.Fatalf("Expected SelectStatement, got %T", stmt.Statements[0])
			}

			// Verify structure based on query type
			switch test.name {
			case "SelectWithJoin":
				if len(selectStmt.Joins) == 0 {
					t.Error("Expected JOIN clause")
				}
			case "SelectWithGroupBy":
				if len(selectStmt.GroupBy) == 0 {
					t.Error("Expected GROUP BY clause")
				}
			case "SelectWithOrderBy":
				if len(selectStmt.OrderBy) == 0 {
					t.Error("Expected ORDER BY clause")
				}
			case "SelectWithLimit":
				if selectStmt.Limit == nil {
					t.Error("Expected LIMIT clause")
				}
			case "SelectWithOffset":
				if selectStmt.Offset == nil {
					t.Error("Expected OFFSET clause")
				}
			case "SelectWithDistinct":
				if !selectStmt.Distinct {
					t.Error("Expected DISTINCT flag")
				}
			case "SelectWithHaving":
				if selectStmt.Having == nil {
					t.Error("Expected HAVING clause")
				}
			}
		})
	}
}

// TestParserMultipleStatements tests parsing multiple statements
func TestParserMultipleStatements(t *testing.T) {
	sql := `
		CREATE TABLE users (id INT);
		INSERT INTO users VALUES (1);
		SELECT * FROM users;
	`

	parser := NewParser(sql)
	stmt, err := parser.Parse()

	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	if len(stmt.Statements) != 3 {
		t.Errorf("Expected 3 statements, got %d", len(stmt.Statements))
	}

	// Check statement types
	if _, ok := stmt.Statements[0].(*CreateTableStatement); !ok {
		t.Error("First statement should be CREATE TABLE")
	}
	if _, ok := stmt.Statements[1].(*InsertStatement); !ok {
		t.Error("Second statement should be INSERT")
	}
	if _, ok := stmt.Statements[2].(*SelectStatement); !ok {
		t.Error("Third statement should be SELECT")
	}
}

// TestParserErrorHandling tests error handling
func TestParserErrorHandling(t *testing.T) {
	invalidQueries := []struct {
		name  string
		query string
	}{
		{
			name:  "MissingTable",
			query: "SELECT FROM;",
		},
		{
			name:  "InvalidCreateTable",
			query: "CREATE TABLE ();",
		},
		{
			name:  "InvalidInsert",
			query: "INSERT INTO VALUES;",
		},
		{
			name:  "InvalidUpdate",
			query: "UPDATE SET;",
		},
		{
			name:  "InvalidDelete",
			query: "DELETE;",
		},
		{
			name:  "UnexpectedToken",
			query: "SELECT * FROM users WHERE id = = 1;",
		},
	}

	for _, test := range invalidQueries {
		t.Run(test.name, func(t *testing.T) {
			parser := NewParser(test.query)
			_, err := parser.Parse()

			if err == nil {
				t.Error("Expected parse error for invalid query")
			}
		})
	}
}

// TestParserEdgeCases tests edge cases
func TestParserEdgeCases(t *testing.T) {
	t.Run("EmptyInput", func(t *testing.T) {
		parser := NewParser("")
		stmt, err := parser.Parse()

		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if len(stmt.Statements) != 0 {
			t.Errorf("Expected 0 statements, got %d", len(stmt.Statements))
		}
	})

	t.Run("WhitespaceOnly", func(t *testing.T) {
		parser := NewParser("   \n\t\r   ")
		stmt, err := parser.Parse()

		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if len(stmt.Statements) != 0 {
			t.Errorf("Expected 0 statements, got %d", len(stmt.Statements))
		}
	})

	t.Run("SemicolonsOnly", func(t *testing.T) {
		parser := NewParser(";;;")
		stmt, err := parser.Parse()

		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if len(stmt.Statements) != 0 {
			t.Errorf("Expected 0 statements, got %d", len(stmt.Statements))
		}
	})

	t.Run("CaseInsensitiveKeywords", func(t *testing.T) {
		queries := []string{
			"select * from users;",
			"SELECT * FROM USERS;",
			"Select * From Users;",
			"sElEcT * fRoM uSeRs;",
		}

		for i, query := range queries {
			t.Run("Case"+string(rune(i)), func(t *testing.T) {
				parser := NewParser(query)
				stmt, err := parser.Parse()

				if err != nil {
					t.Fatalf("Parse error: %v", err)
				}

				if len(stmt.Statements) != 1 {
					t.Fatalf("Expected 1 statement, got %d", len(stmt.Statements))
				}

				if _, ok := stmt.Statements[0].(*SelectStatement); !ok {
					t.Error("Expected SelectStatement")
				}
			})
		}
	})
}

// TestParserCreateTableWithConstraints tests CREATE TABLE with constraints
func TestParserCreateTableWithConstraints(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "PrimaryKey",
			sql:  "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50));",
		},
		{
			name: "NotNull",
			sql:  "CREATE TABLE users (id INT NOT NULL, name VARCHAR(50));",
		},
		{
			name: "Unique",
			sql:  "CREATE TABLE users (id INT UNIQUE, name VARCHAR(50));",
		},
		{
			name: "Default",
			sql:  "CREATE TABLE users (id INT DEFAULT 0, name VARCHAR(50));",
		},
		{
			name: "AutoIncrement",
			sql:  "CREATE TABLE users (id INT AUTO_INCREMENT, name VARCHAR(50));",
		},
		{
			name: "MultiplePrimaryKey",
			sql:  "CREATE TABLE users (id INT, name VARCHAR(50), PRIMARY KEY (id));",
		},
		{
			name: "ForeignKey",
			sql:  "CREATE TABLE orders (id INT, user_id INT, FOREIGN KEY (user_id) REFERENCES users(id));",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parser := NewParser(test.sql)
			stmt, err := parser.Parse()

			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			if len(stmt.Statements) != 1 {
				t.Fatalf("Expected 1 statement, got %d", len(stmt.Statements))
			}

			createStmt, ok := stmt.Statements[0].(*CreateTableStatement)
			if !ok {
				t.Fatalf("Expected CreateTableStatement, got %T", stmt.Statements[0])
			}

			if createStmt.Name == "" {
				t.Error("Table name should not be empty")
			}

			if len(createStmt.Columns) == 0 {
				t.Error("Should have at least one column")
			}
		})
	}
}

// TestParserDataTypes tests data type parsing
func TestParserDataTypes(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		colType string
	}{
		{
			name:    "INT",
			sql:     "CREATE TABLE test (id INT);",
			colType: "INT",
		},
		{
			name:    "VARCHAR",
			sql:     "CREATE TABLE test (name VARCHAR(50));",
			colType: "VARCHAR",
		},
		{
			name:    "TEXT",
			sql:     "CREATE TABLE test (description TEXT);",
			colType: "TEXT",
		},
		{
			name:    "BOOLEAN",
			sql:     "CREATE TABLE test (active BOOLEAN);",
			colType: "BOOLEAN",
		},
		{
			name:    "FLOAT",
			sql:     "CREATE TABLE test (price FLOAT);",
			colType: "FLOAT",
		},
		{
			name:    "DECIMAL",
			sql:     "CREATE TABLE test (price DECIMAL(10,2));",
			colType: "DECIMAL",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parser := NewParser(test.sql)
			stmt, err := parser.Parse()

			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			if len(stmt.Statements) != 1 {
				t.Fatalf("Expected 1 statement, got %d", len(stmt.Statements))
			}

			createStmt, ok := stmt.Statements[0].(*CreateTableStatement)
			if !ok {
				t.Fatalf("Expected CreateTableStatement, got %T", stmt.Statements[0])
			}

			if len(createStmt.Columns) != 1 {
				t.Fatalf("Expected 1 column, got %d", len(createStmt.Columns))
			}

			if createStmt.Columns[0].DataType.Type != test.colType {
				t.Errorf("Expected data type %s, got %s", test.colType, createStmt.Columns[0].DataType.Type)
			}
		})
	}
}

// TestParserInsertMultipleValues tests INSERT with multiple value rows
func TestParserInsertMultipleValues(t *testing.T) {
	sql := "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');"

	parser := NewParser(sql)
	stmt, err := parser.Parse()

	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	if len(stmt.Statements) != 1 {
		t.Fatalf("Expected 1 statement, got %d", len(stmt.Statements))
	}

	insertStmt, ok := stmt.Statements[0].(*InsertStatement)
	if !ok {
		t.Fatalf("Expected InsertStatement, got %T", stmt.Statements[0])
	}

	if len(insertStmt.Values) != 3 {
		t.Errorf("Expected 3 value rows, got %d", len(insertStmt.Values))
	}

	if len(insertStmt.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(insertStmt.Columns))
	}
}

// TestParserFunctionCalls tests function call parsing
func TestParserFunctionCalls(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		functionName string
	}{
		{
			name:         "COUNT",
			sql:          "SELECT COUNT(*) FROM users;",
			functionName: "COUNT",
		},
		{
			name:         "SUM",
			sql:          "SELECT SUM(salary) FROM employees;",
			functionName: "SUM",
		},
		{
			name:         "AVG",
			sql:          "SELECT AVG(age) FROM users;",
			functionName: "AVG",
		},
		{
			name:         "MIN",
			sql:          "SELECT MIN(price) FROM products;",
			functionName: "MIN",
		},
		{
			name:         "MAX",
			sql:          "SELECT MAX(price) FROM products;",
			functionName: "MAX",
		},
		{
			name:         "COUNT_DISTINCT",
			sql:          "SELECT COUNT(DISTINCT category) FROM products;",
			functionName: "COUNT",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parser := NewParser(test.sql)
			stmt, err := parser.Parse()

			if err != nil {
				t.Fatalf("Parse error: %v", err)
			}

			if len(stmt.Statements) != 1 {
				t.Fatalf("Expected 1 statement, got %d", len(stmt.Statements))
			}

			selectStmt, ok := stmt.Statements[0].(*SelectStatement)
			if !ok {
				t.Fatalf("Expected SelectStatement, got %T", stmt.Statements[0])
			}

			if len(selectStmt.Columns) != 1 {
				t.Fatalf("Expected 1 column, got %d", len(selectStmt.Columns))
			}

			funcCall, ok := selectStmt.Columns[0].(*FunctionCall)
			if !ok {
				t.Fatalf("Expected FunctionCall, got %T", selectStmt.Columns[0])
			}

			if funcCall.Name != test.functionName {
				t.Errorf("Expected function name %s, got %s", test.functionName, funcCall.Name)
			}
		})
	}
}

func TestParserInvalidSQL(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"Empty", ""},
		{"Garbage", "asdfghjkl"},
		{"UnclosedString", "SELECT * FROM users WHERE name = 'Alice"},
		{"UnclosedParen", "SELECT (id FROM users"},
		{"BadKeyword", "SELEC * FROM users"},
		{"BadInsert", "INSERT INTO users id, name VALUES (1, 'Alice');"},
		{"BadUpdate", "UPDATE users SET WHERE id = 1;"},
		{"BadDelete", "DELETE users WHERE id = 1;"},
		{"BadCreate", "CREATE users (id INT);"},
		{"BadDrop", "DROP users;"},
		{"BadJoin", "SELECT * FROM users JOIN WHERE id = 1;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parser := NewParser(test.sql)
			_, err := parser.Parse()
			if err == nil {
				t.Errorf("Expected parse error for input: %q", test.sql)
			}
		})
	}
}

func TestLexerEdgeCases(t *testing.T) {
	inputs := []string{
		"SELECT -- comment\n* FROM users;",
		"SELECT /* block comment */ * FROM users;",
		"SELECT 'single \\' quote' FROM users;",
		"SELECT \"double \" quote\" FROM users;",
		"SELECT * FROM users WHERE name = 'O\\'Reilly';",
		"SELECT * FROM users WHERE name = \"Alice\";",
		"SELECT * FROM users\tWHERE\nname = 'Bob';",
		"SELECT * FROM users WHERE name = 'A\\\\B';",
	}
	for i, input := range inputs {
		t.Run("LexerEdgeCase_"+string(rune(i)), func(t *testing.T) {
			parser := NewParser(input)
			_, err := parser.Parse()
			if err != nil {
				t.Errorf("Lexer failed on edge case: %q, error: %v", input, err)
			}
		})
	}
}

func TestASTNodeMethods(t *testing.T) {
	// Test String() and Type() for all node types
	var nodes []Node
	nodes = append(nodes, &SQLStatement{})
	nodes = append(nodes, &CreateDatabaseStatement{Name: "testdb"})
	nodes = append(nodes, &DropDatabaseStatement{Name: "testdb"})
	nodes = append(nodes, &CreateTableStatement{Name: "users"})
	nodes = append(nodes, &DropTableStatement{Name: "users"})
	nodes = append(nodes, &CreateIndexStatement{Name: "idx", Table: "users", Columns: []string{"id"}})
	nodes = append(nodes, &DropIndexStatement{Name: "idx"})
	nodes = append(nodes, &SelectStatement{})
	nodes = append(nodes, &InsertStatement{})
	nodes = append(nodes, &UpdateStatement{})
	nodes = append(nodes, &DeleteStatement{})
	// Add more as needed
	for _, node := range nodes {
		if node.String() == "" {
			t.Errorf("%T String() returned empty string", node)
		}
		if node.Type() == "" {
			t.Errorf("%T Type() returned empty string", node)
		}
	}
}

func TestParserRareSQLFeatures(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"CreateView", "CREATE VIEW v AS SELECT * FROM users;"},
		{"DropView", "DROP VIEW v;"},
		{"AlterTableAdd", "ALTER TABLE users ADD COLUMN age INT;"},
		{"AlterTableDrop", "ALTER TABLE users DROP COLUMN age;"},
		{"TruncateTable", "TRUNCATE TABLE users;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parser := NewParser(test.sql)
			_, err := parser.Parse()
			if err != nil {
				t.Errorf("Failed to parse rare SQL feature: %q, error: %v", test.sql, err)
			}
		})
	}
}
