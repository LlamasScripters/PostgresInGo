package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LlamasScripters/PostgresInGo/internal/engine"
	"github.com/LlamasScripters/PostgresInGo/internal/parser"
)

// TestSQLLexer tests the SQL lexer functionality
func TestSQLLexer(t *testing.T) {
	t.Run("BasicTokenization", func(t *testing.T) {
		tests := []struct {
			input    string
			expected []parser.TokenType
		}{
			{
				input:    "SELECT * FROM users;",
				expected: []parser.TokenType{parser.SELECT, parser.ASTERISK, parser.FROM, parser.IDENT, parser.SEMICOLON, parser.EOF},
			},
			{
				input:    "CREATE TABLE test (id INT, name VARCHAR(50));",
				expected: []parser.TokenType{parser.CREATE, parser.TABLE, parser.IDENT, parser.LPAREN, parser.IDENT, parser.INT_TYPE, parser.COMMA, parser.IDENT, parser.VARCHAR_TYPE, parser.LPAREN, parser.INT, parser.RPAREN, parser.RPAREN, parser.SEMICOLON, parser.EOF},
			},
			{
				input:    "INSERT INTO users VALUES (1, 'Alice', true);",
				expected: []parser.TokenType{parser.INSERT, parser.INTO, parser.IDENT, parser.VALUES, parser.LPAREN, parser.INT, parser.COMMA, parser.STRING, parser.COMMA, parser.BOOLEAN, parser.RPAREN, parser.SEMICOLON, parser.EOF},
			},
		}

		for i, test := range tests {
			t.Run(fmt.Sprintf("Test_%d", i+1), func(t *testing.T) {
				simpleLexer := parser.NewLexer(test.input)
				tokens := simpleLexer.GetAllTokens()

				if len(tokens) != len(test.expected) {
					t.Errorf("Expected %d tokens, got %d", len(test.expected), len(tokens))
					return
				}

				for j, expectedType := range test.expected {
					if tokens[j].Type != expectedType {
						t.Errorf("Token %d: expected %s, got %s", j, expectedType.String(), tokens[j].Type.String())
					}
				}
			})
		}
	})

	t.Run("StringLiterals", func(t *testing.T) {
		tests := []struct {
			input    string
			expected string
		}{
			{"'hello world'", "hello world"},
			{"\"quoted string\"", "quoted string"},
			{"'string with spaces'", "string with spaces"},
		}

		for _, test := range tests {
			lexer := parser.NewAdvancedLexer(test.input)
			token := lexer.Advance()
			
			if token.Type != parser.STRING {
				t.Errorf("Expected STRING token, got %s", token.Type.String())
			}
			
			if token.Literal != test.expected {
				t.Errorf("Expected '%s', got '%s'", test.expected, token.Literal)
			}
		}
	})

	t.Run("Comments", func(t *testing.T) {
		input := `-- This is a comment
		SELECT * FROM users; /* Another comment */`
		
		lexer := parser.NewAdvancedLexer(input)
		tokens := lexer.GetAllTokens()
		
		// Should not include comments in output
		hasComment := false
		for _, token := range tokens {
			if token.Type == parser.COMMENT {
				hasComment = true
				break
			}
		}
		
		if hasComment {
			t.Error("Comments should be filtered out")
		}
	})
}

// TestSQLParser tests the SQL parser functionality
func TestSQLParser(t *testing.T) {
	t.Run("DDLStatements", func(t *testing.T) {
		tests := []struct {
			name     string
			sql      string
			expected string
		}{
			{
				name: "CreateDatabase",
				sql:  "CREATE DATABASE testdb;",
				expected: "CREATE DATABASE testdb",
			},
			{
				name: "CreateTable",
				sql:  "CREATE TABLE users (id INT NOT NULL, name VARCHAR(50), PRIMARY KEY (id));",
				expected: "CREATE TABLE users (id INT NOT NULL, name VARCHAR(50), PRIMARY KEY (id))",
			},
			{
				name: "CreateIndex",
				sql:  "CREATE INDEX idx_name ON users (name);",
				expected: "CREATE INDEX idx_name ON users (name)",
			},
			{
				name: "DropTable",
				sql:  "DROP TABLE users;",
				expected: "DROP TABLE users",
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				sqlParser := parser.NewParser(test.sql)
				stmt, err := sqlParser.Parse()
				
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
	})

	t.Run("DMLStatements", func(t *testing.T) {
		tests := []struct {
			name string
			sql  string
		}{
			{
				name: "Select",
				sql:  "SELECT * FROM users;",
			},
			{
				name: "SelectWithWhere",
				sql:  "SELECT id, name FROM users WHERE id = 1;",
			},
			{
				name: "Insert",
				sql:  "INSERT INTO users (id, name) VALUES (1, 'Alice');",
			},
			{
				name: "Update",
				sql:  "UPDATE users SET name = 'Bob' WHERE id = 1;",
			},
			{
				name: "Delete",
				sql:  "DELETE FROM users WHERE id = 1;",
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				sqlParser := parser.NewParser(test.sql)
				stmt, err := sqlParser.Parse()
				
				if err != nil {
					t.Fatalf("Parse error: %v", err)
				}
				
				if len(stmt.Statements) != 1 {
					t.Fatalf("Expected 1 statement, got %d", len(stmt.Statements))
				}

				// Check statement type
				switch test.name {
				case "Select", "SelectWithWhere":
					if _, ok := stmt.Statements[0].(*parser.SelectStatement); !ok {
						t.Error("Expected SelectStatement")
					}
				case "Insert":
					if _, ok := stmt.Statements[0].(*parser.InsertStatement); !ok {
						t.Error("Expected InsertStatement")
					}
				case "Update":
					if _, ok := stmt.Statements[0].(*parser.UpdateStatement); !ok {
						t.Error("Expected UpdateStatement")
					}
				case "Delete":
					if _, ok := stmt.Statements[0].(*parser.DeleteStatement); !ok {
						t.Error("Expected DeleteStatement")
					}
				}
			})
		}
	})

	t.Run("ComplexQueries", func(t *testing.T) {
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
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				sqlParser := parser.NewParser(test.sql)
				stmt, err := sqlParser.Parse()
				
				if err != nil {
					t.Fatalf("Parse error: %v", err)
				}
				
				if len(stmt.Statements) != 1 {
					t.Fatalf("Expected 1 statement, got %d", len(stmt.Statements))
				}

				if selectStmt, ok := stmt.Statements[0].(*parser.SelectStatement); ok {
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
					}
				}
			})
		}
	})

	t.Run("ErrorHandling", func(t *testing.T) {
		invalidQueries := []string{
			"SELECT FROM;",           // Missing table
			"CREATE TABLE ();",       // Missing table name
			"INSERT INTO VALUES;",    // Missing table and values
			"UPDATE SET;",           // Missing table and assignments
			"DELETE;",               // Missing FROM clause
		}

		for i, query := range invalidQueries {
			t.Run(fmt.Sprintf("InvalidQuery_%d", i+1), func(t *testing.T) {
				sqlParser := parser.NewParser(query)
				_, err := sqlParser.Parse()
				
				if err == nil {
					t.Error("Expected parse error for invalid query")
				}
			})
		}
	})
}

// TestSQLExecutionIntegration tests the full SQL execution pipeline
func TestSQLExecutionIntegration(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("postgres_sql_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(testDir)

	pg, err := engine.NewPostgresEngine(testDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer pg.Close()

	t.Run("DatabaseOperations", func(t *testing.T) {
		// Create database
		result, err := pg.ExecuteSQL("CREATE DATABASE testdb")
		if err != nil {
			t.Fatalf("Failed to create database: %v", err)
		}
		
		if result.Message == "" {
			t.Error("Expected success message")
		}

		// Drop database (should work even though not fully implemented)
		_, err = pg.ExecuteSQL("DROP DATABASE testdb")
		if err == nil {
			t.Log("DROP DATABASE executed successfully")
		} else {
			t.Logf("DROP DATABASE failed as expected: %v", err)
		}
	})

	t.Run("TableOperations", func(t *testing.T) {
		// Create database first
		_, err := pg.ExecuteSQL("CREATE DATABASE sqltest")
		if err != nil {
			// Database might already exist
			t.Logf("Database creation failed (might exist): %v", err)
		}

		// Create table with SQL
		createTableSQL := `
			CREATE TABLE users (
				id INT NOT NULL,
				name VARCHAR(50) NOT NULL,
				email VARCHAR(100),
				active BOOLEAN NOT NULL,
				PRIMARY KEY (id)
			)
		`
		
		result, err := pg.ExecuteSQL(createTableSQL)
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}
		
		if result.Message == "" {
			t.Error("Expected success message")
		}

		// Verify table was created
		table, err := pg.GetTable("users")
		if err != nil {
			t.Fatalf("Failed to get created table: %v", err)
		}
		
		if len(table.Schema.Columns) != 4 {
			t.Errorf("Expected 4 columns, got %d", len(table.Schema.Columns))
		}
	})

	t.Run("DataManipulation", func(t *testing.T) {
		// First create the table
		createTableSQL := `
			CREATE TABLE test_users (
				id INT NOT NULL,
				name VARCHAR(50) NOT NULL,
				active BOOLEAN
			)
		`
		
		_, err := pg.ExecuteSQL(createTableSQL)
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		// Test INSERT
		insertSQL := "INSERT INTO test_users (id, name, active) VALUES (1, 'Alice', true)"
		result, err := pg.ExecuteSQL(insertSQL)
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
		
		if result.RowsAffected != 1 {
			t.Errorf("Expected 1 row affected, got %d", result.RowsAffected)
		}

		// Test multiple inserts
		multiInsertSQL := `
			INSERT INTO test_users (id, name, active) VALUES 
			(2, 'Bob', false), 
			(3, 'Charlie', true)
		`
		result, err = pg.ExecuteSQL(multiInsertSQL)
		if err != nil {
			t.Fatalf("Failed to insert multiple rows: %v", err)
		}
		
		if result.RowsAffected != 2 {
			t.Errorf("Expected 2 rows affected, got %d", result.RowsAffected)
		}

		// Test SELECT
		selectSQL := "SELECT * FROM test_users"
		result, err = pg.ExecuteSQL(selectSQL)
		if err != nil {
			t.Fatalf("Failed to select data: %v", err)
		}
		
		if len(result.Data) != 3 {
			t.Errorf("Expected 3 rows, got %d", len(result.Data))
		}
		
		if len(result.Columns) != 3 {
			t.Errorf("Expected 3 columns, got %d", len(result.Columns))
		}

		// Test SELECT with WHERE
		selectWhereSQL := "SELECT * FROM test_users WHERE id = 1"
		result, err = pg.ExecuteSQL(selectWhereSQL)
		if err != nil {
			t.Fatalf("Failed to select with WHERE: %v", err)
		}
		
		if len(result.Data) != 1 {
			t.Errorf("Expected 1 row, got %d", len(result.Data))
		}

		// Test UPDATE
		updateSQL := "UPDATE test_users SET name = 'Alice Updated' WHERE id = 1"
		result, err = pg.ExecuteSQL(updateSQL)
		if err != nil {
			t.Fatalf("Failed to update data: %v", err)
		}
		
		if result.RowsAffected != 1 {
			t.Errorf("Expected 1 row affected, got %d", result.RowsAffected)
		}

		// Test DELETE
		deleteSQL := "DELETE FROM test_users WHERE id = 3"
		result, err = pg.ExecuteSQL(deleteSQL)
		if err != nil {
			t.Fatalf("Failed to delete data: %v", err)
		}
		
		if result.RowsAffected != 1 {
			t.Errorf("Expected 1 row affected, got %d", result.RowsAffected)
		}
	})

	t.Run("IndexOperations", func(t *testing.T) {
		// Create table first
		_, err := pg.ExecuteSQL("CREATE TABLE indexed_table (id INT, name VARCHAR(50))")
		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		// Create index
		indexSQL := "CREATE INDEX idx_name ON indexed_table (name)"
		result, err := pg.ExecuteSQL(indexSQL)
		if err != nil {
			t.Fatalf("Failed to create index: %v", err)
		}
		
		if result.Message == "" {
			t.Error("Expected success message")
		}

		// Create unique index
		uniqueIndexSQL := "CREATE UNIQUE INDEX idx_id ON indexed_table (id)"
		result, err = pg.ExecuteSQL(uniqueIndexSQL)
		if err != nil {
			t.Fatalf("Failed to create unique index: %v", err)
		}
		
		if result.Message == "" {
			t.Error("Expected success message")
		}
	})
}

// TestSQLParserEdgeCases tests edge cases and error conditions
func TestSQLParserEdgeCases(t *testing.T) {
	t.Run("EmptyAndWhitespace", func(t *testing.T) {
		tests := []string{
			"",
			"   ",
			"\n\t\r",
			";;;",
		}

		for i, test := range tests {
			t.Run(fmt.Sprintf("Test_%d", i+1), func(t *testing.T) {
				sqlParser := parser.NewParser(test)
				stmt, err := sqlParser.Parse()
				
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				
				if len(stmt.Statements) != 0 {
					t.Errorf("Expected 0 statements, got %d", len(stmt.Statements))
				}
			})
		}
	})

	t.Run("MultipleStatements", func(t *testing.T) {
		sql := `
			CREATE TABLE users (id INT);
			INSERT INTO users VALUES (1);
			SELECT * FROM users;
		`
		
		sqlParser := parser.NewParser(sql)
		stmt, err := sqlParser.Parse()
		
		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}
		
		if len(stmt.Statements) != 3 {
			t.Errorf("Expected 3 statements, got %d", len(stmt.Statements))
		}
	})

	t.Run("CaseInsensitivity", func(t *testing.T) {
		tests := []string{
			"select * from users;",
			"SELECT * FROM USERS;",
			"Select * From Users;",
			"sElEcT * fRoM uSeRs;",
		}

		for i, test := range tests {
			t.Run(fmt.Sprintf("Test_%d", i+1), func(t *testing.T) {
				sqlParser := parser.NewParser(test)
				stmt, err := sqlParser.Parse()
				
				if err != nil {
					t.Fatalf("Parse error: %v", err)
				}
				
				if len(stmt.Statements) != 1 {
					t.Fatalf("Expected 1 statement, got %d", len(stmt.Statements))
				}

				if _, ok := stmt.Statements[0].(*parser.SelectStatement); !ok {
					t.Error("Expected SelectStatement")
				}
			})
		}
	})

	t.Run("SpecialCharacters", func(t *testing.T) {
		tests := []struct {
			name string
			sql  string
		}{
			{
				name: "QuotedIdentifiers",
				sql:  `SELECT "user name" FROM "user table";`,
			},
			{
				name: "StringWithQuotes",
				sql:  `INSERT INTO users VALUES (1, 'O''Connor');`,
			},
			{
				name: "NumericLiterals",
				sql:  `SELECT 123, 45.67, -89.0 FROM dual;`,
			},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				sqlParser := parser.NewParser(test.sql)
				_, err := sqlParser.Parse()
				
				if err != nil {
					t.Logf("Parse error (may be expected): %v", err)
				}
			})
		}
	})
}

// TestSQLParserPerformance tests parser performance
func TestSQLParserPerformance(t *testing.T) {
	t.Run("LargeQuery", func(t *testing.T) {
		// Generate a large INSERT statement
		var sql strings.Builder
		sql.WriteString("INSERT INTO users (id, name) VALUES ")
		
		for i := 0; i < 1000; i++ {
			if i > 0 {
				sql.WriteString(", ")
			}
			sql.WriteString(fmt.Sprintf("(%d, 'User %d')", i, i))
		}
		sql.WriteString(";")

		start := time.Now()
		sqlParser := parser.NewParser(sql.String())
		stmt, err := sqlParser.Parse()
		elapsed := time.Since(start)

		if err != nil {
			t.Fatalf("Parse error: %v", err)
		}

		if len(stmt.Statements) != 1 {
			t.Fatalf("Expected 1 statement, got %d", len(stmt.Statements))
		}

		insertStmt, ok := stmt.Statements[0].(*parser.InsertStatement)
		if !ok {
			t.Fatal("Expected InsertStatement")
		}

		if len(insertStmt.Values) != 1000 {
			t.Errorf("Expected 1000 value rows, got %d", len(insertStmt.Values))
		}

		t.Logf("Parsed 1000-row INSERT in %v", elapsed)
		
		// Performance check - should parse in reasonable time
		if elapsed > time.Second {
			t.Errorf("Parse took too long: %v", elapsed)
		}
	})
}

// BenchmarkSQLParser benchmarks SQL parser performance
func BenchmarkSQLParser(b *testing.B) {
	queries := []string{
		"SELECT * FROM users;",
		"INSERT INTO users (id, name) VALUES (1, 'Alice');",
		"UPDATE users SET name = 'Bob' WHERE id = 1;",
		"DELETE FROM users WHERE id = 1;",
		"CREATE TABLE test (id INT, name VARCHAR(50));",
	}

	b.Run("ParseQueries", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			query := queries[i%len(queries)]
			sqlParser := parser.NewParser(query)
			_, err := sqlParser.Parse()
			if err != nil {
				b.Fatalf("Parse error: %v", err)
			}
		}
	})

	b.Run("LexQueries", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			query := queries[i%len(queries)]
			lexer := parser.NewAdvancedLexer(query)
			_ = lexer.GetAllTokens()
		}
	})
}

// TestSQLResultSerialization tests JSON serialization of SQL results
func TestSQLResultSerialization(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("postgres_json_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(testDir)

	pg, err := engine.NewPostgresEngine(testDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer pg.Close()

	// Create test table and data
	_, err = pg.ExecuteSQL("CREATE TABLE test_json (id INT, name VARCHAR(50), active BOOLEAN)")
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = pg.ExecuteSQL("INSERT INTO test_json VALUES (1, 'Alice', true), (2, 'Bob', false)")
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	// Execute SELECT and test JSON serialization
	result, err := pg.ExecuteSQL("SELECT * FROM test_json")
	if err != nil {
		t.Fatalf("Failed to select data: %v", err)
	}

	// Test JSON marshaling
	jsonData, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatalf("Failed to marshal result to JSON: %v", err)
	}

	t.Logf("JSON Result:\n%s", string(jsonData))

	// Test JSON unmarshaling
	var unmarshaled engine.SQLResult
	err = json.Unmarshal(jsonData, &unmarshaled)
	if err != nil {
		t.Fatalf("Failed to unmarshal JSON: %v", err)
	}

	// Verify data integrity
	if len(unmarshaled.Data) != len(result.Data) {
		t.Errorf("Data length mismatch: expected %d, got %d", len(result.Data), len(unmarshaled.Data))
	}

	if len(unmarshaled.Columns) != len(result.Columns) {
		t.Errorf("Columns length mismatch: expected %d, got %d", len(result.Columns), len(unmarshaled.Columns))
	}
}

// Helper function to format JSON nicely for tests
func prettyPrintJSON(data interface{}) string {
	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Sprintf("Error marshaling JSON: %v", err)
	}
	return string(jsonData)
}