package main

import (
	"testing"

	"github.com/LlamasScripters/PostgresInGo/internal/parser"
)

// TestViewParsing tests just the parsing of view statements
func TestViewParsing(t *testing.T) {
	t.Run("ParseSimpleCreateView", func(t *testing.T) {
		sql := "CREATE VIEW test_view AS SELECT 1"

		p := parser.NewParser(sql)
		stmt, err := p.Parse()

		if err != nil {
			t.Errorf("Failed to parse CREATE VIEW: %v", err)
			return
		}

		if len(stmt.Statements) != 1 {
			t.Errorf("Expected 1 statement, got %d", len(stmt.Statements))
			return
		}

		createView, ok := stmt.Statements[0].(*parser.CreateViewStatement)
		if !ok {
			t.Errorf("Expected CreateViewStatement, got %T", stmt.Statements[0])
			return
		}

		if createView.Name != "test_view" {
			t.Errorf("Expected view name 'test_view', got '%s'", createView.Name)
		}

		t.Logf("Successfully parsed CREATE VIEW: %s", createView.String())
	})

	t.Run("ParseDropView", func(t *testing.T) {
		sql := "DROP VIEW test_view"

		p := parser.NewParser(sql)
		stmt, err := p.Parse()

		if err != nil {
			t.Errorf("Failed to parse DROP VIEW: %v", err)
			return
		}

		if len(stmt.Statements) != 1 {
			t.Errorf("Expected 1 statement, got %d", len(stmt.Statements))
			return
		}

		dropView, ok := stmt.Statements[0].(*parser.DropViewStatement)
		if !ok {
			t.Errorf("Expected DropViewStatement, got %T", stmt.Statements[0])
			return
		}

		if dropView.Name != "test_view" {
			t.Errorf("Expected view name 'test_view', got '%s'", dropView.Name)
		}

		t.Logf("Successfully parsed DROP VIEW: %s", dropView.String())
	})
}
