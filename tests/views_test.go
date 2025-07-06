package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/esgi-git/postgres-engine/internal/engine"
	"github.com/esgi-git/postgres-engine/internal/types"
)

// TestViewOperations tests all view-related operations following TDD approach
func TestViewOperations(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("postgres_views_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(testDir)

	pg, err := engine.NewPostgresEngine(testDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer pg.Close()

	// Setup database and base tables
	err = pg.CreateDatabase("testdb")
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	err = pg.UseDatabase("testdb")
	if err != nil {
		t.Fatalf("Failed to use database: %v", err)
	}

	// Create base tables for testing views
	setupBaseTables(t, pg)

	t.Run("CreateSimpleView", func(t *testing.T) {
		// Test creating a simple view that selects from one table
		viewSQL := "CREATE VIEW active_users AS SELECT id, name, email FROM users WHERE active = true"
		result, err := pg.ExecuteSQL(viewSQL)
		if err != nil {
			t.Fatalf("Failed to create simple view: %v", err)
		}

		if result.Message == "" {
			t.Error("Expected success message for view creation")
		}

		// Verify view exists in metadata
		view, err := pg.GetView("active_users")
		if err != nil {
			t.Errorf("Failed to get created view: %v", err)
		}
		if view == nil {
			t.Error("GetView should return view")
		}
		if view.Name != "active_users" {
			t.Errorf("Expected view name 'active_users', got %s", view.Name)
		}
	})

	t.Run("CreateComplexView", func(t *testing.T) {
		// Test creating a view with JOINs and aggregate functions
		viewSQL := `CREATE VIEW user_order_summary AS 
					SELECT u.id, u.name, COUNT(o.id) as order_count, SUM(o.total) as total_spent
					FROM users u 
					LEFT JOIN orders o ON u.id = o.user_id 
					WHERE u.active = true 
					GROUP BY u.id, u.name`

		result, err := pg.ExecuteSQL(viewSQL)
		if err != nil {
			t.Fatalf("Failed to create complex view: %v", err)
		}

		if result.Message == "" {
			t.Error("Expected success message for complex view creation")
		}

		// Verify view exists
		view, err := pg.GetView("user_order_summary")
		if err != nil {
			t.Errorf("Failed to get complex view: %v", err)
		}
		if view == nil {
			t.Error("GetView should return complex view")
		}
	})

	t.Run("SelectFromView", func(t *testing.T) {
		// Test selecting data from a view
		selectSQL := "SELECT * FROM active_users"
		result, err := pg.ExecuteSQL(selectSQL)
		if err != nil {
			t.Fatalf("Failed to select from view: %v", err)
		}

		// Should return only active users (2 out of 3 test users)
		if len(result.Data) != 2 {
			t.Errorf("Expected 2 active users, got %d", len(result.Data))
		}

		// Verify columns are correct
		if len(result.Columns) != 3 {
			t.Errorf("Expected 3 columns (id, name, email), got %d", len(result.Columns))
		}

		expectedColumns := []string{"id", "name", "email"}
		for i, col := range expectedColumns {
			if i >= len(result.Columns) || result.Columns[i] != col {
				t.Errorf("Expected column %s at position %d, got %v", col, i, result.Columns)
			}
		}
	})

	t.Run("SelectFromComplexView", func(t *testing.T) {
		// Test selecting from complex view with aggregations
		selectSQL := "SELECT * FROM user_order_summary ORDER BY total_spent DESC"
		result, err := pg.ExecuteSQL(selectSQL)
		if err != nil {
			t.Fatalf("Failed to select from complex view: %v", err)
		}

		// Should return summary for active users only
		if len(result.Data) != 2 {
			t.Errorf("Expected 2 user summaries, got %d", len(result.Data))
		}

		// Verify columns include aggregated data
		expectedColumns := []string{"id", "name", "order_count", "total_spent"}
		if len(result.Columns) != len(expectedColumns) {
			t.Errorf("Expected %d columns, got %d", len(expectedColumns), len(result.Columns))
		}
	})

	t.Run("ViewWithFilter", func(t *testing.T) {
		// Test applying additional filters when selecting from view
		selectSQL := "SELECT * FROM active_users WHERE name LIKE '%Alice%'"
		result, err := pg.ExecuteSQL(selectSQL)
		if err != nil {
			t.Fatalf("Failed to select from view with filter: %v", err)
		}

		// Should return only Alice (1 user)
		if len(result.Data) != 1 {
			t.Errorf("Expected 1 user matching filter, got %d", len(result.Data))
		}
	})

	t.Run("DropView", func(t *testing.T) {
		// Test dropping a view
		dropSQL := "DROP VIEW active_users"
		result, err := pg.ExecuteSQL(dropSQL)
		if err != nil {
			t.Fatalf("Failed to drop view: %v", err)
		}

		if result.Message == "" {
			t.Error("Expected success message for view drop")
		}

		// Verify view no longer exists
		_, err = pg.GetView("active_users")
		if err == nil {
			t.Error("Should fail when getting dropped view")
		}

		// Verify selecting from dropped view fails
		_, err = pg.ExecuteSQL("SELECT * FROM active_users")
		if err == nil {
			t.Error("Should fail when selecting from dropped view")
		}
	})

	t.Run("CreateViewErrors", func(t *testing.T) {
		// Test various error conditions

		// Duplicate view name
		_, err := pg.ExecuteSQL("CREATE VIEW user_order_summary AS SELECT * FROM users")
		if err == nil {
			t.Error("Should fail when creating view with duplicate name")
		}

		// View referencing non-existent table
		_, err = pg.ExecuteSQL("CREATE VIEW bad_view AS SELECT * FROM nonexistent_table")
		if err == nil {
			t.Error("Should fail when creating view referencing non-existent table")
		}

		// Invalid SQL in view definition
		_, err = pg.ExecuteSQL("CREATE VIEW invalid_view AS INVALID SQL SYNTAX")
		if err == nil {
			t.Error("Should fail when creating view with invalid SQL")
		}
	})

	t.Run("ViewDependencies", func(t *testing.T) {
		// Test that views are updated when underlying tables change

		// Insert new user
		err := pg.Insert("users", map[string]any{
			"id":     4,
			"name":   "David",
			"email":  "david@example.com",
			"active": true,
		})
		if err != nil {
			t.Fatalf("Failed to insert new user: %v", err)
		}

		// Recreate the view for testing
		_, err = pg.ExecuteSQL("CREATE VIEW active_users_v2 AS SELECT id, name, email FROM users WHERE active = true")
		if err != nil {
			t.Fatalf("Failed to create view for dependency test: %v", err)
		}

		// Select from view should now include the new user
		result, err := pg.ExecuteSQL("SELECT * FROM active_users_v2")
		if err != nil {
			t.Fatalf("Failed to select from view after table update: %v", err)
		}

		// Should now return 3 active users (including David)
		if len(result.Data) != 3 {
			t.Errorf("Expected 3 active users after insert, got %d", len(result.Data))
		}
	})

	t.Run("ViewMetadata", func(t *testing.T) {
		// Test view metadata and information

		// Get all views
		views, err := pg.GetAllViews()
		if err != nil {
			t.Errorf("Failed to get all views: %v", err)
		}

		// Should have at least 2 views (user_order_summary and active_users_v2)
		if len(views) < 2 {
			t.Errorf("Expected at least 2 views, got %d", len(views))
		}

		// Verify view definition is stored correctly
		view, err := pg.GetView("user_order_summary")
		if err != nil {
			t.Errorf("Failed to get view for metadata check: %v", err)
		}

		if view.Definition == "" {
			t.Error("View definition should not be empty")
		}

		if len(view.Columns) == 0 {
			t.Error("View should have column metadata")
		}
	})
}

// setupBaseTables creates the base tables needed for view testing
func setupBaseTables(t *testing.T, pg *engine.PostgresEngine) {
	// Create users table
	userSchema := types.Schema{
		Columns: []types.Column{
			{Name: "id", Type: types.IntType, Nullable: false, IsPrimaryKey: true},
			{Name: "name", Type: types.VarcharType, Size: 100, Nullable: false},
			{Name: "email", Type: types.VarcharType, Size: 100, Nullable: false},
			{Name: "active", Type: types.BoolType, Nullable: false},
		},
	}
	err := pg.CreateTable("users", userSchema)
	if err != nil {
		t.Fatalf("Failed to create users table: %v", err)
	}

	// Create orders table
	orderSchema := types.Schema{
		Columns: []types.Column{
			{Name: "id", Type: types.IntType, Nullable: false, IsPrimaryKey: true},
			{Name: "user_id", Type: types.IntType, Nullable: false},
			{Name: "total", Type: types.FloatType, Nullable: false},
			{Name: "status", Type: types.VarcharType, Size: 50, Nullable: false},
		},
	}
	err = pg.CreateTable("orders", orderSchema)
	if err != nil {
		t.Fatalf("Failed to create orders table: %v", err)
	}

	// Insert test data
	users := []map[string]any{
		{"id": 1, "name": "Alice", "email": "alice@example.com", "active": true},
		{"id": 2, "name": "Bob", "email": "bob@example.com", "active": true},
		{"id": 3, "name": "Charlie", "email": "charlie@example.com", "active": false},
	}

	for _, user := range users {
		err := pg.Insert("users", user)
		if err != nil {
			t.Fatalf("Failed to insert test user: %v", err)
		}
	}

	orders := []map[string]any{
		{"id": 1, "user_id": 1, "total": 150.00, "status": "completed"},
		{"id": 2, "user_id": 1, "total": 75.50, "status": "pending"},
		{"id": 3, "user_id": 2, "total": 200.00, "status": "completed"},
	}

	for _, order := range orders {
		err := pg.Insert("orders", order)
		if err != nil {
			t.Fatalf("Failed to insert test order: %v", err)
		}
	}
}
