package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/LlamasScripters/PostgresInGo/internal/engine"
	"github.com/LlamasScripters/PostgresInGo/internal/types"
)

// TestPrimaryKeyConstraints tests primary key constraints
func TestPrimaryKeyConstraints(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("postgres_pk_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(testDir)

	pg, err := engine.NewPostgresEngine(testDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer pg.Close()

	// Setup database
	err = pg.CreateDatabase("testdb")
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	err = pg.UseDatabase("testdb")
	if err != nil {
		t.Fatalf("Failed to use database: %v", err)
	}

	t.Run("CreateTableWithPrimaryKey", func(t *testing.T) {
		schema := types.Schema{
			Columns: []types.Column{
				{Name: "id", Type: types.IntType, Nullable: false, IsPrimaryKey: true},
				{Name: "name", Type: types.VarcharType, Size: 50, Nullable: false},
				{Name: "email", Type: types.VarcharType, Size: 100, Nullable: true},
			},
		}
		err := pg.CreateTable("users", schema)
		if err != nil {
			t.Errorf("Failed to create table with primary key: %v", err)
		}

		// Add primary key constraint
		err = pg.AddPrimaryKey("users", []string{"id"})
		if err != nil {
			t.Errorf("Failed to add primary key constraint: %v", err)
		}
	})

	t.Run("InsertValidPrimaryKey", func(t *testing.T) {
		err := pg.Insert("users", map[string]any{
			"id":    1,
			"name":  "Alice",
			"email": "alice@example.com",
		})
		if err != nil {
			t.Errorf("Failed to insert valid primary key: %v", err)
		}
	})

	t.Run("InsertDuplicatePrimaryKey", func(t *testing.T) {
		err := pg.Insert("users", map[string]any{
			"id":    1,
			"name":  "Bob",
			"email": "bob@example.com",
		})
		if err == nil {
			t.Error("Should fail when inserting duplicate primary key")
		}
	})

	t.Run("InsertNullPrimaryKey", func(t *testing.T) {
		err := pg.Insert("users", map[string]any{
			"id":    nil,
			"name":  "Charlie",
			"email": "charlie@example.com",
		})
		if err == nil {
			t.Error("Should fail when inserting null primary key")
		}
	})

	t.Run("CompositePrimaryKey", func(t *testing.T) {
		schema := types.Schema{
			Columns: []types.Column{
				{Name: "order_id", Type: types.IntType, Nullable: false},
				{Name: "product_id", Type: types.IntType, Nullable: false},
				{Name: "quantity", Type: types.IntType, Nullable: false},
			},
		}
		err := pg.CreateTable("order_items", schema)
		if err != nil {
			t.Errorf("Failed to create table: %v", err)
		}

		// Add composite primary key
		err = pg.AddPrimaryKey("order_items", []string{"order_id", "product_id"})
		if err != nil {
			t.Errorf("Failed to add composite primary key: %v", err)
		}

		// Insert valid data
		err = pg.Insert("order_items", map[string]any{
			"order_id":   1,
			"product_id": 101,
			"quantity":   5,
		})
		if err != nil {
			t.Errorf("Failed to insert valid composite key: %v", err)
		}

		// Try to insert duplicate composite key
		err = pg.Insert("order_items", map[string]any{
			"order_id":   1,
			"product_id": 101,
			"quantity":   3,
		})
		if err == nil {
			t.Error("Should fail when inserting duplicate composite primary key")
		}
	})
}

// TestForeignKeyConstraints tests foreign key constraints
func TestForeignKeyConstraints(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("postgres_fk_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(testDir)

	pg, err := engine.NewPostgresEngine(testDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer pg.Close()

	// Setup database
	err = pg.CreateDatabase("testdb")
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	err = pg.UseDatabase("testdb")
	if err != nil {
		t.Fatalf("Failed to use database: %v", err)
	}

	t.Run("CreateTablesWithForeignKey", func(t *testing.T) {
		// Create parent table (users)
		userSchema := types.Schema{
			Columns: []types.Column{
				{Name: "id", Type: types.IntType, Nullable: false, IsPrimaryKey: true},
				{Name: "name", Type: types.VarcharType, Size: 50, Nullable: false},
			},
		}
		err := pg.CreateTable("users", userSchema)
		if err != nil {
			t.Errorf("Failed to create users table: %v", err)
		}

		err = pg.AddPrimaryKey("users", []string{"id"})
		if err != nil {
			t.Errorf("Failed to add primary key to users: %v", err)
		}

		// Create child table (orders)
		orderSchema := types.Schema{
			Columns: []types.Column{
				{Name: "id", Type: types.IntType, Nullable: false, IsPrimaryKey: true},
				{Name: "user_id", Type: types.IntType, Nullable: false},
				{Name: "amount", Type: types.FloatType, Nullable: false},
			},
		}
		err = pg.CreateTable("orders", orderSchema)
		if err != nil {
			t.Errorf("Failed to create orders table: %v", err)
		}

		err = pg.AddPrimaryKey("orders", []string{"id"})
		if err != nil {
			t.Errorf("Failed to add primary key to orders: %v", err)
		}

		// Add foreign key constraint
		err = pg.AddForeignKey("orders", []string{"user_id"}, "users", []string{"id"}, "RESTRICT", "RESTRICT")
		if err != nil {
			t.Errorf("Failed to add foreign key constraint: %v", err)
		}
	})

	t.Run("InsertValidForeignKey", func(t *testing.T) {
		// Insert parent record
		err := pg.Insert("users", map[string]any{
			"id":   1,
			"name": "Alice",
		})
		if err != nil {
			t.Errorf("Failed to insert user: %v", err)
		}

		// Insert child record with valid foreign key
		err = pg.Insert("orders", map[string]any{
			"id":      1,
			"user_id": 1,
			"amount":  99.99,
		})
		if err != nil {
			t.Errorf("Failed to insert order with valid foreign key: %v", err)
		}
	})

	t.Run("InsertInvalidForeignKey", func(t *testing.T) {
		// Try to insert child record with invalid foreign key
		err := pg.Insert("orders", map[string]any{
			"id":      2,
			"user_id": 999, // Non-existent user
			"amount":  50.00,
		})
		if err == nil {
			t.Error("Should fail when inserting invalid foreign key")
		}
	})

	t.Run("InsertNullForeignKey", func(t *testing.T) {
		// Create table with nullable foreign key
		profileSchema := types.Schema{
			Columns: []types.Column{
				{Name: "id", Type: types.IntType, Nullable: false, IsPrimaryKey: true},
				{Name: "user_id", Type: types.IntType, Nullable: true},
				{Name: "bio", Type: types.VarcharType, Size: 255, Nullable: true},
			},
		}
		err := pg.CreateTable("profiles", profileSchema)
		if err != nil {
			t.Errorf("Failed to create profiles table: %v", err)
		}

		err = pg.AddPrimaryKey("profiles", []string{"id"})
		if err != nil {
			t.Errorf("Failed to add primary key to profiles: %v", err)
		}

		err = pg.AddForeignKey("profiles", []string{"user_id"}, "users", []string{"id"}, "RESTRICT", "RESTRICT")
		if err != nil {
			t.Errorf("Failed to add foreign key to profiles: %v", err)
		}

		// Insert record with null foreign key (should be allowed)
		err = pg.Insert("profiles", map[string]any{
			"id":      1,
			"user_id": nil,
			"bio":     "Anonymous user",
		})
		if err != nil {
			t.Errorf("Failed to insert profile with null foreign key: %v", err)
		}
	})
}

// TestUniqueConstraints tests unique constraints
func TestUniqueConstraints(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("postgres_unique_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(testDir)

	pg, err := engine.NewPostgresEngine(testDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer pg.Close()

	// Setup database
	err = pg.CreateDatabase("testdb")
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	err = pg.UseDatabase("testdb")
	if err != nil {
		t.Fatalf("Failed to use database: %v", err)
	}

	t.Run("UniqueColumnConstraint", func(t *testing.T) {
		schema := types.Schema{
			Columns: []types.Column{
				{Name: "id", Type: types.IntType, Nullable: false, IsPrimaryKey: true},
				{Name: "email", Type: types.VarcharType, Size: 100, Nullable: false, IsUnique: true},
				{Name: "name", Type: types.VarcharType, Size: 50, Nullable: false},
			},
		}
		err := pg.CreateTable("users", schema)
		if err != nil {
			t.Errorf("Failed to create table with unique column: %v", err)
		}

		err = pg.AddPrimaryKey("users", []string{"id"})
		if err != nil {
			t.Errorf("Failed to add primary key: %v", err)
		}

		// Insert first record
		err = pg.Insert("users", map[string]any{
			"id":    1,
			"email": "alice@example.com",
			"name":  "Alice",
		})
		if err != nil {
			t.Errorf("Failed to insert first record: %v", err)
		}

		// Try to insert duplicate email
		err = pg.Insert("users", map[string]any{
			"id":    2,
			"email": "alice@example.com",
			"name":  "Bob",
		})
		if err == nil {
			t.Error("Should fail when inserting duplicate unique value")
		}
	})

	t.Run("CompositeUniqueConstraint", func(t *testing.T) {
		schema := types.Schema{
			Columns: []types.Column{
				{Name: "id", Type: types.IntType, Nullable: false, IsPrimaryKey: true},
				{Name: "first_name", Type: types.VarcharType, Size: 50, Nullable: false},
				{Name: "last_name", Type: types.VarcharType, Size: 50, Nullable: false},
				{Name: "birth_date", Type: types.DateType, Nullable: false},
			},
		}
		err := pg.CreateTable("people", schema)
		if err != nil {
			t.Errorf("Failed to create people table: %v", err)
		}

		err = pg.AddPrimaryKey("people", []string{"id"})
		if err != nil {
			t.Errorf("Failed to add primary key: %v", err)
		}

		// Add composite unique constraint
		err = pg.AddUniqueConstraint("people", []string{"first_name", "last_name", "birth_date"})
		if err != nil {
			t.Errorf("Failed to add composite unique constraint: %v", err)
		}

		// Insert first record
		err = pg.Insert("people", map[string]any{
			"id":         1,
			"first_name": "John",
			"last_name":  "Doe",
			"birth_date": "1990-01-01",
		})
		if err != nil {
			t.Errorf("Failed to insert first person: %v", err)
		}

		// Insert record with same name but different birth date (should succeed)
		err = pg.Insert("people", map[string]any{
			"id":         2,
			"first_name": "John",
			"last_name":  "Doe",
			"birth_date": "1991-01-01",
		})
		if err != nil {
			t.Errorf("Failed to insert person with different birth date: %v", err)
		}

		// Try to insert duplicate composite unique values
		err = pg.Insert("people", map[string]any{
			"id":         3,
			"first_name": "John",
			"last_name":  "Doe",
			"birth_date": "1990-01-01",
		})
		if err == nil {
			t.Error("Should fail when inserting duplicate composite unique values")
		}
	})
}

// TestJoinOperations tests all types of JOIN operations
func TestJoinOperations(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("postgres_join_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(testDir)

	pg, err := engine.NewPostgresEngine(testDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer pg.Close()

	// Setup database and tables
	err = pg.CreateDatabase("testdb")
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	err = pg.UseDatabase("testdb")
	if err != nil {
		t.Fatalf("Failed to use database: %v", err)
	}

	// Setup test data
	t.Run("SetupTestData", func(t *testing.T) {
		// Create users table
		userSchema := types.Schema{
			Columns: []types.Column{
				{Name: "id", Type: types.IntType, Nullable: false, IsPrimaryKey: true},
				{Name: "name", Type: types.VarcharType, Size: 50, Nullable: false},
				{Name: "department_id", Type: types.IntType, Nullable: true},
			},
		}
		err := pg.CreateTable("users", userSchema)
		if err != nil {
			t.Errorf("Failed to create users table: %v", err)
		}

		// Create departments table
		deptSchema := types.Schema{
			Columns: []types.Column{
				{Name: "id", Type: types.IntType, Nullable: false, IsPrimaryKey: true},
				{Name: "name", Type: types.VarcharType, Size: 50, Nullable: false},
			},
		}
		err = pg.CreateTable("departments", deptSchema)
		if err != nil {
			t.Errorf("Failed to create departments table: %v", err)
		}

		// Insert test data
		departments := []map[string]any{
			{"id": 1, "name": "Engineering"},
			{"id": 2, "name": "Marketing"},
			{"id": 3, "name": "HR"},
		}
		for _, dept := range departments {
			err := pg.Insert("departments", dept)
			if err != nil {
				t.Errorf("Failed to insert department: %v", err)
			}
		}

		users := []map[string]any{
			{"id": 1, "name": "Alice", "department_id": 1},
			{"id": 2, "name": "Bob", "department_id": 1},
			{"id": 3, "name": "Charlie", "department_id": 2},
			{"id": 4, "name": "Diana", "department_id": nil}, // No department
		}
		for _, user := range users {
			err := pg.Insert("users", user)
			if err != nil {
				t.Errorf("Failed to insert user: %v", err)
			}
		}
	})

	t.Run("InnerJoin", func(t *testing.T) {
		results, err := pg.InnerJoin("users", "departments", "department_id", "id")
		if err != nil {
			t.Errorf("Failed to perform inner join: %v", err)
		}

		// Should return 3 results (Alice, Bob, Charlie)
		// Diana should be excluded because she has no department
		if len(results) < 3 {
			t.Errorf("Expected at least 3 results from inner join, got %d", len(results))
		}

		t.Logf("Inner join returned %d results", len(results))
		for i, result := range results {
			data := pg.DeserializeDataForTesting(result.Data)
			t.Logf("Result %d: %+v", i+1, data)
		}
	})

	t.Run("LeftJoin", func(t *testing.T) {
		results, err := pg.LeftJoin("users", "departments", "department_id", "id")
		if err != nil {
			t.Errorf("Failed to perform left join: %v", err)
		}

		// Should return 4 results (all users, including Diana with null department)
		if len(results) < 4 {
			t.Errorf("Expected at least 4 results from left join, got %d", len(results))
		}

		t.Logf("Left join returned %d results", len(results))
		for i, result := range results {
			data := pg.DeserializeDataForTesting(result.Data)
			t.Logf("Result %d: %+v", i+1, data)
		}
	})

	t.Run("RightJoin", func(t *testing.T) {
		results, err := pg.RightJoin("users", "departments", "department_id", "id")
		if err != nil {
			t.Errorf("Failed to perform right join: %v", err)
		}

		// Should return at least 3 results (may include HR department with null user)
		if len(results) < 3 {
			t.Errorf("Expected at least 3 results from right join, got %d", len(results))
		}

		t.Logf("Right join returned %d results", len(results))
		for i, result := range results {
			data := pg.DeserializeDataForTesting(result.Data)
			t.Logf("Result %d: %+v", i+1, data)
		}
	})

	t.Run("CrossJoin", func(t *testing.T) {
		results, err := pg.CrossJoin("users", "departments")
		if err != nil {
			t.Errorf("Failed to perform cross join: %v", err)
		}

		// Should return 4 * 3 = 12 results (cartesian product)
		expectedResults := 12
		if len(results) != expectedResults {
			t.Errorf("Expected %d results from cross join, got %d", expectedResults, len(results))
		}

		t.Logf("Cross join returned %d results", len(results))
	})
}

// TestComplexQueries tests complex queries with multiple joins and conditions
func TestComplexQueries(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("postgres_complex_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(testDir)

	pg, err := engine.NewPostgresEngine(testDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer pg.Close()

	// Setup database
	err = pg.CreateDatabase("testdb")
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	err = pg.UseDatabase("testdb")
	if err != nil {
		t.Fatalf("Failed to use database: %v", err)
	}

	t.Run("SetupComplexSchema", func(t *testing.T) {
		// Create tables for a more complex scenario
		tables := map[string]types.Schema{
			"customers": {
				Columns: []types.Column{
					{Name: "id", Type: types.IntType, Nullable: false, IsPrimaryKey: true},
					{Name: "name", Type: types.VarcharType, Size: 100, Nullable: false},
					{Name: "email", Type: types.VarcharType, Size: 100, Nullable: false, IsUnique: true},
				},
			},
			"orders": {
				Columns: []types.Column{
					{Name: "id", Type: types.IntType, Nullable: false, IsPrimaryKey: true},
					{Name: "customer_id", Type: types.IntType, Nullable: false},
					{Name: "total", Type: types.FloatType, Nullable: false},
					{Name: "status", Type: types.VarcharType, Size: 20, Nullable: false},
				},
			},
			"products": {
				Columns: []types.Column{
					{Name: "id", Type: types.IntType, Nullable: false, IsPrimaryKey: true},
					{Name: "name", Type: types.VarcharType, Size: 100, Nullable: false},
					{Name: "price", Type: types.FloatType, Nullable: false},
				},
			},
			"order_items": {
				Columns: []types.Column{
					{Name: "order_id", Type: types.IntType, Nullable: false},
					{Name: "product_id", Type: types.IntType, Nullable: false},
					{Name: "quantity", Type: types.IntType, Nullable: false},
					{Name: "price", Type: types.FloatType, Nullable: false},
				},
			},
		}

		for tableName, schema := range tables {
			err := pg.CreateTable(tableName, schema)
			if err != nil {
				t.Errorf("Failed to create table %s: %v", tableName, err)
			}
		}

		// Add primary keys
		primaryKeys := map[string][]string{
			"customers":   {"id"},
			"orders":      {"id"},
			"products":    {"id"},
			"order_items": {"order_id", "product_id"},
		}

		for tableName, pkColumns := range primaryKeys {
			err := pg.AddPrimaryKey(tableName, pkColumns)
			if err != nil {
				t.Errorf("Failed to add primary key to %s: %v", tableName, err)
			}
		}

		// Add foreign keys
		err = pg.AddForeignKey("orders", []string{"customer_id"}, "customers", []string{"id"}, "RESTRICT", "RESTRICT")
		if err != nil {
			t.Errorf("Failed to add foreign key orders -> customers: %v", err)
		}

		err = pg.AddForeignKey("order_items", []string{"order_id"}, "orders", []string{"id"}, "CASCADE", "CASCADE")
		if err != nil {
			t.Errorf("Failed to add foreign key order_items -> orders: %v", err)
		}

		err = pg.AddForeignKey("order_items", []string{"product_id"}, "products", []string{"id"}, "RESTRICT", "RESTRICT")
		if err != nil {
			t.Errorf("Failed to add foreign key order_items -> products: %v", err)
		}
	})

	t.Run("InsertComplexTestData", func(t *testing.T) {
		// Insert customers
		customers := []map[string]any{
			{"id": 1, "name": "Alice Johnson", "email": "alice@example.com"},
			{"id": 2, "name": "Bob Smith", "email": "bob@example.com"},
			{"id": 3, "name": "Charlie Brown", "email": "charlie@example.com"},
		}
		for _, customer := range customers {
			err := pg.Insert("customers", customer)
			if err != nil {
				t.Errorf("Failed to insert customer: %v", err)
			}
		}

		// Insert products
		products := []map[string]any{
			{"id": 1, "name": "Laptop", "price": 999.99},
			{"id": 2, "name": "Mouse", "price": 29.99},
			{"id": 3, "name": "Keyboard", "price": 79.99},
		}
		for _, product := range products {
			err := pg.Insert("products", product)
			if err != nil {
				t.Errorf("Failed to insert product: %v", err)
			}
		}

		// Insert orders
		orders := []map[string]any{
			{"id": 1, "customer_id": 1, "total": 1109.97, "status": "completed"},
			{"id": 2, "customer_id": 2, "total": 29.99, "status": "pending"},
			{"id": 3, "customer_id": 1, "total": 79.99, "status": "shipped"},
		}
		for _, order := range orders {
			err := pg.Insert("orders", order)
			if err != nil {
				t.Errorf("Failed to insert order: %v", err)
			}
		}

		// Insert order items
		orderItems := []map[string]any{
			{"order_id": 1, "product_id": 1, "quantity": 1, "price": 999.99},
			{"order_id": 1, "product_id": 2, "quantity": 1, "price": 29.99},
			{"order_id": 1, "product_id": 3, "quantity": 1, "price": 79.99},
			{"order_id": 2, "product_id": 2, "quantity": 1, "price": 29.99},
			{"order_id": 3, "product_id": 3, "quantity": 1, "price": 79.99},
		}
		for _, item := range orderItems {
			err := pg.Insert("order_items", item)
			if err != nil {
				t.Errorf("Failed to insert order item: %v", err)
			}
		}
	})

	t.Run("QueryPlanExecution", func(t *testing.T) {
		// Create a complex query plan
		customerTable, _ := pg.GetTable("customers")
		orderTable, _ := pg.GetTable("orders")

		queryPlan := &types.QueryPlan{
			Tables:         []*types.Table{customerTable, orderTable},
			JoinType:       types.InnerJoin,
			JoinConditions: []types.JoinCondition{
				{LeftColumn: "id", RightColumn: "customer_id", Operator: "="},
			},
			SelectColumns: []string{"name", "total", "status"},
		}

		results, err := pg.Query(queryPlan)
		if err != nil {
			t.Errorf("Failed to execute query plan: %v", err)
		}

		if len(results) == 0 {
			t.Error("Query plan should return some results")
		}

		t.Logf("Query plan returned %d results", len(results))
		for i, result := range results {
			data := pg.DeserializeDataForTesting(result.Data)
			t.Logf("Result %d: %+v", i+1, data)
		}
	})
}

// TestConstraintViolationRecovery tests recovery from constraint violations
func TestConstraintViolationRecovery(t *testing.T) {
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("postgres_recovery_test_%d", time.Now().UnixNano()))
	defer os.RemoveAll(testDir)

	pg, err := engine.NewPostgresEngine(testDir)
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer pg.Close()

	// Setup database
	err = pg.CreateDatabase("testdb")
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}
	err = pg.UseDatabase("testdb")
	if err != nil {
		t.Fatalf("Failed to use database: %v", err)
	}

	t.Run("TransactionRollbackOnConstraintViolation", func(t *testing.T) {
		schema := types.Schema{
			Columns: []types.Column{
				{Name: "id", Type: types.IntType, Nullable: false, IsPrimaryKey: true},
				{Name: "email", Type: types.VarcharType, Size: 100, Nullable: false, IsUnique: true},
				{Name: "name", Type: types.VarcharType, Size: 50, Nullable: false},
			},
		}
		err := pg.CreateTable("users", schema)
		if err != nil {
			t.Errorf("Failed to create table: %v", err)
		}

		err = pg.AddPrimaryKey("users", []string{"id"})
		if err != nil {
			t.Errorf("Failed to add primary key: %v", err)
		}

		// Insert first user successfully
		err = pg.Insert("users", map[string]any{
			"id":    1,
			"email": "alice@example.com",
			"name":  "Alice",
		})
		if err != nil {
			t.Errorf("Failed to insert first user: %v", err)
		}

		// Try to insert user with duplicate email (should fail)
		err = pg.Insert("users", map[string]any{
			"id":    2,
			"email": "alice@example.com",
			"name":  "Bob",
		})
		if err == nil {
			t.Error("Should fail due to unique constraint violation")
		}

		// Verify that the database is still in a consistent state
		results, err := pg.Select("users", nil)
		if err != nil {
			t.Errorf("Failed to select users after constraint violation: %v", err)
		}

		if len(results) != 1 {
			t.Errorf("Expected 1 user after constraint violation, got %d", len(results))
		}

		// Insert another user with different email (should succeed)
		err = pg.Insert("users", map[string]any{
			"id":    2,
			"email": "bob@example.com",
			"name":  "Bob",
		})
		if err != nil {
			t.Errorf("Failed to insert user after constraint violation: %v", err)
		}
	})
}

// BenchmarkConstraintValidation benchmarks constraint validation performance
func BenchmarkConstraintValidation(b *testing.B) {
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("postgres_constraint_bench_%d", time.Now().UnixNano()))
	defer os.RemoveAll(testDir)

	pg, err := engine.NewPostgresEngine(testDir)
	if err != nil {
		b.Fatalf("Failed to create engine: %v", err)
	}
	defer pg.Close()

	// Setup
	err = pg.CreateDatabase("benchdb")
	if err != nil {
		b.Fatalf("Failed to create database: %v", err)
	}
	err = pg.UseDatabase("benchdb")
	if err != nil {
		b.Fatalf("Failed to use database: %v", err)
	}

	schema := types.Schema{
		Columns: []types.Column{
			{Name: "id", Type: types.IntType, Nullable: false, IsPrimaryKey: true},
			{Name: "email", Type: types.VarcharType, Size: 100, Nullable: false, IsUnique: true},
			{Name: "name", Type: types.VarcharType, Size: 50, Nullable: false},
		},
	}
	err = pg.CreateTable("bench_users", schema)
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	err = pg.AddPrimaryKey("bench_users", []string{"id"})
	if err != nil {
		b.Fatalf("Failed to add primary key: %v", err)
	}

	b.Run("PrimaryKeyValidation", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err := pg.Insert("bench_users", map[string]any{
				"id":    i + 1000000, // Large ID to avoid conflicts
				"email": fmt.Sprintf("user%d@example.com", i),
				"name":  fmt.Sprintf("User %d", i),
			})
			if err != nil {
				b.Errorf("Insert failed: %v", err)
			}
		}
	})
}

// BenchmarkJoinOperations benchmarks JOIN operation performance
func BenchmarkJoinOperations(b *testing.B) {
	testDir := filepath.Join(os.TempDir(), fmt.Sprintf("postgres_join_bench_%d", time.Now().UnixNano()))
	defer os.RemoveAll(testDir)

	pg, err := engine.NewPostgresEngine(testDir)
	if err != nil {
		b.Fatalf("Failed to create engine: %v", err)
	}
	defer pg.Close()

	// Setup
	err = pg.CreateDatabase("benchdb")
	if err != nil {
		b.Fatalf("Failed to create database: %v", err)
	}
	err = pg.UseDatabase("benchdb")
	if err != nil {
		b.Fatalf("Failed to use database: %v", err)
	}

	// Create tables
	userSchema := types.Schema{
		Columns: []types.Column{
			{Name: "id", Type: types.IntType, Nullable: false, IsPrimaryKey: true},
			{Name: "name", Type: types.VarcharType, Size: 50, Nullable: false},
			{Name: "department_id", Type: types.IntType, Nullable: true},
		},
	}
	err = pg.CreateTable("bench_users", userSchema)
	if err != nil {
		b.Fatalf("Failed to create users table: %v", err)
	}

	deptSchema := types.Schema{
		Columns: []types.Column{
			{Name: "id", Type: types.IntType, Nullable: false, IsPrimaryKey: true},
			{Name: "name", Type: types.VarcharType, Size: 50, Nullable: false},
		},
	}
	err = pg.CreateTable("bench_departments", deptSchema)
	if err != nil {
		b.Fatalf("Failed to create departments table: %v", err)
	}

	// Insert test data
	for i := range 10 {
		err := pg.Insert("bench_departments", map[string]any{
			"id":   i + 1,
			"name": fmt.Sprintf("Department %d", i+1),
		})
		if err != nil {
			b.Fatalf("Failed to insert department: %v", err)
		}
	}

	for i := range 100 {
		err := pg.Insert("bench_users", map[string]any{
			"id":            i + 1,
			"name":          fmt.Sprintf("User %d", i+1),
			"department_id": (i % 10) + 1,
		})
		if err != nil {
			b.Fatalf("Failed to insert user: %v", err)
		}
	}

	b.Run("InnerJoin", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := pg.InnerJoin("bench_users", "bench_departments", "department_id", "id")
			if err != nil {
				b.Errorf("Inner join failed: %v", err)
			}
		}
	})

	b.Run("LeftJoin", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := pg.LeftJoin("bench_users", "bench_departments", "department_id", "id")
			if err != nil {
				b.Errorf("Left join failed: %v", err)
			}
		}
	})

	b.Run("CrossJoin", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := pg.CrossJoin("bench_users", "bench_departments")
			if err != nil {
				b.Errorf("Cross join failed: %v", err)
			}
		}
	})
}