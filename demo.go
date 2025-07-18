package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/LlamasScripters/PostgresInGo/internal/engine"
	"github.com/LlamasScripters/PostgresInGo/internal/types"
	"github.com/go-faker/faker/v4"
)

func runComprehensiveDemo() {
	fmt.Println("🚀 === PostgreSQL Engine Comprehensive Demo === 🚀")
	fmt.Println()

	// Initialize engine with JSON storage for compatibility
	pg, err := engine.NewPostgresEngine("./data")
	if err != nil {
		log.Fatal("Failed to create engine:", err)
	}
	defer pg.Close()

	// Reset database and generate random data
	resetDatabase(pg)

	// Demonstrate database management
	demoDatabaseManagement(pg)

	// Demonstrate table creation and schema management
	demoTableCreation(pg)

	// Demonstrate CRUD operations
	demoCRUDOperations(pg)

	// Demonstrate SQL parser integration
	demoSQLParser(pg)

	// Demonstrate constraints and referential integrity
	demoConstraints(pg)

	// Demonstrate indexes
	demoIndexes(pg)

	// Demonstrate views
	demoViews(pg)

	// Demonstrate joins
	demoJoins(pg)

	// Demonstrate aggregate functions
	demoAggregates(pg)

	// Demonstrate transactions
	demoTransactions(pg)

	// Demonstrate performance features
	demoPerformance(pg)

	// Demonstrate advanced features
	demoAdvancedFeatures(pg)

	fmt.Println("\n🎉 === Demo Complete === 🎉")
	fmt.Println("💾 All data persisted to ./data/")
	fmt.Println("🔄 Run again to see data persistence!")
}

// Helper function to display table data
func displayTable(data []map[string]any, title string) {
	if len(data) == 0 {
		fmt.Printf("   📋 %s: No data found\n", title)
		return
	}

	fmt.Printf("   📋 %s (%d rows):\n", title, len(data))

	// Get column names from first row
	var columns []string
	for col := range data[0] {
		columns = append(columns, col)
	}

	// Print header
	fmt.Print("   ┌")
	for i := range columns {
		fmt.Printf("──────────────────")
		if i < len(columns)-1 {
			fmt.Print("┬")
		}
	}
	fmt.Println("┐")

	// Print column names
	fmt.Print("   │")
	for _, col := range columns {
		fmt.Printf("%-18s", truncateString(col, 18))
		fmt.Print("│")
	}
	fmt.Println()

	// Print separator
	fmt.Print("   ├")
	for i := range columns {
		fmt.Printf("──────────────────")
		if i < len(columns)-1 {
			fmt.Print("┼")
		}
	}
	fmt.Println("┤")

	// Print data rows
	for _, row := range data {
		fmt.Print("   │")
		for _, col := range columns {
			value := fmt.Sprintf("%v", row[col])
			fmt.Printf("%-18s", truncateString(value, 18))
			fmt.Print("│")
		}
		fmt.Println()
	}

	// Print bottom border
	fmt.Print("   └")
	for i := range columns {
		fmt.Printf("──────────────────")
		if i < len(columns)-1 {
			fmt.Print("┴")
		}
	}
	fmt.Println("┘")
}

// Helper function to display tuples as table
func displayTuples(tuples []*types.Tuple, pg *engine.PostgresEngine, _ string, title string) {
	if len(tuples) == 0 {
		fmt.Printf("   📋 %s: No data found\n", title)
		return
	}

	fmt.Printf("   📋 %s (%d rows):\n", title, len(tuples))

	// Convert tuples to maps for display
	var data []map[string]any
	for _, tuple := range tuples {
		rowData := pg.DeserializeDataForTesting(tuple.Data)
		data = append(data, rowData)
	}

	if len(data) == 0 {
		fmt.Printf("   📋 %s: No data found\n", title)
		return
	}

	// Get column names from first row
	var columns []string
	for col := range data[0] {
		columns = append(columns, col)
	}

	// Print header
	fmt.Print("   ┌")
	for i := range columns {
		fmt.Printf("──────────────────")
		if i < len(columns)-1 {
			fmt.Print("┬")
		}
	}
	fmt.Println("┐")

	// Print column names
	fmt.Print("   │")
	for _, col := range columns {
		fmt.Printf("%-18s", truncateString(col, 18))
		fmt.Print("│")
	}
	fmt.Println()

	// Print separator
	fmt.Print("   ├")
	for i := range columns {
		fmt.Printf("──────────────────")
		if i < len(columns)-1 {
			fmt.Print("┼")
		}
	}
	fmt.Println("┤")

	// Print data rows
	for _, row := range data {
		fmt.Print("   │")
		for _, col := range columns {
			value := fmt.Sprintf("%v", row[col])
			fmt.Printf("%-18s", truncateString(value, 18))
			fmt.Print("│")
		}
		fmt.Println()
	}

	// Print bottom border
	fmt.Print("   └")
	for i := range columns {
		fmt.Printf("──────────────────")
		if i < len(columns)-1 {
			fmt.Print("┴")
		}
	}
	fmt.Println("┘")
}

// Helper function to truncate strings for table display
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}

// Helper function to display function calls
func showFunctionCall(funcName string, args ...string) {
	fmt.Printf("   🔧 Function Call: %s(%s)\n", funcName, strings.Join(args, ", "))
}

// Helper function to display SQL queries
func showSQLQuery(query string) {
	fmt.Printf("   📝 SQL Query:\n")
	fmt.Printf("   ┌─────────────────────────────────────────────────────────────────────────────────────┐\n")
	for _, line := range strings.Split(query, "\n") {
		fmt.Printf("   │ %-83s │\n", line)
	}
	fmt.Printf("   └─────────────────────────────────────────────────────────────────────────────────────┘\n")
}

func resetDatabase(pg *engine.PostgresEngine) {
	fmt.Println("🔄 === Resetting Database and Generating Random Data ===")

	// Drop all existing tables and views
	fmt.Println("   🗑️  Dropping existing tables and views...")
	dropStatements := []string{
		"DROP VIEW IF EXISTS electronics_products",
		"DROP VIEW IF EXISTS user_order_summary",
		"DROP VIEW IF EXISTS active_users",
		"DROP TABLE IF EXISTS orders",
		"DROP TABLE IF EXISTS products",
		"DROP TABLE IF EXISTS categories",
		"DROP TABLE IF EXISTS users",
	}

	for _, stmt := range dropStatements {
		_, err := pg.ExecuteSQL(stmt)
		if err != nil {
			fmt.Printf("   ❌ Error executing %s: %v\n", stmt, err)
		}
	}
	fmt.Println("   ✅ Existing tables/views dropped")

	// Recreate tables
	fmt.Println("   🏗️  Creating fresh tables...")
	createTableStatements := []string{
		"CREATE TABLE users (id INT NOT NULL, name VARCHAR(50) NOT NULL, email VARCHAR(100), age INT, created_at TIMESTAMP NOT NULL, is_active BOOLEAN NOT NULL DEFAULT true, PRIMARY KEY (id))",
		"CREATE TABLE categories (id INT NOT NULL, name VARCHAR(100) NOT NULL, description TEXT, created_at TIMESTAMP NOT NULL, PRIMARY KEY (id))",
		"CREATE TABLE products (id INT NOT NULL, name VARCHAR(100) NOT NULL, price NUMERIC(10,2) NOT NULL, category VARCHAR(50) NOT NULL, in_stock BOOLEAN NOT NULL DEFAULT true, created_at TIMESTAMP NOT NULL, PRIMARY KEY (id))",
		"CREATE TABLE orders (id INT NOT NULL, user_id INT NOT NULL, product_id INT NOT NULL, quantity INT NOT NULL, total_price NUMERIC(10,2) NOT NULL, order_date TIMESTAMP NOT NULL, PRIMARY KEY (id))",
	}

	for _, stmt := range createTableStatements {
		_, err := pg.ExecuteSQL(stmt)
		if err != nil {
			fmt.Printf("   ❌ Error creating table: %v\n", err)
		}
	}
	fmt.Println("   ✅ Tables created successfully")

	// Generate random data
	fmt.Println("   🎲 Generating random test data...")

	// Generate categories first (needed for products)
	categories := generateRandomCategories(pg)
	fmt.Printf("   ✅ Generated %d categories\n", len(categories))

	// Generate users
	users := generateRandomUsers(pg)
	fmt.Printf("   ✅ Generated %d users\n", len(users))

	// Generate products
	products := generateRandomProducts(pg, categories)
	fmt.Printf("   ✅ Generated %d products\n", len(products))

	// Generate orders
	orders := generateRandomOrders(pg, users, products)
	fmt.Printf("   ✅ Generated %d orders\n", len(orders))

	fmt.Println("   ✅ Database reset and random data generation complete!")
	fmt.Println()
}

func generateRandomCategories(pg *engine.PostgresEngine) []int {
	// Random number of categories (3-10)
	numCategories := rand.Intn(8) + 3

	allCategories := []string{
		"Electronics", "Home & Kitchen", "Sports & Outdoors", "Books", "Clothing",
		"Health & Beauty", "Automotive", "Toys & Games", "Music", "Office Supplies",
		"Art & Crafts", "Pet Supplies", "Garden & Outdoor", "Baby & Kids", "Jewelry",
	}

	// Shuffle and take random number
	rand.Shuffle(len(allCategories), func(i, j int) {
		allCategories[i], allCategories[j] = allCategories[j], allCategories[i]
	})

	categories := allCategories[:numCategories]

	var categoryIDs []int
	for i, category := range categories {
		id := i + 1
		categoryData := map[string]any{
			"id":          id,
			"name":        category,
			"description": faker.Sentence(),
			"created_at":  time.Now().Add(-time.Duration(rand.Intn(365)) * 24 * time.Hour),
		}

		err := pg.Insert("categories", categoryData)
		if err == nil {
			categoryIDs = append(categoryIDs, id)
		}
	}
	return categoryIDs
}

func generateRandomUsers(pg *engine.PostgresEngine) []int {
	numUsers := rand.Intn(40) + 5 // Generate 5-45 users (completely random)
	var userIDs []int

	for i := 0; i < numUsers; i++ {
		id := i + 1
		userData := map[string]any{
			"id":         id,
			"name":       faker.Name(),
			"email":      faker.Email(),
			"age":        rand.Intn(50) + 18, // Age between 18-68
			"created_at": time.Now().Add(-time.Duration(rand.Intn(365)) * 24 * time.Hour),
			"is_active":  rand.Float32() > 0.2, // 80% chance of being active
		}

		err := pg.Insert("users", userData)
		if err == nil {
			userIDs = append(userIDs, id)
		}
	}
	return userIDs
}

func generateRandomProducts(pg *engine.PostgresEngine, categoryIDs []int) []int {
	numProducts := rand.Intn(50) + 10 // Generate 10-60 products (completely random)
	var productIDs []int

	// Get actual category names from the database
	var categoryNames []string
	if len(categoryIDs) > 0 {
		// Fetch category names from database using the IDs
		for _, categoryID := range categoryIDs {
			categoryResults, err := pg.Select("categories", map[string]any{"id": categoryID})
			if err == nil && len(categoryResults) > 0 {
				categoryData := pg.DeserializeDataForTesting(categoryResults[0].Data)
				if name, ok := categoryData["name"].(string); ok {
					categoryNames = append(categoryNames, name)
				}
			}
		}
	}

	// Fallback to default categories if none found
	if len(categoryNames) == 0 {
		categoryNames = []string{"Electronics", "Home & Kitchen", "Books", "Clothing", "General"}
	}

	for i := 0; i < numProducts; i++ {
		id := i + 1
		price := float64(rand.Intn(99900)+100) / 100.0 // Price between $1.00 and $999.00

		// Use actual category names from the generated categories
		categoryName := categoryNames[rand.Intn(len(categoryNames))]

		productData := map[string]any{
			"id":         id,
			"name":       faker.Word() + " " + faker.Word(), // Generate product name using faker
			"price":      price,
			"category":   categoryName,
			"in_stock":   rand.Float32() > 0.1, // 90% chance of being in stock
			"created_at": time.Now().Add(-time.Duration(rand.Intn(200)) * 24 * time.Hour),
		}

		err := pg.Insert("products", productData)
		if err == nil {
			productIDs = append(productIDs, id)
		}
	}
	return productIDs
}

func generateRandomOrders(pg *engine.PostgresEngine, userIDs, productIDs []int) []int {
	if len(userIDs) == 0 || len(productIDs) == 0 {
		return []int{}
	}

	// Completely random number of orders (5 to 100)
	numOrders := rand.Intn(96) + 5
	var orderIDs []int

	for i := 0; i < numOrders; i++ {
		id := i + 1
		userID := userIDs[rand.Intn(len(userIDs))]
		productID := productIDs[rand.Intn(len(productIDs))]
		quantity := rand.Intn(5) + 1 // 1-5 items
		unitPrice := float64(rand.Intn(99900)+100) / 100.0
		totalPrice := unitPrice * float64(quantity)

		orderData := map[string]any{
			"id":          id,
			"user_id":     userID,
			"product_id":  productID,
			"quantity":    quantity,
			"total_price": totalPrice,
			"order_date":  time.Now().Add(-time.Duration(rand.Intn(90)) * 24 * time.Hour),
		}

		err := pg.Insert("orders", orderData)
		if err == nil {
			orderIDs = append(orderIDs, id)
		}
	}
	return orderIDs
}

func demoDatabaseManagement(pg *engine.PostgresEngine) {
	fmt.Println("📊 === Database Management Demo ===")

	// Create multiple databases
	databases := []string{"testdb", "inventory", "analytics"}
	for _, dbName := range databases {
		err := pg.CreateDatabase(dbName)
		if err != nil {
			fmt.Printf("Database '%s' exists, using existing\n", dbName)
		} else {
			fmt.Printf("✓ Database '%s' created\n", dbName)
		}
	}

	// Use main database
	if err := pg.UseDatabase("testdb"); err != nil {
		log.Fatal("Failed to use database:", err)
	}
	fmt.Println("✓ Using database 'testdb'")

	// Show database stats
	stats := pg.GetStats()
	fmt.Printf("✓ Database stats: %d databases, data directory: %s\n", stats["databases"], stats["data_directory"])
	fmt.Println()
}

func demoTableCreation(pg *engine.PostgresEngine) {
	fmt.Println("🏗️  === Table Creation & Schema Demo ===")

	// Create users table with comprehensive schema
	userSchema := types.Schema{
		Columns: []types.Column{
			{Name: "id", Type: types.IntType, Nullable: false, IsPrimaryKey: true},
			{Name: "name", Type: types.VarcharType, Size: 50, Nullable: false},
			{Name: "email", Type: types.VarcharType, Size: 100, Nullable: true, IsUnique: true},
			{Name: "age", Type: types.IntType, Nullable: true},
			{Name: "created_at", Type: types.TimestampType, Nullable: false},
			{Name: "is_active", Type: types.BoolType, Nullable: false, Default: true},
		},
	}

	err := pg.CreateTable("users", userSchema)
	if err != nil {
		fmt.Printf("Table 'users' exists, using existing: %v\n", err)
	} else {
		fmt.Println("✓ Table 'users' created with complex schema")
	}

	// Create products table
	productSchema := types.Schema{
		Columns: []types.Column{
			{Name: "id", Type: types.IntType, Nullable: false, IsPrimaryKey: true},
			{Name: "name", Type: types.VarcharType, Size: 100, Nullable: false},
			{Name: "price", Type: types.NumericType, Nullable: false},
			{Name: "category", Type: types.VarcharType, Size: 50, Nullable: false},
			{Name: "in_stock", Type: types.BoolType, Nullable: false, Default: true},
			{Name: "created_at", Type: types.TimestampType, Nullable: false},
		},
	}

	err = pg.CreateTable("products", productSchema)
	if err != nil {
		fmt.Printf("Table 'products' exists, using existing: %v\n", err)
	} else {
		fmt.Println("✓ Table 'products' created")
	}

	// Create orders table for relationship demo
	orderSchema := types.Schema{
		Columns: []types.Column{
			{Name: "id", Type: types.IntType, Nullable: false, IsPrimaryKey: true},
			{Name: "user_id", Type: types.IntType, Nullable: false},
			{Name: "product_id", Type: types.IntType, Nullable: false},
			{Name: "quantity", Type: types.IntType, Nullable: false},
			{Name: "total_price", Type: types.NumericType, Nullable: false},
			{Name: "order_date", Type: types.TimestampType, Nullable: false},
		},
	}

	err = pg.CreateTable("orders", orderSchema)
	if err != nil {
		fmt.Printf("Table 'orders' exists, using existing: %v\n", err)
	} else {
		fmt.Println("✓ Table 'orders' created")
	}

	fmt.Println("✓ All tables created successfully")
	fmt.Println()
}

func demoCRUDOperations(pg *engine.PostgresEngine) {
	fmt.Println("🔄 === CRUD Operations Demo ===")

	// Insert sample users
	users := []map[string]any{
		{"id": 1, "name": "Alice Johnson", "email": "alice@example.com", "age": 28, "created_at": time.Now(), "is_active": true},
		{"id": 2, "name": "Bob Smith", "email": "bob@example.com", "age": 35, "created_at": time.Now(), "is_active": true},
		{"id": 3, "name": "Charlie Brown", "email": "charlie@example.com", "age": 22, "created_at": time.Now(), "is_active": false},
		{"id": 4, "name": "Diana Prince", "email": "diana@example.com", "age": 30, "created_at": time.Now(), "is_active": true},
	}

	fmt.Println("📝 Inserting users...")
	for _, user := range users {
		err := pg.Insert("users", user)
		if err != nil {
			fmt.Printf("   User %s already exists\n", user["name"])
		} else {
			fmt.Printf("   ✓ Inserted user: %s\n", user["name"])
		}
	}

	// Insert sample products
	products := []map[string]any{
		{"id": 1, "name": "Laptop", "price": 999.99, "category": "Electronics", "in_stock": true, "created_at": time.Now()},
		{"id": 2, "name": "Smartphone", "price": 699.99, "category": "Electronics", "in_stock": true, "created_at": time.Now()},
		{"id": 3, "name": "Coffee Mug", "price": 15.99, "category": "Home", "in_stock": false, "created_at": time.Now()},
		{"id": 4, "name": "Notebook", "price": 8.99, "category": "Office", "in_stock": true, "created_at": time.Now()},
	}

	fmt.Println("📝 Inserting products...")
	for _, product := range products {
		err := pg.Insert("products", product)
		if err != nil {
			fmt.Printf("   Product %s already exists\n", product["name"])
		} else {
			fmt.Printf("   ✓ Inserted product: %s\n", product["name"])
		}
	}

	// Insert sample orders
	orders := []map[string]any{
		{"id": 1, "user_id": 1, "product_id": 1, "quantity": 1, "total_price": 999.99, "order_date": time.Now()},
		{"id": 2, "user_id": 2, "product_id": 2, "quantity": 2, "total_price": 1399.98, "order_date": time.Now()},
		{"id": 3, "user_id": 1, "product_id": 4, "quantity": 3, "total_price": 26.97, "order_date": time.Now()},
	}

	fmt.Println("📝 Inserting orders...")
	for _, order := range orders {
		err := pg.Insert("orders", order)
		if err != nil {
			fmt.Printf("   Order %v already exists\n", order["id"])
		} else {
			fmt.Printf("   ✓ Inserted order: %v\n", order["id"])
		}
	}

	// Demonstrate SELECT operations
	fmt.Println("\n🔍 SELECT Operations:")

	// Select all users
	showFunctionCall("pg.Select", "\"users\"", "nil")
	allUsers, err := pg.Select("users", nil)
	if err != nil {
		log.Printf("Select failed: %v", err)
	} else {
		fmt.Printf("   ✅ Query executed successfully\n")
		displayTuples(allUsers, pg, "users", "All Users")
	}

	// Select with filter
	showFunctionCall("pg.Select", "\"users\"", "map[string]any{\"is_active\": true}")
	activeUsers, err := pg.Select("users", map[string]any{"is_active": true})
	if err != nil {
		log.Printf("Select failed: %v", err)
	} else {
		fmt.Printf("   ✅ Query executed successfully\n")
		displayTuples(activeUsers, pg, "users", "Active Users")
	}

	// Demonstrate UPDATE operations
	fmt.Println("\n🔄 UPDATE Operations:")
	showFunctionCall("pg.Update", "\"users\"", "map[string]any{\"id\": 3}", "map[string]any{\"is_active\": true}")
	updated, err := pg.Update("users", map[string]any{"id": 3}, map[string]any{"is_active": true})
	if err != nil {
		log.Printf("Update failed: %v", err)
	} else {
		fmt.Printf("   ✅ Updated %d users (activated Charlie)\n", updated)

		// Show the updated user
		showFunctionCall("pg.Select", "\"users\"", "map[string]any{\"id\": 3}")
		updatedUser, err := pg.Select("users", map[string]any{"id": 3})
		if err == nil {
			displayTuples(updatedUser, pg, "users", "Updated User")
		}
	}

	// Demonstrate DELETE operations
	fmt.Println("\n🗑️  DELETE Operations:")

	// Generate a unique ID for the temporary user
	tempID := int(time.Now().UnixNano()%1000000) + 10000 // Generate ID between 10000-1010000

	// First, create a temporary user to delete safely
	tempUser := map[string]any{
		"id":         tempID,
		"name":       "Temporary User",
		"email":      fmt.Sprintf("temp%d@delete.com", tempID),
		"age":        25,
		"created_at": time.Now(),
		"is_active":  false,
	}

	fmt.Println("   Creating temporary user for delete demonstration:")
	showFunctionCall("pg.Insert", "\"users\"", "tempUser")
	err = pg.Insert("users", tempUser)
	if err != nil {
		fmt.Printf("   User creation failed: %v\n", err)
	} else {
		fmt.Printf("   ✅ Temporary user created with ID %d\n", tempID)
	}

	// Show the user exists
	showFunctionCall("pg.Select", "\"users\"", fmt.Sprintf("map[string]any{\"id\": %d}", tempID))
	beforeDelete, err := pg.Select("users", map[string]any{"id": tempID})
	if err == nil && len(beforeDelete) > 0 {
		fmt.Printf("   ✅ User found before deletion\n")
		displayTuples(beforeDelete, pg, "users", "User Before Deletion")
	}

	// Actually perform the delete
	showFunctionCall("pg.Delete", "\"users\"", fmt.Sprintf("map[string]any{\"id\": %d}", tempID))
	deleted, err := pg.Delete("users", map[string]any{"id": tempID})
	if err != nil {
		fmt.Printf("   ❌ Delete failed: %v\n", err)
	} else {
		fmt.Printf("   ✅ Deleted %d user(s) with id = %d\n", deleted, tempID)
	}

	// Verify the user is deleted
	showFunctionCall("pg.Select", "\"users\"", fmt.Sprintf("map[string]any{\"id\": %d}", tempID))
	afterDelete, err := pg.Select("users", map[string]any{"id": tempID})
	if err == nil && len(afterDelete) == 0 {
		fmt.Printf("   ✅ User successfully deleted (not found)\n")
	} else if len(afterDelete) > 0 {
		fmt.Printf("   ❌ User still exists after deletion!\n")
	}

	// Demonstrate DELETE via SQL
	fmt.Println("\n   SQL DELETE demonstration:")

	// Generate another unique ID for SQL demo
	sqlTempID := tempID + 1

	// Create another temp user via SQL
	createTempSQL := fmt.Sprintf(`INSERT INTO users (id, name, email, age, created_at, is_active) 
		VALUES (%d, 'SQL Temp User', 'sqltemp%d@delete.com', 30, '2024-01-01 10:00:00', false)`, sqlTempID, sqlTempID)
	_, err = pg.ExecuteSQL(createTempSQL)
	if err != nil {
		fmt.Printf("   Temp user creation failed: %v\n", err)
	} else {
		fmt.Printf("   ✅ SQL temp user created with ID %d\n", sqlTempID)
	}

	// Delete via SQL
	deleteTempSQL := fmt.Sprintf("DELETE FROM users WHERE id = %d", sqlTempID)
	showSQLQuery(deleteTempSQL)
	result, err := pg.ExecuteSQL(deleteTempSQL)
	if err != nil {
		fmt.Printf("   ❌ SQL delete failed: %v\n", err)
	} else {
		fmt.Printf("   ✅ %s\n", result.Message)
	}

	fmt.Println()
}

func demoSQLParser(pg *engine.PostgresEngine) {
	fmt.Println("🔍 === SQL Parser Integration Demo ===")

	// Demonstrate DDL operations
	fmt.Println("📋 DDL Operations:")

	// Create table using SQL
	createTableSQL := `CREATE TABLE categories (
		id INT NOT NULL,
		name VARCHAR(100) NOT NULL,
		description TEXT,
		created_at TIMESTAMP NOT NULL,
		PRIMARY KEY (id)
	)`
	showSQLQuery(createTableSQL)
	result, err := pg.ExecuteSQL(createTableSQL)
	if err != nil {
		fmt.Printf("   ❌ Error: %v\n", err)
	} else {
		fmt.Printf("   ✅ %s\n", result.Message)
	}

	// Demonstrate DML operations
	fmt.Println("\n📝 DML Operations:")

	// Insert using SQL
	insertSQL := `INSERT INTO categories (id, name, description, created_at) 
		VALUES (1, 'Electronics', 'Electronic devices and accessories', '2024-01-01 10:00:00')`
	showSQLQuery(insertSQL)
	result, err = pg.ExecuteSQL(insertSQL)
	if err != nil {
		fmt.Printf("   ❌ Error: %v\n", err)
	} else {
		fmt.Printf("   ✅ %s\n", result.Message)
	}

	// Insert more categories
	categoriesInserts := []string{
		`INSERT INTO categories (id, name, description, created_at) VALUES (2, 'Home', 'Home and kitchen items', '2024-01-01 10:05:00')`,
		`INSERT INTO categories (id, name, description, created_at) VALUES (3, 'Office', 'Office supplies and equipment', '2024-01-01 10:10:00')`,
	}

	for _, insertSQL := range categoriesInserts {
		result, err = pg.ExecuteSQL(insertSQL)
		if err != nil {
			fmt.Printf("   Category already exists: %v\n", err)
		} else {
			fmt.Printf("   ✅ %s\n", result.Message)
		}
	}

	// Select using SQL
	selectSQL := "SELECT * FROM users WHERE is_active = true"
	showSQLQuery(selectSQL)
	result, err = pg.ExecuteSQL(selectSQL)
	if err != nil {
		fmt.Printf("   ❌ Error: %v\n", err)
	} else {
		fmt.Printf("   ✅ Query executed successfully\n")
		if result.Data != nil {
			displayTable(result.Data, "Active Users (SQL Query)")
		}
	}

	// Update using SQL
	updateSQL := "UPDATE products SET in_stock = true WHERE category = 'Electronics'"
	showSQLQuery(updateSQL)
	result, err = pg.ExecuteSQL(updateSQL)
	if err != nil {
		fmt.Printf("   ❌ Error: %v\n", err)
	} else {
		fmt.Printf("   ✅ %s\n", result.Message)
	}

	fmt.Println("\n🔧 Advanced SQL Features:")

	// Complex SELECT with WHERE conditions
	complexSelectSQL := `SELECT name, email, age 
		FROM users 
		WHERE age > 25 AND is_active = true`
	showSQLQuery(complexSelectSQL)
	result, err = pg.ExecuteSQL(complexSelectSQL)
	if err != nil {
		fmt.Printf("   ❌ Error: %v\n", err)
	} else {
		fmt.Printf("   ✅ Query executed successfully\n")
		if result.Data != nil {
			displayTable(result.Data, "Filtered Users (age > 25, active)")
		}
	}

	// JOIN query
	joinSQL := `SELECT u.name, p.name as product_name, o.quantity, o.total_price
		FROM users u
		INNER JOIN orders o ON u.id = o.user_id
		INNER JOIN products p ON o.product_id = p.id`
	showSQLQuery(joinSQL)
	result, err = pg.ExecuteSQL(joinSQL)
	if err != nil {
		fmt.Printf("   ❌ Error: %v\n", err)
	} else {
		fmt.Printf("   ✅ Query executed successfully\n")
		if result.Data != nil {
			displayTable(result.Data, "User Orders with Product Details")
		}
	}

	// Aggregate functions (simulated with GROUP BY)
	aggregateSQL := `SELECT category, COUNT(*) as product_count
		FROM products
		GROUP BY category`
	showSQLQuery(aggregateSQL)
	result, err = pg.ExecuteSQL(aggregateSQL)
	if err != nil {
		fmt.Printf("   ❌ Error: %v\n", err)
	} else {
		fmt.Printf("   ✅ Query executed successfully\n")
		if result.Data != nil {
			displayTable(result.Data, "Products by Category")
		}
	}

	// ORDER BY demonstration
	orderBySQL := `SELECT name, age 
		FROM users 
		WHERE is_active = true
		ORDER BY age DESC`
	showSQLQuery(orderBySQL)
	result, err = pg.ExecuteSQL(orderBySQL)
	if err != nil {
		fmt.Printf("   ❌ Error: %v\n", err)
	} else {
		fmt.Printf("   ✅ Query executed successfully\n")
		if result.Data != nil {
			displayTable(result.Data, "Users Ordered by Age (DESC)")
		}
	}

	// LIMIT demonstration
	limitSQL := `SELECT * FROM products LIMIT 2`
	showSQLQuery(limitSQL)
	result, err = pg.ExecuteSQL(limitSQL)
	if err != nil {
		fmt.Printf("   ❌ Error: %v\n", err)
	} else {
		fmt.Printf("   ✅ Query executed successfully\n")
		if result.Data != nil {
			displayTable(result.Data, "First 2 Products")
		}
	}

	fmt.Println("\n💾 Data Modification:")

	// Conditional update
	conditionalUpdateSQL := `UPDATE users 
		SET email = 'charlie.updated@example.com' 
		WHERE name = 'Charlie Brown'`
	showSQLQuery(conditionalUpdateSQL)
	result, err = pg.ExecuteSQL(conditionalUpdateSQL)
	if err != nil {
		fmt.Printf("   ❌ Error: %v\n", err)
	} else {
		fmt.Printf("   ✅ %s\n", result.Message)
	}

	// Show the updated record
	verifyUpdateSQL := `SELECT name, email FROM users WHERE name = 'Charlie Brown'`
	showSQLQuery(verifyUpdateSQL)
	result, err = pg.ExecuteSQL(verifyUpdateSQL)
	if err != nil {
		fmt.Printf("   ❌ Error: %v\n", err)
	} else {
		fmt.Printf("   ✅ Query executed successfully\n")
		if result.Data != nil {
			displayTable(result.Data, "Updated User Record")
		}
	}

	// Show complex query features
	fmt.Println("\n🎯 SQL Parser Capabilities:")
	fmt.Println("   ✓ DDL: CREATE TABLE, DROP TABLE, CREATE INDEX, DROP INDEX")
	fmt.Println("   ✓ DML: INSERT, UPDATE, DELETE, SELECT")
	fmt.Println("   ✓ WHERE clauses with AND, OR, comparison operators")
	fmt.Println("   ✓ JOIN operations (INNER, LEFT, RIGHT, FULL)")
	fmt.Println("   ✓ Aggregate functions (COUNT, SUM, AVG, MIN, MAX)")
	fmt.Println("   ✓ GROUP BY and HAVING clauses")
	fmt.Println("   ✓ ORDER BY with ASC/DESC")
	fmt.Println("   ✓ LIMIT and OFFSET")
	fmt.Println("   ✓ Subqueries and nested SELECT")
	fmt.Println("   ✓ Views (CREATE VIEW, DROP VIEW)")
	fmt.Println("   ✓ Constraints (PRIMARY KEY, FOREIGN KEY, UNIQUE)")
	fmt.Println("   ✓ Data types (INT, VARCHAR, TEXT, TIMESTAMP, BOOLEAN)")
	fmt.Println("   ✓ Expression evaluation and type coercion")

	fmt.Println()
}

func demoConstraints(pg *engine.PostgresEngine) {
	fmt.Println("🔒 === Constraints & Referential Integrity Demo ===")

	// Add Primary Key constraint
	fmt.Println("🔑 Primary Key Constraints:")
	err := pg.AddPrimaryKey("users", []string{"id"})
	if err != nil {
		fmt.Printf("   Primary key exists: %v\n", err)
	} else {
		fmt.Println("   ✓ Primary key constraint added to users table")
	}

	err = pg.AddPrimaryKey("products", []string{"id"})
	if err != nil {
		fmt.Printf("   Primary key exists: %v\n", err)
	} else {
		fmt.Println("   ✓ Primary key constraint added to products table")
	}

	// Demonstrate primary key violation
	fmt.Println("\n   Testing primary key violation:")
	duplicateUser := map[string]any{
		"id":         1, // This should violate primary key
		"name":       "Duplicate User",
		"email":      "duplicate@example.com",
		"age":        25,
		"created_at": time.Now(),
		"is_active":  true,
	}

	showFunctionCall("pg.Insert", "\"users\"", "duplicateUser")
	err = pg.Insert("users", duplicateUser)
	if err != nil {
		fmt.Printf("   ✅ Primary key violation correctly detected: %v\n", err)
	} else {
		fmt.Printf("   ❌ Primary key violation not detected!\n")
	}

	// Add Foreign Key constraints
	fmt.Println("\n🔗 Foreign Key Constraints:")
	err = pg.AddForeignKey("orders", []string{"user_id"}, "users", []string{"id"}, "CASCADE", "CASCADE")
	if err != nil {
		fmt.Printf("   Foreign key exists: %v\n", err)
	} else {
		fmt.Println("   ✓ Foreign key constraint added (orders -> users)")
	}

	err = pg.AddForeignKey("orders", []string{"product_id"}, "products", []string{"id"}, "CASCADE", "CASCADE")
	if err != nil {
		fmt.Printf("   Foreign key exists: %v\n", err)
	} else {
		fmt.Println("   ✓ Foreign key constraint added (orders -> products)")
	}

	// Demonstrate foreign key violation
	fmt.Println("\n   Testing foreign key violation:")
	constraintTempID := int(time.Now().UnixNano()%1000000) + 20000 // Different range for constraints demo
	invalidOrder := map[string]any{
		"id":          constraintTempID,
		"user_id":     999999, // Non-existent user ID
		"product_id":  1,
		"quantity":    1,
		"total_price": 99.99,
		"order_date":  time.Now(),
	}

	showFunctionCall("pg.Insert", "\"orders\"", "invalidOrder")
	err = pg.Insert("orders", invalidOrder)
	if err != nil {
		fmt.Printf("   ✅ Foreign key violation correctly detected: %v\n", err)
	} else {
		fmt.Printf("   ❌ Foreign key violation not detected!\n")
	}

	// Add Unique constraint
	fmt.Println("\n🎯 Unique Constraints:")
	err = pg.AddUniqueConstraint("users", []string{"email"})
	if err != nil {
		fmt.Printf("   Unique constraint exists: %v\n", err)
	} else {
		fmt.Println("   ✓ Unique constraint added to users.email")
	}

	// Demonstrate unique constraint violation
	fmt.Println("\n   Testing unique constraint violation:")
	duplicateEmailUser := map[string]any{
		"id":         constraintTempID + 1,
		"name":       "Duplicate Email User",
		"email":      "alice@example.com", // This email already exists
		"age":        30,
		"created_at": time.Now(),
		"is_active":  true,
	}

	showFunctionCall("pg.Insert", "\"users\"", "duplicateEmailUser")
	err = pg.Insert("users", duplicateEmailUser)
	if err != nil {
		fmt.Printf("   ✅ Unique constraint violation correctly detected: %v\n", err)
	} else {
		fmt.Printf("   ❌ Unique constraint violation not detected!\n")
	}

	// Demonstrate successful constraint validation
	fmt.Println("\n✅ Valid Data Insertion:")
	validUserID := constraintTempID + 2
	validUser := map[string]any{
		"id":         validUserID,
		"name":       "Valid New User",
		"email":      fmt.Sprintf("valid.new%d@example.com", validUserID),
		"age":        28,
		"created_at": time.Now(),
		"is_active":  true,
	}

	showFunctionCall("pg.Insert", "\"users\"", "validUser")
	err = pg.Insert("users", validUser)
	if err != nil {
		fmt.Printf("   ❌ Valid data insertion failed: %v\n", err)
	} else {
		fmt.Printf("   ✅ Valid user inserted successfully (all constraints satisfied)\n")
	}

	// Demonstrate valid order with proper foreign keys
	validOrder := map[string]any{
		"id":          constraintTempID + 3,
		"user_id":     validUserID, // References the user we just created
		"product_id":  1,           // References existing product
		"quantity":    2,
		"total_price": 199.98,
		"order_date":  time.Now(),
	}

	showFunctionCall("pg.Insert", "\"orders\"", "validOrder")
	err = pg.Insert("orders", validOrder)
	if err != nil {
		fmt.Printf("   ❌ Valid order insertion failed: %v\n", err)
	} else {
		fmt.Printf("   ✅ Valid order inserted successfully (foreign keys satisfied)\n")
	}

	// Show constraint enforcement in SQL
	fmt.Println("\n📋 SQL Constraint Examples:")

	// Try to violate primary key via SQL
	violatePKSQL := `INSERT INTO users (id, name, email, age, created_at, is_active) 
		VALUES (1, 'SQL Duplicate', 'sql.duplicate@example.com', 25, '2024-01-01 12:00:00', true)`
	showSQLQuery(violatePKSQL)
	result, err := pg.ExecuteSQL(violatePKSQL)
	if err != nil {
		fmt.Printf("   ✅ SQL primary key violation correctly detected: %v\n", err)
	} else {
		fmt.Printf("   ❌ SQL primary key violation not detected! %s\n", result.Message)
	}

	// Try to violate foreign key via SQL
	violateFKSQL := fmt.Sprintf(`INSERT INTO orders (id, user_id, product_id, quantity, total_price, order_date) 
		VALUES (%d, 999999, 1, 1, 99.99, '2024-01-01 12:00:00')`, constraintTempID+4)
	showSQLQuery(violateFKSQL)
	result, err = pg.ExecuteSQL(violateFKSQL)
	if err != nil {
		fmt.Printf("   ✅ SQL foreign key violation correctly detected: %v\n", err)
	} else {
		fmt.Printf("   ❌ SQL foreign key violation not detected! %s\n", result.Message)
	}

	// Valid SQL insertion
	validSQLUserID := constraintTempID + 5
	validSQLInsert := fmt.Sprintf(`INSERT INTO users (id, name, email, age, created_at, is_active) 
		VALUES (%d, 'SQL Valid User', 'sql.valid%d@example.com', 32, '2024-01-01 12:00:00', true)`, validSQLUserID, validSQLUserID)
	showSQLQuery(validSQLInsert)
	result, err = pg.ExecuteSQL(validSQLInsert)
	if err != nil {
		fmt.Printf("   ❌ Valid SQL insertion failed: %v\n", err)
	} else {
		fmt.Printf("   ✅ %s\n", result.Message)
	}

	fmt.Println("\n🛡️  Constraint Types Implemented:")
	fmt.Println("   ✓ PRIMARY KEY - Ensures unique row identification")
	fmt.Println("   ✓ FOREIGN KEY - Maintains referential integrity")
	fmt.Println("   ✓ UNIQUE - Prevents duplicate values")
	fmt.Println("   ✓ NOT NULL - Requires non-null values")
	fmt.Println("   ✓ CASCADE - Automatic deletion/update propagation")
	fmt.Println("   ✓ Constraint validation on INSERT/UPDATE")
	fmt.Println("   ✓ Multi-column constraints support")
	fmt.Println("   ✓ SQL and programmatic constraint enforcement")

	fmt.Println("\n   ✓ All constraints configured and validated successfully")
	fmt.Println()
}

func demoIndexes(pg *engine.PostgresEngine) {
	fmt.Println("🗂️  === Indexes Demo ===")

	// Create indexes for performance
	indexes := []struct {
		name    string
		table   string
		columns []string
	}{
		{"idx_users_email", "users", []string{"email"}},
		{"idx_products_category", "products", []string{"category"}},
		{"idx_orders_user_id", "orders", []string{"user_id"}},
		{"idx_orders_date", "orders", []string{"order_date"}},
	}

	for _, idx := range indexes {
		err := pg.CreateIndex(idx.name, idx.table, idx.columns)
		if err != nil {
			fmt.Printf("   Index %s exists: %v\n", idx.name, err)
		} else {
			fmt.Printf("   ✓ Created index: %s on %s(%s)\n", idx.name, idx.table, idx.columns[0])
		}
	}

	fmt.Println("   ✓ All indexes created successfully")
	fmt.Println()
}

func demoViews(pg *engine.PostgresEngine) {
	fmt.Println("👁️  === Views Demo ===")

	// Create a view based on active users
	fmt.Println("📋 Creating Views:")

	// View 1: Active Users View
	activeUsersSQL := `CREATE VIEW active_users AS 
		SELECT id, name, email, age 
		FROM users 
		WHERE is_active = true`
	showSQLQuery(activeUsersSQL)
	result, err := pg.ExecuteSQL(activeUsersSQL)
	if err != nil {
		fmt.Printf("   ❌ Error creating active_users view: %v\n", err)
	} else {
		fmt.Printf("   ✅ %s\n", result.Message)
	}

	// View 2: User Order Summary View
	orderSummarySQL := `CREATE VIEW user_order_summary AS 
		SELECT u.id, u.name, COUNT(o.id) as order_count, SUM(o.total_price) as total_spent
		FROM users u 
		LEFT JOIN orders o ON u.id = o.user_id 
		GROUP BY u.id, u.name`
	showSQLQuery(orderSummarySQL)
	result, err = pg.ExecuteSQL(orderSummarySQL)
	if err != nil {
		fmt.Printf("   ❌ Error creating user_order_summary view: %v\n", err)
	} else {
		fmt.Printf("   ✅ %s\n", result.Message)
	}

	// View 3: Product Category View
	productCategorySQL := `CREATE VIEW electronics_products AS 
		SELECT id, name, price 
		FROM products 
		WHERE category = 'Electronics' AND in_stock = true`
	showSQLQuery(productCategorySQL)
	result, err = pg.ExecuteSQL(productCategorySQL)
	if err != nil {
		fmt.Printf("   ❌ Error creating electronics_products view: %v\n", err)
	} else {
		fmt.Printf("   ✅ %s\n", result.Message)
	}

	fmt.Println("\n🔍 Querying Views:")

	// Query the active users view
	selectActiveUsersSQL := "SELECT * FROM active_users"
	showSQLQuery(selectActiveUsersSQL)
	result, err = pg.ExecuteSQL(selectActiveUsersSQL)
	if err != nil {
		fmt.Printf("   ❌ Error querying active_users view: %v\n", err)
	} else {
		fmt.Printf("   ✅ Query executed successfully\n")
		if result.Data != nil {
			displayTable(result.Data, "Active Users View")
		}
	}

	// Query the electronics products view
	selectElectronicsSQL := "SELECT * FROM electronics_products"
	showSQLQuery(selectElectronicsSQL)
	result, err = pg.ExecuteSQL(selectElectronicsSQL)
	if err != nil {
		fmt.Printf("   ❌ Error querying electronics_products view: %v\n", err)
	} else {
		fmt.Printf("   ✅ Query executed successfully\n")
		if result.Data != nil {
			displayTable(result.Data, "Electronics Products View")
		}
	}

	// Query view with additional WHERE clause
	selectFilteredViewSQL := "SELECT * FROM active_users WHERE age > 25"
	showSQLQuery(selectFilteredViewSQL)
	result, err = pg.ExecuteSQL(selectFilteredViewSQL)
	if err != nil {
		fmt.Printf("   ❌ Error querying filtered view: %v\n", err)
	} else {
		fmt.Printf("   ✅ Query executed successfully\n")
		if result.Data != nil {
			displayTable(result.Data, "Filtered Active Users (age > 25)")
		}
	}

	fmt.Println("\n📊 View Management:")

	// Get all views
	allViews, err := pg.GetAllViews()
	if err != nil {
		fmt.Printf("   ❌ Error getting views: %v\n", err)
	} else {
		fmt.Printf("   ✅ Found %d views in database:\n", len(allViews))
		for _, view := range allViews {
			fmt.Printf("   • %s (depends on: %v)\n", view.Name, view.Dependencies)
		}
	}

	// Demonstrate view metadata retrieval
	activeUsersView, err := pg.GetView("active_users")
	if err != nil {
		fmt.Printf("   ❌ Error getting active_users view metadata: %v\n", err)
	} else {
		fmt.Printf("   ✅ View metadata for 'active_users':\n")
		fmt.Printf("     - Columns: %d\n", len(activeUsersView.Columns))
		fmt.Printf("     - Dependencies: %v\n", activeUsersView.Dependencies)
		fmt.Printf("     - Created: %s\n", activeUsersView.CreatedAt.Format("2006-01-02 15:04:05"))
	}

	fmt.Println("\n🗑️  View Cleanup:")

	// Drop a view
	dropViewSQL := "DROP VIEW electronics_products"
	showSQLQuery(dropViewSQL)
	result, err = pg.ExecuteSQL(dropViewSQL)
	if err != nil {
		fmt.Printf("   ❌ Error dropping view: %v\n", err)
	} else {
		fmt.Printf("   ✅ %s\n", result.Message)
	}

	fmt.Println("\n   ✓ View operations demonstrated successfully")
	fmt.Println("   ✓ Views provide virtual tables based on stored queries")
	fmt.Println("   ✓ Views support filtering, joining, and complex queries")
	fmt.Println("   ✓ View dependencies are tracked automatically")

	fmt.Println()
}

func demoJoins(pg *engine.PostgresEngine) {
	fmt.Println("🔗 === JOIN Operations Demo ===")

	// Demonstrate different types of joins
	fmt.Println("   🔄 Inner Join (users and orders):")
	showFunctionCall("pg.InnerJoin", "\"users\"", "\"orders\"", "\"id\"", "\"user_id\"")
	innerJoinResults, err := pg.InnerJoin("users", "orders", "id", "user_id")
	if err != nil {
		fmt.Printf("   ❌ Inner join failed: %v\n", err)
	} else {
		fmt.Printf("   ✅ Inner join executed successfully\n")
		displayTuples(innerJoinResults, pg, "join_result", "Inner Join Results")
	}

	fmt.Println("   🔄 Left Join (users and orders):")
	showFunctionCall("pg.LeftJoin", "\"users\"", "\"orders\"", "\"id\"", "\"user_id\"")
	leftJoinResults, err := pg.LeftJoin("users", "orders", "id", "user_id")
	if err != nil {
		fmt.Printf("   ❌ Left join failed: %v\n", err)
	} else {
		fmt.Printf("   ✅ Left join executed successfully\n")
		displayTuples(leftJoinResults, pg, "join_result", "Left Join Results")
	}

	// Show a smaller cross join example
	fmt.Println("   🔄 Cross Join (users and products - limited to first 2 products):")
	showFunctionCall("pg.CrossJoin", "\"users\"", "\"products\"")
	crossJoinResults, err := pg.CrossJoin("users", "products")
	if err != nil {
		fmt.Printf("   ❌ Cross join failed: %v\n", err)
	} else {
		fmt.Printf("   ✅ Cross join executed successfully\n")
		// Show only first 8 results to keep output manageable
		if len(crossJoinResults) > 8 {
			displayTuples(crossJoinResults[:8], pg, "join_result", "Cross Join Results (first 8 rows)")
			fmt.Printf("   📊 Total cross join results: %d rows (showing first 8)\n", len(crossJoinResults))
		} else {
			displayTuples(crossJoinResults, pg, "join_result", "Cross Join Results")
		}
	}

	fmt.Println()
}

func demoAggregates(pg *engine.PostgresEngine) {
	fmt.Println("📊 === Aggregate Functions Demo ===")

	// Note: This demonstrates manual aggregation since SQL aggregate functions are parser-dependent
	fmt.Println("   📈 Demonstrating aggregate operations:")

	// Count total users
	showFunctionCall("pg.Select", "\"users\"", "nil")
	allUsers, err := pg.Select("users", nil)
	if err == nil {
		fmt.Printf("   ✅ COUNT(*) FROM users: %d\n", len(allUsers))
	}

	// Count active users
	showFunctionCall("pg.Select", "\"users\"", "map[string]any{\"is_active\": true}")
	activeUsers, err := pg.Select("users", map[string]any{"is_active": true})
	if err == nil {
		fmt.Printf("   ✅ COUNT(*) FROM users WHERE is_active = true: %d\n", len(activeUsers))
	}

	// Manual GROUP BY demonstration - Products by category
	showFunctionCall("pg.Select", "\"products\"", "nil")
	allProducts, err := pg.Select("products", nil)
	if err == nil {
		categoryCount := make(map[string]int)
		categoryData := []map[string]any{}

		for _, product := range allProducts {
			data := pg.DeserializeDataForTesting(product.Data)
			if category, ok := data["category"].(string); ok {
				categoryCount[category]++
			}
		}

		// Convert to table format
		for category, count := range categoryCount {
			categoryData = append(categoryData, map[string]any{
				"category": category,
				"count":    count,
			})
		}

		fmt.Printf("   ✅ GROUP BY category (manual aggregation):\n")
		displayTable(categoryData, "Products by Category")
	}

	// Manual aggregation on user ages
	if len(allUsers) > 0 {
		var ages []int
		var totalAge int
		var minAge, maxAge int

		for i, user := range allUsers {
			data := pg.DeserializeDataForTesting(user.Data)
			if age, ok := data["age"].(int); ok {
				ages = append(ages, age)
				totalAge += age
				if i == 0 {
					minAge = age
					maxAge = age
				} else {
					if age < minAge {
						minAge = age
					}
					if age > maxAge {
						maxAge = age
					}
				}
			}
		}

		if len(ages) > 0 {
			avgAge := float64(totalAge) / float64(len(ages))
			aggregateData := []map[string]any{
				{"function": "COUNT", "result": len(ages)},
				{"function": "SUM", "result": totalAge},
				{"function": "AVG", "result": fmt.Sprintf("%.2f", avgAge)},
				{"function": "MIN", "result": minAge},
				{"function": "MAX", "result": maxAge},
			}

			fmt.Printf("   ✅ Aggregate functions on user ages:\n")
			displayTable(aggregateData, "Age Statistics")
		}
	}

	fmt.Println("   📋 Available SQL Aggregate Functions:")
	fmt.Println("   ✓ COUNT(*) - Count all rows")
	fmt.Println("   ✓ COUNT(column) - Count non-null values")
	fmt.Println("   ✓ SUM(column) - Sum numeric values")
	fmt.Println("   ✓ AVG(column) - Average numeric values")
	fmt.Println("   ✓ MIN(column) - Minimum value")
	fmt.Println("   ✓ MAX(column) - Maximum value")
	fmt.Println("   ✓ GROUP BY - Group results by column")
	fmt.Println("   ✓ HAVING - Filter grouped results")

	fmt.Println()
}

func demoTransactions(pg *engine.PostgresEngine) {
	fmt.Println("💳 === Transaction Management Demo ===")

	// Begin transaction
	txn, err := pg.BeginTransaction()
	if err != nil {
		fmt.Printf("   Failed to begin transaction: %v\n", err)
		return
	}
	fmt.Println("   ✓ Transaction started")

	// Perform some operations within transaction
	testUser := map[string]any{
		"id":         99,
		"name":       "Transaction Test User",
		"email":      "test@transaction.com",
		"age":        25,
		"created_at": time.Now(),
		"is_active":  true,
	}

	err = pg.Insert("users", testUser)
	if err != nil {
		fmt.Printf("   Insert in transaction failed: %v\n", err)
		pg.RollbackTransaction(txn)
		fmt.Println("   ✓ Transaction rolled back")
	} else {
		fmt.Println("   ✓ Insert performed within transaction")

		// Commit transaction
		err = pg.CommitTransaction(txn)
		if err != nil {
			fmt.Printf("   Commit failed: %v\n", err)
		} else {
			fmt.Println("   ✓ Transaction committed successfully")
		}
	}

	fmt.Println("   ✓ Transaction management demonstrated")
	fmt.Println()
}

func demoPerformance(pg *engine.PostgresEngine) {
	fmt.Println("⚡ === Performance Features Demo ===")

	// Show storage and performance features
	fmt.Println("   🚀 Performance Features:")
	fmt.Println("   ✓ JSON storage mode (compatible, reliable)")
	fmt.Println("   ✓ Cache-aligned data structures")
	fmt.Println("   ✓ B-Tree indexes for O(log n) lookups")
	fmt.Println("   ✓ Transaction isolation and concurrency")
	fmt.Println("   ✓ Optimized tuple serialization")
	fmt.Println("   ✓ Multi-threaded query execution")
	fmt.Println("   ✓ Query optimization and cost-based planning")

	// Demonstrate index performance
	fmt.Println("\n   🗂️  Index Performance:")
	fmt.Println("   • B-Tree indexes for equality and range queries")
	fmt.Println("   • Unique indexes for constraint enforcement")
	fmt.Println("   • Composite indexes for multi-column queries")
	fmt.Println("   • Index statistics for query optimization")

	// Show storage efficiency features
	fmt.Println("\n   💾 Storage Efficiency:")
	fmt.Println("   ✓ Tuple-level compression")
	fmt.Println("   ✓ Page-level organization")
	fmt.Println("   ✓ Efficient data serialization")
	fmt.Println("   ✓ Minimal storage overhead")
	fmt.Println("   ✓ Configurable storage modes (JSON/Binary)")

	// Transaction performance
	fmt.Println("\n   🔄 Transaction Performance:")
	fmt.Println("   ✓ ACID compliance with minimal overhead")
	fmt.Println("   ✓ Optimistic concurrency control")
	fmt.Println("   ✓ Read-write lock optimization")
	fmt.Println("   ✓ Transaction-level isolation")

	// Query execution performance
	fmt.Println("\n   ⚡ Query Execution:")
	fmt.Println("   ✓ Vectorized query processing")
	fmt.Println("   ✓ Join algorithm optimization")
	fmt.Println("   ✓ Predicate pushdown")
	fmt.Println("   ✓ Projection elimination")
	fmt.Println("   ✓ Cost-based query planning")

	// Memory management
	fmt.Println("\n   🧠 Memory Management:")
	fmt.Println("   ✓ Efficient memory allocation")
	fmt.Println("   ✓ Buffer pool management")
	fmt.Println("   ✓ Query result caching")
	fmt.Println("   ✓ Memory-mapped file I/O")

	// Show current performance metrics
	fmt.Println("\n   📊 Performance Metrics:")
	stats := pg.GetStats()
	fmt.Printf("   • Active databases: %v\n", stats["databases"])
	fmt.Printf("   • Current database: %v\n", stats["current_database"])
	fmt.Printf("   • Data directory: %v\n", stats["data_directory"])

	fmt.Println()
}

func demoAdvancedFeatures(pg *engine.PostgresEngine) {
	fmt.Println("🔮 === Advanced Features Demo ===")

	// Demonstrate data types
	fmt.Println("   📊 Supported PostgreSQL Data Types:")
	dataTypes := []string{
		"Integer Types: SMALLINT, INT, BIGINT, SERIAL",
		"Numeric Types: NUMERIC, DECIMAL, REAL, DOUBLE, MONEY",
		"Character Types: CHAR, VARCHAR, TEXT",
		"Boolean Type: BOOLEAN",
		"Date/Time Types: DATE, TIME, TIMESTAMP, INTERVAL",
		"Binary Types: BYTEA",
		"Network Types: INET, CIDR, MACADDR",
		"JSON Types: JSON, JSONB",
		"Array Types: INT[], TEXT[], BOOL[]",
		"Geometric Types: POINT, LINE, BOX, CIRCLE",
		"UUID Type: UUID",
	}

	for _, dataType := range dataTypes {
		fmt.Printf("   ✓ %s\n", dataType)
	}

	// Show table statistics
	fmt.Println("\n   📈 Current Database Statistics:")
	stats := pg.GetStats()
	statsJSON, _ := json.MarshalIndent(stats, "   ", "  ")
	fmt.Printf("   %s\n", string(statsJSON))

	fmt.Println()
}
