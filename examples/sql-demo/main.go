package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/esgi-git/postgres-engine/internal/engine"
)

func main() {
	fmt.Println("=== PostgreSQL Engine SQL Parser Demo ===")

	// Initialize engine
	pg, err := engine.NewPostgresEngine("./demo_data")
	if err != nil {
		log.Fatal("Failed to create engine:", err)
	}
	defer pg.Close()

	// Clean up previous data
	os.RemoveAll("./demo_data")

	fmt.Println("\n🗄️ Creating database and table with SQL...")

	// Create database using SQL
	result, err := pg.ExecuteSQL("CREATE DATABASE ecommerce")
	if err != nil {
		log.Printf("Database creation warning: %v\n", err)
	} else {
		fmt.Printf("✓ %s\n", result.Message)
	}

	// Create table using SQL
	createTableSQL := `
		CREATE TABLE products (
			id INT NOT NULL,
			name VARCHAR(100) NOT NULL,
			price FLOAT NOT NULL,
			category VARCHAR(50),
			in_stock BOOLEAN NOT NULL,
			PRIMARY KEY (id)
		)
	`

	result, err = pg.ExecuteSQL(createTableSQL)
	if err != nil {
		log.Fatal("Failed to create table:", err)
	}
	fmt.Printf("✓ %s\n", result.Message)

	fmt.Println("\n📊 Inserting data with SQL...")

	// Insert multiple products using SQL
	insertSQL := `
		INSERT INTO products (id, name, price, category, in_stock) VALUES
		(1, 'MacBook Pro', 2499.99, 'Laptops', true),
		(2, 'iPhone 15', 999.99, 'Phones', true),
		(3, 'iPad Air', 599.99, 'Tablets', false),
		(4, 'AirPods Pro', 249.99, 'Audio', true),
		(5, 'Mac Studio', 1999.99, 'Desktops', true)
	`

	result, err = pg.ExecuteSQL(insertSQL)
	if err != nil {
		log.Fatal("Failed to insert data:", err)
	}
	fmt.Printf("✓ %s\n", result.Message)

	fmt.Println("\n🔍 Querying data with SQL...")

	// Select all products
	result, err = pg.ExecuteSQL("SELECT * FROM products")
	if err != nil {
		log.Fatal("Failed to select data:", err)
	}

	fmt.Printf("📋 All Products (%d rows):\n", len(result.Data))
	printTableResult(result)

	// Select with WHERE clause
	result, err = pg.ExecuteSQL("SELECT name, price FROM products WHERE in_stock = true")
	if err != nil {
		log.Fatal("Failed to select with WHERE:", err)
	}

	fmt.Printf("\n📦 In-Stock Products (%d rows):\n", len(result.Data))
	printTableResult(result)

	// Select with price filter
	result, err = pg.ExecuteSQL("SELECT * FROM products WHERE price > 1000")
	if err != nil {
		log.Fatal("Failed to select expensive products:", err)
	}

	fmt.Printf("\n💰 Expensive Products (>$1000) (%d rows):\n", len(result.Data))
	printTableResult(result)

	fmt.Println("\n🔄 Updating data with SQL...")

	// Update a product
	result, err = pg.ExecuteSQL("UPDATE products SET price = 549.99 WHERE id = 3")
	if err != nil {
		log.Fatal("Failed to update data:", err)
	}
	fmt.Printf("✓ %s\n", result.Message)

	// Check the update
	result, err = pg.ExecuteSQL("SELECT name, price FROM products WHERE id = 3")
	if err != nil {
		log.Fatal("Failed to verify update:", err)
	}

	fmt.Printf("📱 Updated Product:\n")
	printTableResult(result)

	fmt.Println("\n🗑️ Deleting data with SQL...")

	// Delete a product
	result, err = pg.ExecuteSQL("DELETE FROM products WHERE id = 5")
	if err != nil {
		log.Fatal("Failed to delete data:", err)
	}
	fmt.Printf("✓ %s\n", result.Message)

	// Verify deletion
	result, err = pg.ExecuteSQL("SELECT COUNT(*) as total_products FROM products")
	if err != nil {
		log.Printf("Count query not fully supported: %v\n", err)
		// Fallback to SELECT *
		result, err = pg.ExecuteSQL("SELECT * FROM products")
		if err != nil {
			log.Fatal("Failed to count products:", err)
		}
		fmt.Printf("📊 Remaining products: %d\n", len(result.Data))
	} else {
		printTableResult(result)
	}

	fmt.Println("\n🏗️ Creating index with SQL...")

	// Create an index
	result, err = pg.ExecuteSQL("CREATE INDEX idx_category ON products (category)")
	if err != nil {
		log.Fatal("Failed to create index:", err)
	}
	fmt.Printf("✓ %s\n", result.Message)

	// Create unique index
	result, err = pg.ExecuteSQL("CREATE UNIQUE INDEX idx_product_name ON products (name)")
	if err != nil {
		log.Fatal("Failed to create unique index:", err)
	}
	fmt.Printf("✓ %s\n", result.Message)

	fmt.Println("\n🧪 Testing complex SQL queries...")

	// Test different data types
	complexTableSQL := `
		CREATE TABLE test_types (
			id INT NOT NULL,
			description TEXT,
			price DECIMAL,
			created_date DATE,
			is_active BOOLEAN,
			metadata VARCHAR(200)
		)
	`

	result, err = pg.ExecuteSQL(complexTableSQL)
	if err != nil {
		log.Fatal("Failed to create complex table:", err)
	}
	fmt.Printf("✓ %s\n", result.Message)

	// Insert with different types
	complexInsertSQL := `
		INSERT INTO test_types (id, description, price, created_date, is_active, metadata) VALUES
		(1, 'Test product with long description', 99.99, '2024-01-15', true, 'some metadata'),
		(2, 'Another test item', 149.50, '2024-02-20', false, 'more metadata')
	`

	result, err = pg.ExecuteSQL(complexInsertSQL)
	if err != nil {
		log.Fatal("Failed to insert complex data:", err)
	}
	fmt.Printf("✓ %s\n", result.Message)

	// Query complex data
	result, err = pg.ExecuteSQL("SELECT * FROM test_types")
	if err != nil {
		log.Fatal("Failed to select complex data:", err)
	}

	fmt.Printf("\n📊 Complex Data Types (%d rows):\n", len(result.Data))
	printTableResult(result)

	fmt.Println("\n🎯 SQL Parser Demo completed successfully!")
	fmt.Printf("💾 Data persisted to ./demo_data/\n")
	fmt.Printf("🔄 Run again to see data persistence!\n")

	// Show JSON output
	fmt.Println("\n📄 Sample JSON output:")
	jsonData, _ := json.MarshalIndent(result, "", "  ")
	fmt.Printf("%s\n", string(jsonData))
}

// printTableResult prints SQL result in a nice table format
func printTableResult(result *engine.SQLResult) {
	if len(result.Data) == 0 {
		fmt.Println("   No data returned")
		return
	}

	// Print headers
	fmt.Printf("   ")
	for i, col := range result.Columns {
		if i > 0 {
			fmt.Printf(" | ")
		}
		fmt.Printf("%-15s", col)
	}
	fmt.Println()

	// Print separator
	fmt.Printf("   ")
	for i := range result.Columns {
		if i > 0 {
			fmt.Printf("-+-")
		}
		fmt.Printf("%-15s", "---------------")
	}
	fmt.Println()

	// Print data rows
	for _, row := range result.Data {
		fmt.Printf("   ")
		for i, col := range result.Columns {
			if i > 0 {
				fmt.Printf(" | ")
			}
			value := row[col]
			if value == nil {
				fmt.Printf("%-15s", "NULL")
			} else {
				fmt.Printf("%-15v", value)
			}
		}
		fmt.Println()
	}
}
