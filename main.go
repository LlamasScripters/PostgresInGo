package main

import (
	"fmt"
	"log"

	"github.com/LlamasScripters/PostgresInGo/internal/engine"
	"github.com/LlamasScripters/PostgresInGo/internal/types"
)

func main() {
	fmt.Println("=== PostgreSQL Engine Demo ===")

	// Initialize engine
	pg, err := engine.NewPostgresEngine("./data")
	if err != nil {
		log.Fatal("Failed to create engine:", err)
	}
	defer pg.Close()

	// Create database
	err = pg.CreateDatabase("testdb")
	if err != nil {
		fmt.Printf("Database exists, using existing: %v\n", err)
		if err := pg.UseDatabase("testdb"); err != nil {
			log.Fatal("Failed to use database:", err)
		}
	} else {
		fmt.Println("✓ Database 'testdb' created")
	}

	// Create table schema
	userSchema := types.Schema{
		Columns: []types.Column{
			{Name: "id", Type: types.IntType, Nullable: false},
			{Name: "name", Type: types.VarcharType, Size: 50, Nullable: false},
			{Name: "email", Type: types.VarcharType, Size: 100, Nullable: true},
		},
	}

	// Create table
	err = pg.CreateTable("users", userSchema)
	if err != nil {
		fmt.Printf("Table exists, using existing: %v\n", err)
	} else {
		fmt.Println("✓ Table 'users' created")
	}

	// Check existing data first
	existing, err := pg.Select("users", nil)
	if err == nil && len(existing) > 0 {
		fmt.Printf("Found %d existing users:\n", len(existing))
		for i, tuple := range existing {
			data := pg.DeserializeDataForTesting(tuple.Data)
			fmt.Printf("  %d. %s (ID: %v)\n", i+1, data["name"], data["id"])
		}
	}

	// Insert new data
	newUser := map[string]any{
		"id":    len(existing) + 1,
		"name":  fmt.Sprintf("User %d", len(existing)+1),
		"email": fmt.Sprintf("user%d@example.com", len(existing)+1),
	}

	if err := pg.Insert("users", newUser); err != nil {
		log.Printf("Insert failed: %v", err)
	} else {
		fmt.Printf("✓ Inserted new user: %s\n", newUser["name"])
	}

	// Demonstrate CRUD operations
	fmt.Println("\n--- CRUD Operations ---")

	// SELECT with filter
	filter := map[string]any{"id": 1}
	results, err := pg.Select("users", filter)
	if err != nil {
		log.Printf("Select failed: %v", err)
	} else {
		fmt.Printf("✓ Found %d users with ID=1\n", len(results))
	}

	// UPDATE
	if len(results) > 0 {
		updated, err := pg.Update("users", filter, map[string]any{"name": "Updated User"})
		if err != nil {
			log.Printf("Update failed: %v", err)
		} else {
			fmt.Printf("✓ Updated %d records\n", updated)
		}
	}

	// Final count
	finalResults, err := pg.Select("users", nil)
	if err != nil {
		log.Printf("Final select failed: %v", err)
	} else {
		fmt.Printf("✓ Total users: %d\n", len(finalResults))
	}

	fmt.Println("\n💾 Data persisted to ./data/")
	fmt.Println("Run again to see data persistence!")
}
