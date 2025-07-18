package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/LlamasScripters/PostgresInGo/internal/engine"
)

func main() {
	var (
		command    = flag.String("command", "", "SQL command to execute")
		cmdShort   = flag.String("c", "", "SQL command to execute (short)")
		dataFolder = flag.String("data-folder", "./data", "Data folder path")
		dataShort  = flag.String("d", "./data", "Data folder path (short)")
		sqlFile    = flag.String("sql", "", "SQL file to execute")
		sqlShort   = flag.String("s", "", "SQL file to execute (short)")
		help       = flag.Bool("help", false, "Show help")
		helpShort  = flag.Bool("h", false, "Show help (short)")
		demo       = flag.Bool("demo", false, "Run comprehensive demo")
	)

	flag.Parse()

	// Show help
	if *help || *helpShort {
		showHelp()
		return
	}

	// Run demo if requested
	if *demo {
		runDemo()
		return
	}

	// Determine data folder (long flag takes precedence)
	dataDir := *dataFolder
	if *dataShort != "./data" && *dataFolder == "./data" {
		dataDir = *dataShort
	}

	// Initialize engine
	pg, err := engine.NewPostgresEngine(dataDir)
	if err != nil {
		log.Fatal("Failed to create engine:", err)
	}
	defer pg.Close()

	// Determine SQL to execute
	var sql string

	// Priority: command flag > sql file > stdin
	if *command != "" {
		sql = *command
	} else if *cmdShort != "" {
		sql = *cmdShort
	} else if *sqlFile != "" {
		sql = readSQLFile(*sqlFile)
	} else if *sqlShort != "" {
		sql = readSQLFile(*sqlShort)
	} else {
		// Check if stdin has data
		sql = readFromStdin()
	}

	if sql == "" {
		fmt.Println("No SQL command provided. Use --help for usage information.")
		os.Exit(1)
	}

	// Execute SQL
	result, err := pg.ExecuteSQL(sql)
	if err != nil {
		log.Fatal("SQL execution failed:", err)
	}

	// Output results
	if result.Data != nil {
		displayResults(result)
	} else {
		fmt.Println(result.Message)
	}
}

func showHelp() {
	fmt.Println("PostgresInGo - A PostgreSQL-compatible database engine")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  PostgresInGo [flags]")
	fmt.Println()
	fmt.Println("Flags:")
	fmt.Println("  -c, --command <sql>        Execute SQL command")
	fmt.Println("  -d, --data-folder <path>   Data folder path (default: ./data)")
	fmt.Println("  -s, --sql <file>           Execute SQL from file")
	fmt.Println("  -h, --help                 Show this help message")
	fmt.Println("      --demo                 Run comprehensive demo")
	fmt.Println()
	fmt.Println("Examples:")
	fmt.Println("  PostgresInGo -c \"SELECT * FROM users\" -d /path/to/data")
	fmt.Println("  PostgresInGo --sql queries.sql")
	fmt.Println("  echo \"SELECT * FROM users\" | PostgresInGo -d /path/to/data")
	fmt.Println("  PostgresInGo --demo")
}

func readSQLFile(filename string) string {
	content, err := os.ReadFile(filename)
	if err != nil {
		log.Fatal("Failed to read SQL file:", err)
	}
	return string(content)
}

func readFromStdin() string {
	stat, err := os.Stdin.Stat()
	if err != nil {
		return ""
	}

	// Check if there's data in stdin
	if (stat.Mode() & os.ModeCharDevice) == 0 {
		var sql strings.Builder
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			sql.WriteString(scanner.Text())
			sql.WriteString("\n")
		}
		if err := scanner.Err(); err != nil && err != io.EOF {
			log.Fatal("Failed to read from stdin:", err)
		}
		return strings.TrimSpace(sql.String())
	}

	return ""
}

func displayResults(result *engine.SQLResult) {
	if len(result.Data) == 0 {
		fmt.Println("No results found.")
		return
	}

	// Get column names
	columns := result.Columns
	if len(columns) == 0 && len(result.Data) > 0 {
		// Extract columns from first row
		for col := range result.Data[0] {
			columns = append(columns, col)
		}
	}

	// Calculate column widths
	colWidths := make(map[string]int)
	for _, col := range columns {
		colWidths[col] = len(col)
	}

	for _, row := range result.Data {
		for _, col := range columns {
			if val, ok := row[col]; ok {
				valStr := fmt.Sprintf("%v", val)
				if len(valStr) > colWidths[col] {
					colWidths[col] = len(valStr)
				}
			}
		}
	}

	// Print header
	fmt.Print("┌")
	for i, col := range columns {
		fmt.Print(strings.Repeat("─", colWidths[col]+2))
		if i < len(columns)-1 {
			fmt.Print("┬")
		}
	}
	fmt.Println("┐")

	fmt.Print("│")
	for _, col := range columns {
		fmt.Printf(" %-*s │", colWidths[col], col)
	}
	fmt.Println()

	fmt.Print("├")
	for i, col := range columns {
		fmt.Print(strings.Repeat("─", colWidths[col]+2))
		if i < len(columns)-1 {
			fmt.Print("┼")
		}
	}
	fmt.Println("┤")

	// Print data rows
	for _, row := range result.Data {
		fmt.Print("│")
		for _, col := range columns {
			val := ""
			if v, ok := row[col]; ok {
				val = fmt.Sprintf("%v", v)
			}
			fmt.Printf(" %-*s │", colWidths[col], val)
		}
		fmt.Println()
	}

	// Print footer
	fmt.Print("└")
	for i, col := range columns {
		fmt.Print(strings.Repeat("─", colWidths[col]+2))
		if i < len(columns)-1 {
			fmt.Print("┴")
		}
	}
	fmt.Println("┘")

	fmt.Printf("\n(%d rows)\n", len(result.Data))
}

func runDemo() {
	fmt.Println("🚀 Running PostgresInGo Demo...")
	fmt.Println("Note: Demo functionality has been moved to demo.go")
	fmt.Println("To run the full demo, use: go run demo.go")
	
	// Call the actual demo function
	runComprehensiveDemo()
}