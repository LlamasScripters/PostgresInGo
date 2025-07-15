package main

import (
	"fmt"

	"github.com/LlamasScripters/PostgresInGo/internal/execution"
	"github.com/LlamasScripters/PostgresInGo/internal/parser"
	"github.com/LlamasScripters/PostgresInGo/internal/types"
)

func main() {
	fmt.Println("=== PostgreSQL Aggregate Functions Demo ===")
	
	// Test SQL parsing for aggregate functions
	testAggregateParsing()
	
	// Test aggregate operator functionality
	testAggregateOperator()
}

func testAggregateParsing() {
	fmt.Println("\n--- Testing Aggregate Function Parsing ---")
	
	aggregateQueries := []string{
		"SELECT COUNT(*) FROM employees",
		"SELECT SUM(salary) FROM employees",
		"SELECT AVG(age) FROM employees",
		"SELECT MIN(salary), MAX(salary) FROM employees",
		"SELECT COUNT(DISTINCT department) FROM employees",
		"SELECT department, COUNT(*), AVG(salary) FROM employees GROUP BY department",
		"SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > 5",
	}
	
	for i, query := range aggregateQueries {
		fmt.Printf("\n%d. Parsing: %s\n", i+1, query)
		
		p := parser.NewParser(query)
		stmt, err := p.Parse()
		
		if err != nil {
			fmt.Printf("   Error: %v\n", err)
			continue
		}
		
		if len(stmt.Statements) == 0 {
			fmt.Printf("   Error: No statements parsed\n")
			continue
		}
		
		selectStmt, ok := stmt.Statements[0].(*parser.SelectStatement)
		if !ok {
			fmt.Printf("   Error: Not a SELECT statement\n")
			continue
		}
		
		fmt.Printf("   ✓ Parsed successfully\n")
		
		// Analyze columns for aggregate functions
		for j, col := range selectStmt.Columns {
			if funcCall, ok := col.(*parser.FunctionCall); ok {
				fmt.Printf("   Column %d: %s() function", j+1, funcCall.Name)
				if funcCall.Distinct {
					fmt.Printf(" with DISTINCT")
				}
				if len(funcCall.Arguments) > 0 {
					fmt.Printf(" on %s", funcCall.Arguments[0].String())
				}
				fmt.Printf("\n")
			} else {
				fmt.Printf("   Column %d: %s\n", j+1, col.String())
			}
		}
		
		// Check for GROUP BY
		if len(selectStmt.GroupBy) > 0 {
			fmt.Printf("   GROUP BY: ")
			for k, groupCol := range selectStmt.GroupBy {
				if k > 0 {
					fmt.Printf(", ")
				}
				fmt.Printf("%s", groupCol.String())
			}
			fmt.Printf("\n")
		}
		
		// Check for HAVING
		if selectStmt.Having != nil {
			fmt.Printf("   HAVING: %s\n", selectStmt.Having.String())
		}
	}
}

func testAggregateOperator() {
	fmt.Println("\n--- Testing Aggregate Operator ---")
	
	// Create test table schema
	testTable := &types.Table{
		Name: "employees",
		Schema: types.Schema{
			Columns: []types.Column{
				{Name: "id", Type: types.IntType},
				{Name: "name", Type: types.VarcharType},
				{Name: "department", Type: types.VarcharType},
				{Name: "salary", Type: types.IntType},
				{Name: "age", Type: types.IntType},
			},
		},
	}
	
	// Create sample data
	sampleData := []*types.Tuple{
		{Data: []byte("id:1;name:Alice;department:IT;salary:75000;age:28")},
		{Data: []byte("id:2;name:Bob;department:IT;salary:82000;age:32")},
		{Data: []byte("id:3;name:Charlie;department:HR;salary:65000;age:29")},
		{Data: []byte("id:4;name:Diana;department:HR;salary:71000;age:35")},
		{Data: []byte("id:5;name:Eve;department:IT;salary:95000;age:40")},
		{Data: []byte("id:6;name:Frank;department:Finance;salary:88000;age:33")},
		{Data: []byte("id:7;name:Grace;department:Finance;salary:79000;age:30")},
	}
	
	fmt.Printf("Sample data (%d employees):\n", len(sampleData))
	for _, tuple := range sampleData {
		fmt.Printf("  %s\n", string(tuple.Data))
	}
	
	// Test Case 1: Overall statistics
	fmt.Println("\n1. Overall Statistics (COUNT, AVG, MIN, MAX):")
	testOverallStats(testTable, sampleData)
	
	// Test Case 2: Department-wise statistics
	fmt.Println("\n2. Department-wise Statistics:")
	testDepartmentStats(testTable, sampleData)
	
	// Test Case 3: Salary analysis
	fmt.Println("\n3. Salary Analysis:")
	testSalaryAnalysis(testTable, sampleData)
}

func testOverallStats(table *types.Table, data []*types.Tuple) {
	// Create mock scan operator
	mockScan := &MockScanOperator{
		table: table,
		data:  data,
	}
	
	// Define aggregates
	aggregates := []*execution.AggregateFunction{
		{Type: "COUNT", Column: "*", Alias: "total_employees"},
		{Type: "AVG", Column: "age", Alias: "avg_age"},
		{Type: "AVG", Column: "salary", Alias: "avg_salary"},
		{Type: "MIN", Column: "salary", Alias: "min_salary"},
		{Type: "MAX", Column: "salary", Alias: "max_salary"},
	}
	
	// Create and execute aggregate operator
	aggOp := execution.NewAggregateOperator(mockScan, []string{}, aggregates)
	
	err := aggOp.Open()
	if err != nil {
		fmt.Printf("   Error opening aggregate operator: %v\n", err)
		return
	}
	
	// Get result
	result, err := aggOp.Next()
	if err != nil {
		fmt.Printf("   Error getting result: %v\n", err)
		aggOp.Close()
		return
	}
	
	fmt.Printf("   Results: %s\n", string(result.Data))
	aggOp.Close()
}

func testDepartmentStats(table *types.Table, data []*types.Tuple) {
	// Create mock scan operator
	mockScan := &MockScanOperator{
		table: table,
		data:  data,
	}
	
	// Define aggregates with GROUP BY department
	aggregates := []*execution.AggregateFunction{
		{Type: "COUNT", Column: "*", Alias: "employee_count"},
		{Type: "AVG", Column: "salary", Alias: "avg_salary"},
		{Type: "MIN", Column: "age", Alias: "min_age"},
		{Type: "MAX", Column: "age", Alias: "max_age"},
	}
	
	// Create and execute aggregate operator
	aggOp := execution.NewAggregateOperator(mockScan, []string{"department"}, aggregates)
	
	err := aggOp.Open()
	if err != nil {
		fmt.Printf("   Error opening aggregate operator: %v\n", err)
		return
	}
	
	// Get all results
	resultCount := 0
	for {
		result, err := aggOp.Next()
		if err != nil {
			break
		}
		resultCount++
		fmt.Printf("   Department %d: %s\n", resultCount, string(result.Data))
	}
	
	aggOp.Close()
	fmt.Printf("   Total departments: %d\n", resultCount)
}

func testSalaryAnalysis(table *types.Table, data []*types.Tuple) {
	// Create mock scan operator
	mockScan := &MockScanOperator{
		table: table,
		data:  data,
	}
	
	// Define salary-focused aggregates
	aggregates := []*execution.AggregateFunction{
		{Type: "SUM", Column: "salary", Alias: "total_payroll"},
		{Type: "COUNT", Column: "salary", Alias: "salary_count"},
		{Type: "AVG", Column: "salary", Alias: "average_salary"},
	}
	
	// Create and execute aggregate operator
	aggOp := execution.NewAggregateOperator(mockScan, []string{}, aggregates)
	
	err := aggOp.Open()
	if err != nil {
		fmt.Printf("   Error opening aggregate operator: %v\n", err)
		return
	}
	
	// Get result
	result, err := aggOp.Next()
	if err != nil {
		fmt.Printf("   Error getting result: %v\n", err)
		aggOp.Close()
		return
	}
	
	fmt.Printf("   Salary Analysis: %s\n", string(result.Data))
	aggOp.Close()
}

// MockScanOperator implements the Operator interface for testing
type MockScanOperator struct {
	table  *types.Table
	data   []*types.Tuple
	index  int
	opened bool
}

func (m *MockScanOperator) Open() error {
	m.opened = true
	m.index = 0
	return nil
}

func (m *MockScanOperator) Next() (*types.Tuple, error) {
	if !m.opened {
		return nil, fmt.Errorf("operator not opened")
	}
	
	if m.index >= len(m.data) {
		return nil, fmt.Errorf("no more tuples")
	}
	
	tuple := m.data[m.index]
	m.index++
	return tuple, nil
}

func (m *MockScanOperator) Close() error {
	m.opened = false
	return nil
}

func (m *MockScanOperator) GetSchema() types.Schema {
	return m.table.Schema
}

func (m *MockScanOperator) Reset() {
	m.index = 0
	m.opened = false
}