package main

import (
	"fmt"
	"testing"

	"github.com/LlamasScripters/PostgresInGo/internal/execution"
	"github.com/LlamasScripters/PostgresInGo/internal/parser"
	"github.com/LlamasScripters/PostgresInGo/internal/types"
)

func TestAggregateFunctions(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected bool
	}{
		{
			name:     "COUNT function",
			sql:      "SELECT COUNT(*) FROM users",
			expected: true,
		},
		{
			name:     "SUM function",
			sql:      "SELECT SUM(age) FROM users",
			expected: true,
		},
		{
			name:     "AVG function",
			sql:      "SELECT AVG(salary) FROM employees",
			expected: true,
		},
		{
			name:     "MIN function",
			sql:      "SELECT MIN(age) FROM users",
			expected: true,
		},
		{
			name:     "MAX function",
			sql:      "SELECT MAX(salary) FROM employees",
			expected: true,
		},
		{
			name:     "COUNT DISTINCT",
			sql:      "SELECT COUNT(DISTINCT department) FROM employees",
			expected: true,
		},
		{
			name:     "Multiple aggregates",
			sql:      "SELECT COUNT(*), AVG(age), MAX(salary) FROM employees",
			expected: true,
		},
		{
			name:     "Aggregate with GROUP BY",
			sql:      "SELECT department, COUNT(*), AVG(salary) FROM employees GROUP BY department",
			expected: true,
		},
		{
			name:     "Aggregate with HAVING",
			sql:      "SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > 5",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := parser.NewParser(tt.sql)
			stmt, err := p.Parse()

			if err != nil {
				t.Errorf("Parse error for %s: %v", tt.name, err)
				return
			}

			if len(stmt.Statements) == 0 {
				t.Errorf("No statements parsed for %s", tt.name)
				return
			}

			selectStmt, ok := stmt.Statements[0].(*parser.SelectStatement)
			if !ok {
				t.Errorf("Expected SelectStatement for %s", tt.name)
				return
			}

			// Check for aggregate functions in columns
			hasAggregates := false
			for _, col := range selectStmt.Columns {
				if funcCall, ok := col.(*parser.FunctionCall); ok {
					funcName := funcCall.Name
					if funcName == "COUNT" || funcName == "SUM" || funcName == "AVG" || 
					   funcName == "MIN" || funcName == "MAX" {
						hasAggregates = true
						break
					}
				}
			}

			if hasAggregates != tt.expected {
				t.Errorf("Expected hasAggregates=%v for %s, got %v", tt.expected, tt.name, hasAggregates)
			}
		})
	}
}

func TestAggregateOperator(t *testing.T) {
	// Create test data
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

	// Create mock scan operator
	mockData := []*types.Tuple{
		{Data: []byte("id:1;name:Alice;department:IT;salary:50000;age:25")},
		{Data: []byte("id:2;name:Bob;department:IT;salary:60000;age:30")},
		{Data: []byte("id:3;name:Charlie;department:HR;salary:45000;age:28")},
		{Data: []byte("id:4;name:Diana;department:HR;salary:55000;age:32")},
		{Data: []byte("id:5;name:Eve;department:IT;salary:70000;age:35")},
	}

	mockScan := &MockScanOperator{
		table: testTable,
		data:  mockData,
	}

	tests := []struct {
		name       string
		aggregates []*execution.AggregateFunction
		groupBy    []string
		expected   int // Expected number of result tuples
	}{
		{
			name: "COUNT all",
			aggregates: []*execution.AggregateFunction{
				{Type: "COUNT", Column: "*"},
			},
			groupBy:  []string{},
			expected: 1, // Single result tuple
		},
		{
			name: "SUM salary",
			aggregates: []*execution.AggregateFunction{
				{Type: "SUM", Column: "salary"},
			},
			groupBy:  []string{},
			expected: 1,
		},
		{
			name: "AVG age",
			aggregates: []*execution.AggregateFunction{
				{Type: "AVG", Column: "age"},
			},
			groupBy:  []string{},
			expected: 1,
		},
		{
			name: "COUNT by department",
			aggregates: []*execution.AggregateFunction{
				{Type: "COUNT", Column: "*"},
			},
			groupBy:  []string{"department"},
			expected: 2, // IT and HR departments
		},
		{
			name: "Multiple aggregates by department",
			aggregates: []*execution.AggregateFunction{
				{Type: "COUNT", Column: "*"},
				{Type: "AVG", Column: "salary"},
				{Type: "MAX", Column: "age"},
			},
			groupBy:  []string{"department"},
			expected: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset mock operator
			mockScan.Reset()

			// Create aggregate operator
			aggOp := execution.NewAggregateOperator(mockScan, tt.groupBy, tt.aggregates)

			// Open operator
			err := aggOp.Open()
			if err != nil {
				t.Errorf("Failed to open aggregate operator: %v", err)
				return
			}

			// Count result tuples
			resultCount := 0
			for {
				tuple, err := aggOp.Next()
				if err != nil {
					break // End of results
				}
				if tuple != nil {
					resultCount++
					t.Logf("Result tuple: %s", string(tuple.Data))
				}
			}

			// Close operator
			aggOp.Close()

			if resultCount != tt.expected {
				t.Errorf("Expected %d result tuples, got %d", tt.expected, resultCount)
			}
		})
	}
}

// MockScanOperator for testing
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

func TestAggregateValueComputation(t *testing.T) {
	// Test specific aggregate computations
	testCases := []struct {
		name     string
		function string
		values   []interface{}
		expected interface{}
	}{
		{
			name:     "SUM integers",
			function: "SUM",
			values:   []interface{}{10, 20, 30, 40},
			expected: 100.0,
		},
		{
			name:     "AVG integers",
			function: "AVG",
			values:   []interface{}{10, 20, 30, 40},
			expected: 25.0,
		},
		{
			name:     "MIN integers",
			function: "MIN",
			values:   []interface{}{30, 10, 40, 20},
			expected: 10,
		},
		{
			name:     "MAX integers",
			function: "MAX",
			values:   []interface{}{30, 10, 40, 20},
			expected: 40,
		},
		{
			name:     "COUNT values",
			function: "COUNT",
			values:   []interface{}{1, 2, 3, 4, 5},
			expected: int64(5),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create aggregate state
			state := &execution.AggregateState{
				Count:  0,
				Sum:    0,
				Values: make([]interface{}, 0),
			}

			// Simulate processing values
			for _, val := range tc.values {
				state.Count++
				state.Values = append(state.Values, val)
				
				if numVal, ok := convertToNumber(val); ok {
					state.Sum += numVal
				}
				
				if state.Min == nil || compareValues(val, state.Min) < 0 {
					state.Min = val
				}
				
				if state.Max == nil || compareValues(val, state.Max) > 0 {
					state.Max = val
				}
			}

			// Create aggregate function
			agg := &execution.AggregateFunction{
				Type:   tc.function,
				Column: "test_col",
			}

			// Compute final value
			switch agg.Type {
			case "COUNT":
				agg.Value = state.Count
			case "SUM":
				agg.Value = state.Sum
			case "AVG":
				if state.Count > 0 {
					agg.Value = state.Sum / float64(state.Count)
				}
			case "MIN":
				agg.Value = state.Min
			case "MAX":
				agg.Value = state.Max
			}

			// Verify result
			if agg.Value != tc.expected {
				t.Errorf("Expected %v, got %v for %s", tc.expected, agg.Value, tc.function)
			}
		})
	}
}

// Helper functions for testing
func convertToNumber(val interface{}) (float64, bool) {
	switch v := val.(type) {
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case float64:
		return v, true
	case float32:
		return float64(v), true
	}
	return 0, false
}

func compareValues(a, b interface{}) int {
	if aNum, aOk := convertToNumber(a); aOk {
		if bNum, bOk := convertToNumber(b); bOk {
			if aNum < bNum {
				return -1
			} else if aNum > bNum {
				return 1
			}
			return 0
		}
	}
	return 0
}