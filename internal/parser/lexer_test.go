package parser

import (
	"testing"
)

// TestLexerBasicTokenization tests the basic tokenization functionality
func TestLexerBasicTokenization(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []TokenType
	}{
		{
			name:     "SimpleSelect",
			input:    "SELECT * FROM users;",
			expected: []TokenType{SELECT, ASTERISK, FROM, IDENT, SEMICOLON, EOF},
		},
		{
			name:     "CreateTable",
			input:    "CREATE TABLE test (id INT, name VARCHAR(50));",
			expected: []TokenType{CREATE, TABLE, IDENT, LPAREN, IDENT, INT_TYPE, COMMA, IDENT, VARCHAR_TYPE, LPAREN, INT, RPAREN, RPAREN, SEMICOLON, EOF},
		},
		{
			name:     "InsertStatement",
			input:    "INSERT INTO users VALUES (1, 'Alice', true);",
			expected: []TokenType{INSERT, INTO, IDENT, VALUES, LPAREN, INT, COMMA, STRING, COMMA, BOOLEAN, RPAREN, SEMICOLON, EOF},
		},
		{
			name:     "UpdateStatement",
			input:    "UPDATE users SET name = 'Bob' WHERE id = 1;",
			expected: []TokenType{UPDATE, IDENT, SET, IDENT, ASSIGN, STRING, WHERE, IDENT, ASSIGN, INT, SEMICOLON, EOF},
		},
		{
			name:     "DeleteStatement",
			input:    "DELETE FROM users WHERE id = 1;",
			expected: []TokenType{DELETE, FROM, IDENT, WHERE, IDENT, ASSIGN, INT, SEMICOLON, EOF},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lexer := NewLexer(test.input)
			tokens := lexer.GetAllTokens()

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
}

// TestLexerStringLiterals tests string literal tokenization
func TestLexerStringLiterals(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "SingleQuotes",
			input:    "'hello world'",
			expected: "hello world",
		},
		{
			name:     "DoubleQuotes",
			input:    "\"quoted string\"",
			expected: "quoted string",
		},
		{
			name:     "WithSpaces",
			input:    "'string with spaces'",
			expected: "string with spaces",
		},
		{
			name:     "Empty",
			input:    "''",
			expected: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lexer := NewAdvancedLexer(test.input)
			token := lexer.Advance()

			if token.Type != STRING {
				t.Errorf("Expected STRING token, got %s", token.Type.String())
			}

			if token.Literal != test.expected {
				t.Errorf("Expected '%s', got '%s'", test.expected, token.Literal)
			}
		})
	}
}

// TestLexerComments tests comment handling
func TestLexerComments(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "SingleLineComment",
			input: "-- This is a comment\nSELECT * FROM users;",
		},
		{
			name:  "BlockComment",
			input: "SELECT * FROM users; /* Another comment */",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lexer := NewAdvancedLexer(test.input)
			tokens := lexer.GetAllTokens()

			// Should not include comments in output
			hasComment := false
			for _, token := range tokens {
				if token.Type == COMMENT {
					hasComment = true
					break
				}
			}

			if hasComment {
				t.Error("Comments should be filtered out")
			}
		})
	}
}

// TestLexerNumbers tests number tokenization
func TestLexerNumbers(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected TokenType
		literal  string
	}{
		{
			name:     "Integer",
			input:    "123",
			expected: INT,
			literal:  "123",
		},
		{
			name:     "Float",
			input:    "123.45",
			expected: FLOAT,
			literal:  "123.45",
		},
		{
			name:     "ScientificNotation",
			input:    "1.23e10",
			expected: FLOAT,
			literal:  "1.23e10",
		},
		{
			name:     "NegativeScientific",
			input:    "1.23e-10",
			expected: FLOAT,
			literal:  "1.23e-10",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lexer := NewLexer(test.input)
			token := lexer.NextToken()

			if token.Type != test.expected {
				t.Errorf("Expected %s token, got %s", test.expected.String(), token.Type.String())
			}

			if token.Literal != test.literal {
				t.Errorf("Expected literal '%s', got '%s'", test.literal, token.Literal)
			}
		})
	}
}

// TestLexerOperators tests operator tokenization
func TestLexerOperators(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected TokenType
	}{
		{name: "Equals", input: "=", expected: ASSIGN},
		{name: "NotEquals", input: "!=", expected: NOT_EQ},
		{name: "LessThan", input: "<", expected: LT},
		{name: "GreaterThan", input: ">", expected: GT},
		{name: "LessEquals", input: "<=", expected: LTE},
		{name: "GreaterEquals", input: ">=", expected: GTE},
		{name: "Plus", input: "+", expected: PLUS},
		{name: "Minus", input: "-", expected: MINUS},
		{name: "Multiply", input: "*", expected: ASTERISK},
		{name: "Divide", input: "/", expected: DIVIDE},
		{name: "Modulo", input: "%", expected: MODULO},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lexer := NewLexer(test.input)
			token := lexer.NextToken()

			if token.Type != test.expected {
				t.Errorf("Expected %s token, got %s", test.expected.String(), token.Type.String())
			}
		})
	}
}

// TestLexerKeywords tests keyword recognition
func TestLexerKeywords(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected TokenType
	}{
		{name: "SELECT", input: "SELECT", expected: SELECT},
		{name: "INSERT", input: "INSERT", expected: INSERT},
		{name: "UPDATE", input: "UPDATE", expected: UPDATE},
		{name: "DELETE", input: "DELETE", expected: DELETE},
		{name: "CREATE", input: "CREATE", expected: CREATE},
		{name: "TABLE", input: "TABLE", expected: TABLE},
		{name: "FROM", input: "FROM", expected: FROM},
		{name: "WHERE", input: "WHERE", expected: WHERE},
		{name: "AND", input: "AND", expected: AND},
		{name: "OR", input: "OR", expected: OR},
		{name: "NOT", input: "NOT", expected: NOT},
		{name: "NULL", input: "NULL", expected: NULL},
		{name: "TRUE", input: "TRUE", expected: BOOLEAN},
		{name: "FALSE", input: "FALSE", expected: BOOLEAN},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lexer := NewLexer(test.input)
			token := lexer.NextToken()

			if token.Type != test.expected {
				t.Errorf("Expected %s token, got %s", test.expected.String(), token.Type.String())
			}
		})
	}
}

// TestLexerCaseInsensitive tests case insensitive keyword recognition
func TestLexerCaseInsensitive(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected TokenType
	}{
		{name: "lowercase", input: "select", expected: SELECT},
		{name: "uppercase", input: "SELECT", expected: SELECT},
		{name: "mixed", input: "Select", expected: SELECT},
		{name: "weird", input: "sElEcT", expected: SELECT},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lexer := NewLexer(test.input)
			token := lexer.NextToken()

			if token.Type != test.expected {
				t.Errorf("Expected %s token, got %s", test.expected.String(), token.Type.String())
			}
		})
	}
}

// TestLexerPunctuation tests punctuation tokenization
func TestLexerPunctuation(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected TokenType
	}{
		{name: "Semicolon", input: ";", expected: SEMICOLON},
		{name: "Comma", input: ",", expected: COMMA},
		{name: "Dot", input: ".", expected: DOT},
		{name: "LeftParen", input: "(", expected: LPAREN},
		{name: "RightParen", input: ")", expected: RPAREN},
		{name: "LeftBracket", input: "[", expected: LBRACKET},
		{name: "RightBracket", input: "]", expected: RBRACKET},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lexer := NewLexer(test.input)
			token := lexer.NextToken()

			if token.Type != test.expected {
				t.Errorf("Expected %s token, got %s", test.expected.String(), token.Type.String())
			}
		})
	}
}

// TestAdvancedLexerFunctionality tests advanced lexer features
func TestAdvancedLexerFunctionality(t *testing.T) {
	t.Run("PeekFunctionality", func(t *testing.T) {
		lexer := NewAdvancedLexer("SELECT * FROM users")
		
		// Test Current
		current := lexer.Current()
		if current.Type != SELECT {
			t.Errorf("Expected SELECT, got %s", current.Type.String())
		}
		
		// Test Peek
		peek := lexer.Peek()
		if peek.Type != ASTERISK {
			t.Errorf("Expected ASTERISK, got %s", peek.Type.String())
		}
		
		// Current should not have changed
		current = lexer.Current()
		if current.Type != SELECT {
			t.Errorf("Current should still be SELECT, got %s", current.Type.String())
		}
	})
	
	t.Run("AdvanceFunctionality", func(t *testing.T) {
		lexer := NewAdvancedLexer("SELECT * FROM")
		
		// Advance and check
		token := lexer.Advance()
		if token.Type != SELECT {
			t.Errorf("Expected SELECT, got %s", token.Type.String())
		}
		
		// Current should now be ASTERISK
		current := lexer.Current()
		if current.Type != ASTERISK {
			t.Errorf("Expected ASTERISK, got %s", current.Type.String())
		}
	})
	
	t.Run("ConsumeFunction", func(t *testing.T) {
		lexer := NewAdvancedLexer("SELECT * FROM")
		
		// Consume SELECT
		success := lexer.Consume(SELECT)
		if !success {
			t.Error("Should have consumed SELECT")
		}
		
		// Current should now be ASTERISK
		current := lexer.Current()
		if current.Type != ASTERISK {
			t.Errorf("Expected ASTERISK, got %s", current.Type.String())
		}
		
		// Try to consume wrong token
		success = lexer.Consume(FROM)
		if success {
			t.Error("Should not have consumed FROM")
		}
	})
}

// TestLexerPositionTracking tests position tracking functionality
func TestLexerPositionTracking(t *testing.T) {
	input := "SELECT *\nFROM users"
	lexer := NewLexer(input)
	
	// First token should be at line 1
	token := lexer.NextToken()
	if token.Line != 1 {
		t.Errorf("Expected line 1, got %d", token.Line)
	}
	
	// Skip to FROM token (after newline)
	lexer.NextToken() // *
	token = lexer.NextToken() // FROM
	if token.Line != 2 {
		t.Errorf("Expected line 2, got %d", token.Line)
	}
}