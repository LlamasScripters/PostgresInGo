package parser

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// Lexer represents the lexical analyzer
type Lexer struct {
	input        string
	position     int  // current position in input (points to current char)
	readPosition int  // current reading position in input (after current char)
	ch           byte // current char under examination
	line         int  // current line number
	column       int  // current column number
}

// NewLexer creates a new lexer instance
func NewLexer(input string) *Lexer {
	l := &Lexer{
		input:  input,
		line:   1,
		column: 0,
	}
	l.readChar()
	return l
}

// readChar gives us the next character and advances our position in the input string
func (l *Lexer) readChar() {
	if l.readPosition >= len(l.input) {
		l.ch = 0 // NUL character represents "EOF"
	} else {
		l.ch = l.input[l.readPosition]
	}
	l.position = l.readPosition
	l.readPosition++
	
	if l.ch == '\n' {
		l.line++
		l.column = 0
	} else {
		l.column++
	}
}

// peekChar returns the next character without advancing the position
func (l *Lexer) peekChar() byte {
	if l.readPosition >= len(l.input) {
		return 0
	}
	return l.input[l.readPosition]
}

// NextToken scans the input and returns the next token
func (l *Lexer) NextToken() Token {
	var tok Token

	l.skipWhitespace()

	switch l.ch {
	case '=':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok = Token{Type: EQ, Literal: string(ch) + string(l.ch), Position: l.position, Line: l.line, Column: l.column}
		} else {
			tok = newToken(ASSIGN, l.ch, l.position, l.line, l.column)
		}
	case '!':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok = Token{Type: NOT_EQ, Literal: string(ch) + string(l.ch), Position: l.position, Line: l.line, Column: l.column}
		} else {
			tok = newToken(ILLEGAL, l.ch, l.position, l.line, l.column)
		}
	case '<':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok = Token{Type: LTE, Literal: string(ch) + string(l.ch), Position: l.position, Line: l.line, Column: l.column}
		} else if l.peekChar() == '>' {
			ch := l.ch
			l.readChar()
			tok = Token{Type: NOT_EQ, Literal: string(ch) + string(l.ch), Position: l.position, Line: l.line, Column: l.column}
		} else {
			tok = newToken(LT, l.ch, l.position, l.line, l.column)
		}
	case '>':
		if l.peekChar() == '=' {
			ch := l.ch
			l.readChar()
			tok = Token{Type: GTE, Literal: string(ch) + string(l.ch), Position: l.position, Line: l.line, Column: l.column}
		} else {
			tok = newToken(GT, l.ch, l.position, l.line, l.column)
		}
	case '+':
		tok = newToken(PLUS, l.ch, l.position, l.line, l.column)
	case '-':
		// Handle SQL comments (-- comment)
		if l.peekChar() == '-' {
			return l.readComment()
		}
		tok = newToken(MINUS, l.ch, l.position, l.line, l.column)
	case '*':
		tok = newToken(ASTERISK, l.ch, l.position, l.line, l.column)
	case '/':
		// Handle SQL block comments (/* comment */)
		if l.peekChar() == '*' {
			return l.readBlockComment()
		}
		tok = newToken(DIVIDE, l.ch, l.position, l.line, l.column)
	case '%':
		tok = newToken(MODULO, l.ch, l.position, l.line, l.column)
	case ';':
		tok = newToken(SEMICOLON, l.ch, l.position, l.line, l.column)
	case ',':
		tok = newToken(COMMA, l.ch, l.position, l.line, l.column)
	case '.':
		tok = newToken(DOT, l.ch, l.position, l.line, l.column)
	case '(':
		tok = newToken(LPAREN, l.ch, l.position, l.line, l.column)
	case ')':
		tok = newToken(RPAREN, l.ch, l.position, l.line, l.column)
	case '[':
		tok = newToken(LBRACKET, l.ch, l.position, l.line, l.column)
	case ']':
		tok = newToken(RBRACKET, l.ch, l.position, l.line, l.column)
	case '\'':
		tok.Type = STRING
		tok.Literal = l.readString('\'')
		tok.Position = l.position
		tok.Line = l.line
		tok.Column = l.column
	case '"':
		tok.Type = STRING
		tok.Literal = l.readString('"')
		tok.Position = l.position
		tok.Line = l.line
		tok.Column = l.column
	case '`':
		tok.Type = IDENT
		tok.Literal = l.readString('`')
		tok.Position = l.position
		tok.Line = l.line
		tok.Column = l.column
	case 0:
		tok.Literal = ""
		tok.Type = EOF
		tok.Position = l.position
		tok.Line = l.line
		tok.Column = l.column
	default:
		if isLetter(l.ch) {
			tok.Position = l.position
			tok.Line = l.line
			tok.Column = l.column
			tok.Literal = l.readIdentifier()
			tok.Type = LookupIdent(strings.ToUpper(tok.Literal))
			return tok
		} else if isDigit(l.ch) {
			tok.Position = l.position
			tok.Line = l.line
			tok.Column = l.column
			tok.Type, tok.Literal = l.readNumber()
			return tok
		} else {
			tok = newToken(ILLEGAL, l.ch, l.position, l.line, l.column)
		}
	}

	l.readChar()
	return tok
}

// newToken creates a new token
func newToken(tokenType TokenType, ch byte, position, line, column int) Token {
	return Token{
		Type:     tokenType,
		Literal:  string(ch),
		Position: position,
		Line:     line,
		Column:   column,
	}
}

// readIdentifier reads an identifier (table name, column name, keyword, etc.)
func (l *Lexer) readIdentifier() string {
	position := l.position
	for isLetter(l.ch) || isDigit(l.ch) || l.ch == '_' {
		l.readChar()
	}
	return l.input[position:l.position]
}

// readNumber reads a number (integer or float)
func (l *Lexer) readNumber() (TokenType, string) {
	position := l.position
	tokenType := INT

	for isDigit(l.ch) {
		l.readChar()
	}

	// Check for decimal point
	if l.ch == '.' && isDigit(l.peekChar()) {
		tokenType = FLOAT
		l.readChar()
		for isDigit(l.ch) {
			l.readChar()
		}
	}

	// Check for scientific notation
	if l.ch == 'e' || l.ch == 'E' {
		tokenType = FLOAT
		l.readChar()
		if l.ch == '+' || l.ch == '-' {
			l.readChar()
		}
		for isDigit(l.ch) {
			l.readChar()
		}
	}

	return tokenType, l.input[position:l.position]
}

// readString reads a string literal enclosed in quotes
func (l *Lexer) readString(delimiter byte) string {
	position := l.position + 1
	for {
		l.readChar()
		if l.ch == delimiter || l.ch == 0 {
			break
		}
		// Handle escaped characters
		if l.ch == '\\' {
			l.readChar()
		}
	}
	return l.input[position:l.position]
}

// readComment reads a single-line comment (-- comment)
func (l *Lexer) readComment() Token {
	position := l.position
	line := l.line
	column := l.column

	for l.ch != '\n' && l.ch != 0 {
		l.readChar()
	}

	return Token{
		Type:     COMMENT,
		Literal:  l.input[position:l.position],
		Position: position,
		Line:     line,
		Column:   column,
	}
}

// readBlockComment reads a block comment (/* comment */)
func (l *Lexer) readBlockComment() Token {
	position := l.position
	line := l.line
	column := l.column

	l.readChar() // skip '/'
	l.readChar() // skip '*'

	for {
		if l.ch == 0 {
			break
		}
		if l.ch == '*' && l.peekChar() == '/' {
			l.readChar() // skip '*'
			l.readChar() // skip '/'
			break
		}
		l.readChar()
	}

	return Token{
		Type:     COMMENT,
		Literal:  l.input[position:l.position],
		Position: position,
		Line:     line,
		Column:   column,
	}
}

// skipWhitespace skips whitespace characters
func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

// isLetter checks if the character is a letter
func isLetter(ch byte) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_'
}

// isDigit checks if the character is a digit
func isDigit(ch byte) bool {
	return '0' <= ch && ch <= '9'
}

// GetAllTokens returns all tokens from the input
func (l *Lexer) GetAllTokens() []Token {
	var tokens []Token
	
	for {
		tok := l.NextToken()
		if tok.Type == EOF {
			tokens = append(tokens, tok)
			break
		}
		// Skip whitespace and comments for most use cases
		if tok.Type != WHITESPACE && tok.Type != COMMENT {
			tokens = append(tokens, tok)
		}
	}
	
	return tokens
}

// Reset resets the lexer to the beginning of the input
func (l *Lexer) Reset() {
	l.position = 0
	l.readPosition = 0
	l.line = 1
	l.column = 0
	l.readChar()
}

// CurrentPosition returns the current position information
func (l *Lexer) CurrentPosition() (line, column int) {
	return l.line, l.column
}

// AdvancedLexer provides additional functionality for complex SQL parsing
type AdvancedLexer struct {
	*Lexer
	tokens    []Token
	current   int
	savePoint int
}

// NewAdvancedLexer creates a new advanced lexer
func NewAdvancedLexer(input string) *AdvancedLexer {
	lexer := NewLexer(input)
	tokens := lexer.GetAllTokens()
	
	return &AdvancedLexer{
		Lexer:   lexer,
		tokens:  tokens,
		current: 0,
	}
}

// Current returns the current token
func (al *AdvancedLexer) Current() Token {
	if al.current >= len(al.tokens) {
		return Token{Type: EOF}
	}
	return al.tokens[al.current]
}

// Peek returns the next token without advancing
func (al *AdvancedLexer) Peek() Token {
	if al.current+1 >= len(al.tokens) {
		return Token{Type: EOF}
	}
	return al.tokens[al.current+1]
}

// PeekN returns the token n positions ahead
func (al *AdvancedLexer) PeekN(n int) Token {
	pos := al.current + n
	if pos >= len(al.tokens) {
		return Token{Type: EOF}
	}
	return al.tokens[pos]
}

// Advance moves to the next token
func (al *AdvancedLexer) Advance() Token {
	tok := al.Current()
	if al.current < len(al.tokens)-1 {
		al.current++
	}
	return tok
}

// Consume advances if the current token matches the expected type
func (al *AdvancedLexer) Consume(expected TokenType) bool {
	if al.Current().Type == expected {
		al.Advance()
		return true
	}
	return false
}

// SavePosition saves the current position for backtracking
func (al *AdvancedLexer) SavePosition() {
	al.savePoint = al.current
}

// RestorePosition restores to the saved position
func (al *AdvancedLexer) RestorePosition() {
	al.current = al.savePoint
}

// IsAtEnd checks if we're at the end of tokens
func (al *AdvancedLexer) IsAtEnd() bool {
	return al.current >= len(al.tokens) || al.Current().Type == EOF
}

// SkipTo advances to the next occurrence of the specified token type
func (al *AdvancedLexer) SkipTo(tokenType TokenType) bool {
	for !al.IsAtEnd() {
		if al.Current().Type == tokenType {
			return true
		}
		al.Advance()
	}
	return false
}

// ExpectSequence checks if the next tokens match the expected sequence
func (al *AdvancedLexer) ExpectSequence(expected ...TokenType) bool {
	al.SavePosition()
	
	for i, expectedType := range expected {
		token := al.PeekN(i)
		if token.Type != expectedType {
			al.RestorePosition()
			return false
		}
	}
	
	return true
}

// TokensRemaining returns the number of tokens remaining
func (al *AdvancedLexer) TokensRemaining() int {
	return len(al.tokens) - al.current
}

// GetTokensInRange returns tokens from current position to end or count tokens
func (al *AdvancedLexer) GetTokensInRange(count int) []Token {
	end := al.current + count
	if end > len(al.tokens) {
		end = len(al.tokens)
	}
	return al.tokens[al.current:end]
}

// ValidateSQL performs basic SQL syntax validation
func (al *AdvancedLexer) ValidateSQL() error {
	// Check for balanced parentheses
	parenCount := 0
	bracketCount := 0
	
	for _, token := range al.tokens {
		switch token.Type {
		case LPAREN:
			parenCount++
		case RPAREN:
			parenCount--
			if parenCount < 0 {
				return NewSyntaxError("Unmatched closing parenthesis", token.Line, token.Column)
			}
		case LBRACKET:
			bracketCount++
		case RBRACKET:
			bracketCount--
			if bracketCount < 0 {
				return NewSyntaxError("Unmatched closing bracket", token.Line, token.Column)
			}
		}
	}
	
	if parenCount != 0 {
		return NewSyntaxError("Unmatched parentheses", 0, 0)
	}
	
	if bracketCount != 0 {
		return NewSyntaxError("Unmatched brackets", 0, 0)
	}
	
	return nil
}

// SyntaxError represents a syntax error in SQL
type SyntaxError struct {
	Message string
	Line    int
	Column  int
}

// NewSyntaxError creates a new syntax error
func NewSyntaxError(message string, line, column int) *SyntaxError {
	return &SyntaxError{
		Message: message,
		Line:    line,
		Column:  column,
	}
}

// Error implements the error interface
func (e *SyntaxError) Error() string {
	if e.Line > 0 && e.Column > 0 {
		return fmt.Sprintf("Syntax error at line %d, column %d: %s", e.Line, e.Column, e.Message)
	}
	return fmt.Sprintf("Syntax error: %s", e.Message)
}

// IsUTF8 checks if the input contains valid UTF-8
func IsUTF8(s string) bool {
	return utf8.ValidString(s)
}

// NormalizeSQL normalizes SQL string (remove extra whitespace, etc.)
func NormalizeSQL(sql string) string {
	// Remove extra whitespace and normalize case for keywords
	lines := strings.Split(sql, "\n")
	var normalizedLines []string
	
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "--") {
			normalizedLines = append(normalizedLines, line)
		}
	}
	
	return strings.Join(normalizedLines, " ")
}

// HasKeyword checks if the SQL contains a specific keyword
func HasKeyword(sql string, keyword TokenType) bool {
	lexer := NewAdvancedLexer(sql)
	
	for !lexer.IsAtEnd() {
		if lexer.Current().Type == keyword {
			return true
		}
		lexer.Advance()
	}
	
	return false
}

// ExtractTableNames extracts all table names from SQL (basic implementation)
func ExtractTableNames(sql string) []string {
	lexer := NewAdvancedLexer(sql)
	var tableNames []string
	
	for !lexer.IsAtEnd() {
		token := lexer.Current()
		
		// Look for patterns like "FROM table_name" or "JOIN table_name"
		if token.Type == FROM || token.Type == JOIN {
			lexer.Advance()
			if !lexer.IsAtEnd() && lexer.Current().Type == IDENT {
				tableNames = append(tableNames, lexer.Current().Literal)
			}
		}
		
		lexer.Advance()
	}
	
	return tableNames
}