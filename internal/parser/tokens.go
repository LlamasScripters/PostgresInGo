package parser

import "fmt"

// TokenType represents the type of a token
type TokenType int

const (
	// Special tokens
	ILLEGAL TokenType = iota
	EOF
	WHITESPACE
	COMMENT

	// Literals
	IDENT   // table_name, column_name, etc.
	INT     // 123
	FLOAT   // 123.45
	STRING  // 'hello'
	BOOLEAN // true, false

	// Keywords - DDL
	CREATE
	TABLE
	DROP
	ALTER
	DATABASE
	INDEX
	VIEW
	PRIMARY
	KEY
	FOREIGN
	REFERENCES
	UNIQUE
	CHECK
	DEFAULT
	NOT
	NULL
	AUTO_INCREMENT
	CONSTRAINT

	// Keywords - DML
	SELECT
	INSERT
	UPDATE
	DELETE
	FROM
	WHERE
	INTO
	VALUES
	SET
	ORDER
	BY
	GROUP
	HAVING
	LIMIT
	OFFSET
	DISTINCT
	AS
	UNION
	ALL

	// Keywords - Joins
	JOIN
	INNER
	LEFT
	RIGHT
	FULL
	OUTER
	ON
	USING

	// Keywords - Functions
	COUNT
	SUM
	AVG
	MIN
	MAX

	// Data types
	INT_TYPE
	VARCHAR_TYPE
	TEXT_TYPE
	BOOLEAN_TYPE
	DATE_TYPE
	TIME_TYPE
	TIMESTAMP_TYPE
	FLOAT_TYPE
	DECIMAL_TYPE
	NUMERIC_TYPE

	// Operators
	ASSIGN  // =
	EQ      // =
	NOT_EQ  // != or <>
	LT      // <
	GT      // >
	LTE     // <=
	GTE     // >=
	LIKE    // LIKE
	IN      // IN
	IS      // IS
	AND     // AND
	OR      // OR
	BETWEEN // BETWEEN

	// Arithmetic operators
	PLUS     // +
	MINUS    // -
	MULTIPLY // *
	DIVIDE   // /
	MODULO   // %

	// Punctuation
	SEMICOLON // ;
	COMMA     // ,
	DOT       // .
	LPAREN    // (
	RPAREN    // )
	LBRACKET  // [
	RBRACKET  // ]

	// Wildcards
	ASTERISK // *
)

// Token represents a single token
type Token struct {
	Type     TokenType
	Literal  string
	Position int
	Line     int
	Column   int
}

// String returns a string representation of the token
func (t Token) String() string {
	return fmt.Sprintf("Token{Type: %s, Literal: %q, Pos: %d:%d}",
		t.Type.String(), t.Literal, t.Line, t.Column)
}

// Keywords map for reserved words
var keywords = map[string]TokenType{
	// DDL Keywords
	"CREATE":         CREATE,
	"TABLE":          TABLE,
	"DROP":           DROP,
	"ALTER":          ALTER,
	"DATABASE":       DATABASE,
	"INDEX":          INDEX,
	"VIEW":           VIEW,
	"PRIMARY":        PRIMARY,
	"KEY":            KEY,
	"FOREIGN":        FOREIGN,
	"REFERENCES":     REFERENCES,
	"UNIQUE":         UNIQUE,
	"CHECK":          CHECK,
	"DEFAULT":        DEFAULT,
	"NOT":            NOT,
	"NULL":           NULL,
	"AUTO_INCREMENT": AUTO_INCREMENT,
	"CONSTRAINT":     CONSTRAINT,

	// DML Keywords
	"SELECT":   SELECT,
	"INSERT":   INSERT,
	"UPDATE":   UPDATE,
	"DELETE":   DELETE,
	"FROM":     FROM,
	"WHERE":    WHERE,
	"INTO":     INTO,
	"VALUES":   VALUES,
	"SET":      SET,
	"ORDER":    ORDER,
	"BY":       BY,
	"GROUP":    GROUP,
	"HAVING":   HAVING,
	"LIMIT":    LIMIT,
	"OFFSET":   OFFSET,
	"DISTINCT": DISTINCT,
	"AS":       AS,
	"UNION":    UNION,
	"ALL":      ALL,

	// Join Keywords
	"JOIN":  JOIN,
	"INNER": INNER,
	"LEFT":  LEFT,
	"RIGHT": RIGHT,
	"FULL":  FULL,
	"OUTER": OUTER,
	"ON":    ON,
	"USING": USING,

	// Function Keywords
	"COUNT": COUNT,
	"SUM":   SUM,
	"AVG":   AVG,
	"MIN":   MIN,
	"MAX":   MAX,

	// Data Types
	"INT":       INT_TYPE,
	"INTEGER":   INT_TYPE,
	"VARCHAR":   VARCHAR_TYPE,
	"TEXT":      TEXT_TYPE,
	"BOOLEAN":   BOOLEAN_TYPE,
	"BOOL":      BOOLEAN_TYPE,
	"DATE":      DATE_TYPE,
	"TIME":      TIME_TYPE,
	"TIMESTAMP": TIMESTAMP_TYPE,
	"FLOAT":     FLOAT_TYPE,
	"DECIMAL":   DECIMAL_TYPE,
	"NUMERIC":   NUMERIC_TYPE,

	// Logical operators
	"AND":     AND,
	"OR":      OR,
	"LIKE":    LIKE,
	"IN":      IN,
	"IS":      IS,
	"BETWEEN": BETWEEN,

	// Boolean literals
	"TRUE":  BOOLEAN,
	"FALSE": BOOLEAN,
}

// String returns the string representation of a TokenType
func (tt TokenType) String() string {
	switch tt {
	case ILLEGAL:
		return "ILLEGAL"
	case EOF:
		return "EOF"
	case WHITESPACE:
		return "WHITESPACE"
	case COMMENT:
		return "COMMENT"
	case IDENT:
		return "IDENT"
	case INT:
		return "INT"
	case FLOAT:
		return "FLOAT"
	case STRING:
		return "STRING"
	case BOOLEAN:
		return "BOOLEAN"
	case CREATE:
		return "CREATE"
	case TABLE:
		return "TABLE"
	case DROP:
		return "DROP"
	case ALTER:
		return "ALTER"
	case DATABASE:
		return "DATABASE"
	case INDEX:
		return "INDEX"
	case VIEW:
		return "VIEW"
	case PRIMARY:
		return "PRIMARY"
	case KEY:
		return "KEY"
	case FOREIGN:
		return "FOREIGN"
	case REFERENCES:
		return "REFERENCES"
	case UNIQUE:
		return "UNIQUE"
	case CHECK:
		return "CHECK"
	case DEFAULT:
		return "DEFAULT"
	case NOT:
		return "NOT"
	case NULL:
		return "NULL"
	case AUTO_INCREMENT:
		return "AUTO_INCREMENT"
	case CONSTRAINT:
		return "CONSTRAINT"
	case SELECT:
		return "SELECT"
	case INSERT:
		return "INSERT"
	case UPDATE:
		return "UPDATE"
	case DELETE:
		return "DELETE"
	case FROM:
		return "FROM"
	case WHERE:
		return "WHERE"
	case INTO:
		return "INTO"
	case VALUES:
		return "VALUES"
	case SET:
		return "SET"
	case ORDER:
		return "ORDER"
	case BY:
		return "BY"
	case GROUP:
		return "GROUP"
	case HAVING:
		return "HAVING"
	case LIMIT:
		return "LIMIT"
	case OFFSET:
		return "OFFSET"
	case DISTINCT:
		return "DISTINCT"
	case AS:
		return "AS"
	case UNION:
		return "UNION"
	case ALL:
		return "ALL"
	case JOIN:
		return "JOIN"
	case INNER:
		return "INNER"
	case LEFT:
		return "LEFT"
	case RIGHT:
		return "RIGHT"
	case FULL:
		return "FULL"
	case OUTER:
		return "OUTER"
	case ON:
		return "ON"
	case USING:
		return "USING"
	case COUNT:
		return "COUNT"
	case SUM:
		return "SUM"
	case AVG:
		return "AVG"
	case MIN:
		return "MIN"
	case MAX:
		return "MAX"
	case INT_TYPE:
		return "INT_TYPE"
	case VARCHAR_TYPE:
		return "VARCHAR_TYPE"
	case TEXT_TYPE:
		return "TEXT_TYPE"
	case BOOLEAN_TYPE:
		return "BOOLEAN_TYPE"
	case DATE_TYPE:
		return "DATE_TYPE"
	case TIME_TYPE:
		return "TIME_TYPE"
	case TIMESTAMP_TYPE:
		return "TIMESTAMP_TYPE"
	case FLOAT_TYPE:
		return "FLOAT_TYPE"
	case DECIMAL_TYPE:
		return "DECIMAL_TYPE"
	case NUMERIC_TYPE:
		return "NUMERIC_TYPE"
	case ASSIGN:
		return "ASSIGN"
	case EQ:
		return "EQ"
	case NOT_EQ:
		return "NOT_EQ"
	case LT:
		return "LT"
	case GT:
		return "GT"
	case LTE:
		return "LTE"
	case GTE:
		return "GTE"
	case LIKE:
		return "LIKE"
	case IN:
		return "IN"
	case IS:
		return "IS"
	case AND:
		return "AND"
	case OR:
		return "OR"
	case BETWEEN:
		return "BETWEEN"
	case PLUS:
		return "PLUS"
	case MINUS:
		return "MINUS"
	case MULTIPLY:
		return "MULTIPLY"
	case DIVIDE:
		return "DIVIDE"
	case MODULO:
		return "MODULO"
	case SEMICOLON:
		return "SEMICOLON"
	case COMMA:
		return "COMMA"
	case DOT:
		return "DOT"
	case LPAREN:
		return "LPAREN"
	case RPAREN:
		return "RPAREN"
	case LBRACKET:
		return "LBRACKET"
	case RBRACKET:
		return "RBRACKET"
	case ASTERISK:
		return "ASTERISK"
	default:
		return fmt.Sprintf("TokenType(%d)", int(tt))
	}
}

// LookupIdent checks if an identifier is a keyword
func LookupIdent(ident string) TokenType {
	if tok, ok := keywords[ident]; ok {
		return tok
	}
	return IDENT
}

// IsKeyword checks if a token type is a keyword
func IsKeyword(tokenType TokenType) bool {
	switch tokenType {
	case CREATE, TABLE, DROP, ALTER, DATABASE, INDEX, VIEW, PRIMARY, KEY, FOREIGN,
		REFERENCES, UNIQUE, CHECK, DEFAULT, NOT, NULL, AUTO_INCREMENT, CONSTRAINT,
		SELECT, INSERT, UPDATE, DELETE, FROM, WHERE, INTO, VALUES, SET,
		ORDER, BY, GROUP, HAVING, LIMIT, OFFSET, DISTINCT, AS, UNION, ALL,
		JOIN, INNER, LEFT, RIGHT, FULL, OUTER, ON, USING,
		COUNT, SUM, AVG, MIN, MAX,
		INT_TYPE, VARCHAR_TYPE, TEXT_TYPE, BOOLEAN_TYPE, DATE_TYPE, TIME_TYPE,
		TIMESTAMP_TYPE, FLOAT_TYPE, DECIMAL_TYPE, NUMERIC_TYPE,
		AND, OR, LIKE, IN, IS, BETWEEN:
		return true
	default:
		return false
	}
}

// IsDataType checks if a token type represents a data type
func IsDataType(tokenType TokenType) bool {
	switch tokenType {
	case INT_TYPE, VARCHAR_TYPE, TEXT_TYPE, BOOLEAN_TYPE, DATE_TYPE,
		TIME_TYPE, TIMESTAMP_TYPE, FLOAT_TYPE, DECIMAL_TYPE, NUMERIC_TYPE:
		return true
	default:
		return false
	}
}

// IsOperator checks if a token type is an operator
func IsOperator(tokenType TokenType) bool {
	switch tokenType {
	case ASSIGN, EQ, NOT_EQ, LT, GT, LTE, GTE, LIKE, IN, IS, AND, OR, BETWEEN,
		PLUS, MINUS, MULTIPLY, DIVIDE, MODULO:
		return true
	default:
		return false
	}
}

// IsComparisonOperator checks if a token type is a comparison operator
func IsComparisonOperator(tokenType TokenType) bool {
	switch tokenType {
	case EQ, ASSIGN, NOT_EQ, LT, GT, LTE, GTE, LIKE, IN, IS, BETWEEN:
		return true
	default:
		return false
	}
}
