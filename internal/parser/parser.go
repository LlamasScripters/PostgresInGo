package parser

import (
	"fmt"
	"strconv"
	"strings"
)

// Parser represents the SQL parser
type Parser struct {
	lexer   *AdvancedLexer
	current Token
}

type ParseError struct {
	Line    int
	Column  int
	Message string
}

func (e *ParseError) String() string {
	return fmt.Sprintf("parse error at %d:%d: %s", e.Line, e.Column, e.Message)
}

// NewParser creates a new SQL parser
func NewParser(input string) *Parser {
	lexer := NewAdvancedLexer(input)
	p := &Parser{
		lexer: lexer,
	}
	p.nextToken() // Initialize current token
	return p
}

// Parse parses the SQL input and returns an AST
func (p *Parser) Parse() (*SQLStatement, error) {
	stmt := &SQLStatement{
		Statements: []Statement{},
	}

	for !p.isAtEnd() {
		if p.current.Type == SEMICOLON {
			p.nextToken()
			continue
		}

		s, err := p.parseStatement()
		if err != nil {
			return nil, err
		}
		if s != nil {
			stmt.Statements = append(stmt.Statements, s)
		}

		// Skip semicolon if present
		if p.current.Type == SEMICOLON {
			p.nextToken()
		}
	}

	return stmt, nil
}

// nextToken advances to the next token
func (p *Parser) nextToken() {
	p.current = p.lexer.Advance()
}

// peekToken returns the next token without advancing
func (p *Parser) peekToken() Token {
	return p.lexer.Peek()
}

// isAtEnd checks if we're at the end of input
func (p *Parser) isAtEnd() bool {
	return p.current.Type == EOF
}

// expectToken consumes a token of the expected type or reports an error
func (p *Parser) expectToken(expected TokenType) (bool, error) {
	if p.current.Type == expected {
		p.nextToken()
		return true, nil
	}
	return false, fmt.Errorf("expected %s, got %s at line %d, column %d",
		expected.String(), p.current.Type.String(), p.current.Line, p.current.Column)
}

// parseStatement parses a top-level SQL statement
func (p *Parser) parseStatement() (Statement, error) {
	switch p.current.Type {
	case CREATE:
		return p.parseCreateStatement()
	case DROP:
		return p.parseDropStatement()
	case SELECT:
		return p.parseSelectStatement()
	case INSERT:
		return p.parseInsertStatement()
	case UPDATE:
		return p.parseUpdateStatement()
	case DELETE:
		return p.parseDeleteStatement()
	default:
		err := fmt.Errorf("unexpected token %s at line %d, column %d",
			p.current.Type.String(), p.current.Line, p.current.Column)
		p.nextToken() // Skip invalid token
		return nil, err
	}
}

// ==================== CREATE Statements ====================

func (p *Parser) parseCreateStatement() (Statement, error) {
	p.nextToken() // consume CREATE

	switch p.current.Type {
	case DATABASE:
		return p.parseCreateDatabase()
	case TABLE:
		return p.parseCreateTable()
	case VIEW:
		return p.parseCreateView()
	case UNIQUE:
		p.nextToken() // consume UNIQUE
		if p.current.Type == INDEX {
			return p.parseCreateIndex(true)
		}
		return nil, fmt.Errorf("expected INDEX after UNIQUE")
	case INDEX:
		return p.parseCreateIndex(false)
	default:
		return nil, fmt.Errorf("unexpected token after CREATE: %s", p.current.Type.String())
	}
}

func (p *Parser) parseCreateDatabase() (*CreateDatabaseStatement, error) {
	p.nextToken() // consume DATABASE

	if p.current.Type != IDENT {
		return nil, fmt.Errorf("expected database name")
	}

	name := p.current.Literal
	p.nextToken()

	return &CreateDatabaseStatement{Name: name}, nil
}

func (p *Parser) parseCreateTable() (*CreateTableStatement, error) {
	p.nextToken() // consume TABLE

	if p.current.Type != IDENT {
		return nil, fmt.Errorf("expected table name")
	}

	name := p.current.Literal
	p.nextToken()

	if ok, err := p.expectToken(LPAREN); !ok {
		return nil, err
	}

	var columns []*ColumnDefinition
	var constraints []*ConstraintDefinition

	for p.current.Type != RPAREN && !p.isAtEnd() {
		if p.current.Type == CONSTRAINT || p.current.Type == PRIMARY ||
			p.current.Type == FOREIGN || p.current.Type == UNIQUE || p.current.Type == CHECK {
			constraint, err := p.parseConstraintDefinition()
			if err != nil {
				return nil, err
			}
			if constraint != nil {
				constraints = append(constraints, constraint)
			}
		} else {
			column, err := p.parseColumnDefinition()
			if err != nil {
				return nil, err
			}
			if column != nil {
				columns = append(columns, column)
			}
		}

		if p.current.Type == COMMA {
			p.nextToken()
		} else if p.current.Type != RPAREN {
			return nil, fmt.Errorf("expected ',' or ')' in table definition")
		}
	}

	if ok, err := p.expectToken(RPAREN); !ok {
		return nil, err
	}

	return &CreateTableStatement{
		Name:        name,
		Columns:     columns,
		Constraints: constraints,
	}, nil
}

func (p *Parser) parseCreateIndex(unique bool) (*CreateIndexStatement, error) {
	p.nextToken() // consume INDEX

	if p.current.Type != IDENT {
		return nil, fmt.Errorf("expected index name")
	}

	name := p.current.Literal
	p.nextToken()

	if ok, err := p.expectToken(ON); !ok {
		return nil, err
	}

	if p.current.Type != IDENT {
		return nil, fmt.Errorf("expected table name")
	}

	table := p.current.Literal
	p.nextToken()

	if ok, err := p.expectToken(LPAREN); !ok {
		return nil, err
	}

	var columns []string
	for p.current.Type != RPAREN && !p.isAtEnd() {
		if p.current.Type != IDENT {
			return nil, fmt.Errorf("expected column name")
		}

		columns = append(columns, p.current.Literal)
		p.nextToken()

		if p.current.Type == COMMA {
			p.nextToken()
		} else if p.current.Type != RPAREN {
			return nil, fmt.Errorf("expected ',' or ')' in index column list")
		}
	}

	if ok, err := p.expectToken(RPAREN); !ok {
		return nil, err
	}

	return &CreateIndexStatement{
		Name:    name,
		Table:   table,
		Columns: columns,
		Unique:  unique,
	}, nil
}

func (p *Parser) parseCreateView() (*CreateViewStatement, error) {
	p.nextToken() // consume VIEW

	if p.current.Type != IDENT {
		return nil, fmt.Errorf("expected view name")
	}

	viewName := p.current.Literal
	p.nextToken()

	var columns []string

	// Check for optional column list
	if p.current.Type == LPAREN {
		p.nextToken() // consume (

		for {
			if p.current.Type != IDENT {
				return nil, fmt.Errorf("expected column name")
			}

			columns = append(columns, p.current.Literal)
			p.nextToken()

			if p.current.Type == RPAREN {
				p.nextToken() // consume )
				break
			}

			if p.current.Type == COMMA {
				p.nextToken() // consume ,
			} else {
				return nil, fmt.Errorf("expected ',' or ')' in column list")
			}
		}
	}

	// Expect AS keyword
	if p.current.Type != AS {
		return nil, fmt.Errorf("expected AS after view name")
	}
	p.nextToken() // consume AS

	// Parse the SELECT statement
	if p.current.Type != SELECT {
		return nil, fmt.Errorf("expected SELECT statement after AS")
	}

	selectStmt, err := p.parseSelectStatement()
	if err != nil {
		return nil, err
	}

	// Extract the raw SQL definition
	definition := selectStmt.String()

	return &CreateViewStatement{
		Name:       viewName,
		Columns:    columns,
		Query:      selectStmt,
		Definition: definition,
	}, nil
}

// ==================== DROP Statements ====================

func (p *Parser) parseDropStatement() (Statement, error) {
	p.nextToken() // consume DROP

	switch p.current.Type {
	case DATABASE:
		return p.parseDropDatabase()
	case TABLE:
		return p.parseDropTable()
	case INDEX:
		return p.parseDropIndex()
	case VIEW:
		return p.parseDropView()
	default:
		return nil, fmt.Errorf("unexpected token after DROP: %s", p.current.Type.String())
	}
}

func (p *Parser) parseDropDatabase() (*DropDatabaseStatement, error) {
	p.nextToken() // consume DATABASE

	if p.current.Type != IDENT {
		return nil, fmt.Errorf("expected database name")
	}

	name := p.current.Literal
	p.nextToken()

	return &DropDatabaseStatement{Name: name}, nil
}

func (p *Parser) parseDropTable() (*DropTableStatement, error) {
	p.nextToken() // consume TABLE

	if p.current.Type != IDENT {
		return nil, fmt.Errorf("expected table name")
	}

	name := p.current.Literal
	p.nextToken()

	return &DropTableStatement{Name: name}, nil
}

func (p *Parser) parseDropIndex() (*DropIndexStatement, error) {
	p.nextToken() // consume INDEX

	if p.current.Type != IDENT {
		return nil, fmt.Errorf("expected index name")
	}

	name := p.current.Literal
	p.nextToken()

	return &DropIndexStatement{Name: name}, nil
}

func (p *Parser) parseDropView() (*DropViewStatement, error) {
	p.nextToken() // consume VIEW

	if p.current.Type != IDENT {
		return nil, fmt.Errorf("expected view name")
	}

	name := p.current.Literal
	p.nextToken()

	return &DropViewStatement{Name: name}, nil
}

// ==================== DML Statements ====================

func (p *Parser) parseSelectStatement() (*SelectStatement, error) {
	p.nextToken() // consume SELECT

	stmt := &SelectStatement{}

	// Check for DISTINCT
	if p.current.Type == DISTINCT {
		stmt.Distinct = true
		p.nextToken()
	}

	// Parse column list
	columns, err := p.parseSelectList()
	if err != nil {
		return nil, err
	}
	stmt.Columns = columns

	// Parse FROM clause
	if p.current.Type == FROM {
		from, err := p.parseFromClause()
		if err != nil {
			return nil, err
		}
		stmt.From = from
	}

	// Parse JOINs
	for p.isJoinKeyword(p.current.Type) {
		join, err := p.parseJoinClause()
		if err != nil {
			return nil, err
		}
		if join != nil {
			stmt.Joins = append(stmt.Joins, join)
		}
	}

	// Parse WHERE clause
	if p.current.Type == WHERE {
		p.nextToken()
		where, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	// Parse GROUP BY clause
	if p.current.Type == GROUP {
		p.nextToken()
		if ok, err := p.expectToken(BY); !ok {
			return nil, err
		}
		groupBy, err := p.parseExpressionList()
		if err != nil {
			return nil, err
		}
		stmt.GroupBy = groupBy
	}

	// Parse HAVING clause
	if p.current.Type == HAVING {
		p.nextToken()
		having, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Having = having
	}

	// Parse ORDER BY clause
	if p.current.Type == ORDER {
		p.nextToken()
		if ok, err := p.expectToken(BY); !ok {
			return nil, err
		}
		orderBy, err := p.parseOrderByClause()
		if err != nil {
			return nil, err
		}
		stmt.OrderBy = orderBy
	}

	// Parse LIMIT clause
	if p.current.Type == LIMIT {
		p.nextToken()
		limit, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Limit = limit
	}

	// Parse OFFSET clause
	if p.current.Type == OFFSET {
		p.nextToken()
		offset, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Offset = offset
	}

	return stmt, nil
}

func (p *Parser) parseInsertStatement() (*InsertStatement, error) {
	p.nextToken() // consume INSERT

	if ok, err := p.expectToken(INTO); !ok {
		return nil, err
	}

	if p.current.Type != IDENT {
		return nil, fmt.Errorf("expected table name")
	}

	stmt := &InsertStatement{
		Table: p.current.Literal,
	}
	p.nextToken()

	// Optional column list
	if p.current.Type == LPAREN {
		p.nextToken()

		for p.current.Type != RPAREN && !p.isAtEnd() {
			if p.current.Type != IDENT {
				return nil, fmt.Errorf("expected column name")
			}

			stmt.Columns = append(stmt.Columns, p.current.Literal)
			p.nextToken()

			if p.current.Type == COMMA {
				p.nextToken()
			} else if p.current.Type != RPAREN {
				return nil, fmt.Errorf("expected ',' or ')' in column list")
			}
		}

		if ok, err := p.expectToken(RPAREN); !ok {
			return nil, err
		}
	}

	if ok, err := p.expectToken(VALUES); !ok {
		return nil, err
	}

	// Parse value lists
	for {
		if ok, err := p.expectToken(LPAREN); !ok {
			return nil, err
		}

		var values []Expression
		for p.current.Type != RPAREN && !p.isAtEnd() {
			expr, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			if expr != nil {
				values = append(values, expr)
			}

			if p.current.Type == COMMA {
				p.nextToken()
			} else if p.current.Type != RPAREN {
				return nil, fmt.Errorf("expected ',' or ')' in value list")
			}
		}

		if ok, err := p.expectToken(RPAREN); !ok {
			return nil, err
		}

		stmt.Values = append(stmt.Values, values)

		if p.current.Type == COMMA {
			p.nextToken()
		} else {
			break
		}
	}

	return stmt, nil
}

func (p *Parser) parseUpdateStatement() (*UpdateStatement, error) {
	p.nextToken() // consume UPDATE

	if p.current.Type != IDENT {
		return nil, fmt.Errorf("expected table name")
	}

	stmt := &UpdateStatement{
		Table: p.current.Literal,
	}
	p.nextToken()

	if ok, err := p.expectToken(SET); !ok {
		return nil, err
	}

	// Parse assignments
	for {
		if p.current.Type != IDENT {
			return nil, fmt.Errorf("expected column name")
		}

		column := p.current.Literal
		p.nextToken()

		if ok, err := p.expectToken(ASSIGN); !ok {
			return nil, err
		}

		value, err := p.parseExpression()
		if err != nil {
			return nil, err
		}

		stmt.Assignments = append(stmt.Assignments, &AssignmentExpression{
			Column: column,
			Value:  value,
		})

		if p.current.Type == COMMA {
			p.nextToken()
		} else {
			break
		}
	}

	// Parse WHERE clause
	if p.current.Type == WHERE {
		p.nextToken()
		where, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	return stmt, nil
}

func (p *Parser) parseDeleteStatement() (*DeleteStatement, error) {
	p.nextToken() // consume DELETE

	if ok, err := p.expectToken(FROM); !ok {
		return nil, err
	}

	if p.current.Type != IDENT {
		return nil, fmt.Errorf("expected table name")
	}

	stmt := &DeleteStatement{
		Table: p.current.Literal,
	}
	p.nextToken()

	// Parse WHERE clause
	if p.current.Type == WHERE {
		p.nextToken()
		where, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}

	return stmt, nil
}

// ==================== Column and Constraint Parsing ====================

func (p *Parser) parseColumnDefinition() (*ColumnDefinition, error) {
	if p.current.Type != IDENT {
		return nil, fmt.Errorf("expected column name")
	}

	column := &ColumnDefinition{
		Name: p.current.Literal,
	}
	p.nextToken()

	// Parse data type
	dataType, err := p.parseDataType()
	if err != nil {
		return nil, err
	}
	column.DataType = dataType

	// Parse column constraints
	for {
		switch p.current.Type {
		case NOT:
			p.nextToken()
			if ok, err := p.expectToken(NULL); ok {
				nullable := false
				column.Nullable = &nullable
			} else if err != nil {
				return nil, err
			}
		case NULL:
			p.nextToken()
			nullable := true
			column.Nullable = &nullable
		case DEFAULT:
			p.nextToken()
			defaultExpr, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			column.Default = defaultExpr
		case AUTO_INCREMENT:
			p.nextToken()
			column.AutoIncrement = true
		case PRIMARY:
			p.nextToken()
			if ok, err := p.expectToken(KEY); ok {
				column.PrimaryKey = true
			} else if err != nil {
				return nil, err
			}
		case UNIQUE:
			p.nextToken()
			column.Unique = true
		default:
			return column, nil
		}
	}
}

func (p *Parser) parseDataType() (DataTypeDefinition, error) {
	if !IsDataType(p.current.Type) {
		return DataTypeDefinition{}, fmt.Errorf("expected data type, got %s", p.current.Type.String())
	}

	dataType := DataTypeDefinition{
		Type: p.current.Literal,
	}
	p.nextToken()

	// Parse optional size/precision
	if p.current.Type == LPAREN {
		p.nextToken()

		if p.current.Type == INT {
			size, err := strconv.Atoi(p.current.Literal)
			if err == nil {
				dataType.Size = &size
			}
			p.nextToken()

			// Check for scale (for DECIMAL/NUMERIC)
			if p.current.Type == COMMA {
				p.nextToken()
				if p.current.Type == INT {
					scale, err := strconv.Atoi(p.current.Literal)
					if err == nil {
						dataType.Scale = &scale
					}
					p.nextToken()
				}
			}
		}

		if ok, err := p.expectToken(RPAREN); !ok {
			return dataType, err
		}
	}

	return dataType, nil
}

func (p *Parser) parseConstraintDefinition() (*ConstraintDefinition, error) {
	constraint := &ConstraintDefinition{}

	// Optional constraint name
	if p.current.Type == CONSTRAINT {
		p.nextToken()
		if p.current.Type == IDENT {
			constraint.Name = p.current.Literal
			p.nextToken()
		}
	}

	switch p.current.Type {
	case PRIMARY:
		p.nextToken()
		if ok, err := p.expectToken(KEY); ok {
			constraint.Type = "PRIMARY KEY"
			if ok, err := p.expectToken(LPAREN); ok {
				cols, err := p.parseColumnList()
				if err != nil {
					return nil, err
				}
				constraint.Columns = cols
				if ok, err := p.expectToken(RPAREN); !ok && err != nil {
					return nil, err
				}
			} else if err != nil {
				return nil, err
			}
		} else if err != nil {
			return nil, err
		}
	case FOREIGN:
		p.nextToken()
		if ok, err := p.expectToken(KEY); ok {
			constraint.Type = "FOREIGN KEY"
			if ok, err := p.expectToken(LPAREN); ok {
				cols, err := p.parseColumnList()
				if err != nil {
					return nil, err
				}
				constraint.Columns = cols
				if ok, err := p.expectToken(RPAREN); ok {
					if ok, err := p.expectToken(REFERENCES); ok {
						if p.current.Type == IDENT {
							constraint.RefTable = p.current.Literal
							p.nextToken()
							if ok, err := p.expectToken(LPAREN); ok {
								refCols, err := p.parseColumnList()
								if err != nil {
									return nil, err
								}
								constraint.RefColumns = refCols
								if ok, err := p.expectToken(RPAREN); !ok && err != nil {
									return nil, err
								}
							} else if err != nil {
								return nil, err
							}
						}
					} else if err != nil {
						return nil, err
					}
				} else if err != nil {
					return nil, err
				}
			} else if err != nil {
				return nil, err
			}
		} else if err != nil {
			return nil, err
		}
	case UNIQUE:
		p.nextToken()
		constraint.Type = "UNIQUE"
		if ok, err := p.expectToken(LPAREN); ok {
			cols, err := p.parseColumnList()
			if err != nil {
				return nil, err
			}
			constraint.Columns = cols
			if ok, err := p.expectToken(RPAREN); !ok && err != nil {
				return nil, err
			}
		} else if err != nil {
			return nil, err
		}
	case CHECK:
		p.nextToken()
		constraint.Type = "CHECK"
		if ok, err := p.expectToken(LPAREN); ok {
			checkExpr, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			constraint.CheckExpr = checkExpr
			if ok, err := p.expectToken(RPAREN); !ok && err != nil {
				return nil, err
			}
		} else if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unexpected constraint type: %s", p.current.Type.String())
	}

	return constraint, nil
}

// ==================== Expression Parsing ====================

func (p *Parser) parseExpression() (Expression, error) {
	return p.parseOrExpression()
}

func (p *Parser) parseOrExpression() (Expression, error) {
	expr, err := p.parseAndExpression()
	if err != nil {
		return nil, err
	}

	for p.current.Type == OR {
		operator := p.current.Literal
		p.nextToken()
		right, err := p.parseAndExpression()
		if err != nil {
			return nil, err
		}
		expr = &BinaryExpression{
			Left:     expr,
			Operator: operator,
			Right:    right,
		}
	}

	return expr, nil
}

func (p *Parser) parseAndExpression() (Expression, error) {
	expr, err := p.parseEqualityExpression()
	if err != nil {
		return nil, err
	}

	for p.current.Type == AND {
		operator := p.current.Literal
		p.nextToken()
		right, err := p.parseEqualityExpression()
		if err != nil {
			return nil, err
		}
		expr = &BinaryExpression{
			Left:     expr,
			Operator: operator,
			Right:    right,
		}
	}

	return expr, nil
}

func (p *Parser) parseEqualityExpression() (Expression, error) {
	expr, err := p.parseComparisonExpression()
	if err != nil {
		return nil, err
	}

	for p.current.Type == EQ || p.current.Type == ASSIGN || p.current.Type == NOT_EQ {
		operator := p.current.Literal
		p.nextToken()
		right, err := p.parseComparisonExpression()
		if err != nil {
			return nil, err
		}
		expr = &BinaryExpression{
			Left:     expr,
			Operator: operator,
			Right:    right,
		}
	}

	return expr, nil
}

func (p *Parser) parseComparisonExpression() (Expression, error) {
	expr, err := p.parseAdditiveExpression()
	if err != nil {
		return nil, err
	}

	for IsComparisonOperator(p.current.Type) {
		operator := p.current.Literal
		p.nextToken()
		right, err := p.parseAdditiveExpression()
		if err != nil {
			return nil, err
		}
		expr = &BinaryExpression{
			Left:     expr,
			Operator: operator,
			Right:    right,
		}
	}

	return expr, nil
}

func (p *Parser) parseAdditiveExpression() (Expression, error) {
	expr, err := p.parseMultiplicativeExpression()
	if err != nil {
		return nil, err
	}

	for p.current.Type == PLUS || p.current.Type == MINUS {
		operator := p.current.Literal
		p.nextToken()
		right, err := p.parseMultiplicativeExpression()
		if err != nil {
			return nil, err
		}
		expr = &BinaryExpression{
			Left:     expr,
			Operator: operator,
			Right:    right,
		}
	}

	return expr, nil
}

func (p *Parser) parseMultiplicativeExpression() (Expression, error) {
	expr, err := p.parseUnaryExpression()
	if err != nil {
		return nil, err
	}

	for p.current.Type == MULTIPLY || p.current.Type == DIVIDE || p.current.Type == MODULO {
		operator := p.current.Literal
		p.nextToken()
		right, err := p.parseUnaryExpression()
		if err != nil {
			return nil, err
		}
		expr = &BinaryExpression{
			Left:     expr,
			Operator: operator,
			Right:    right,
		}
	}

	return expr, nil
}

func (p *Parser) parseUnaryExpression() (Expression, error) {
	if p.current.Type == NOT || p.current.Type == MINUS {
		operator := p.current.Literal
		p.nextToken()
		operand, err := p.parseUnaryExpression()
		if err != nil {
			return nil, err
		}
		return &UnaryExpression{
			Operator: operator,
			Operand:  operand,
		}, nil
	}

	return p.parsePrimaryExpression()
}

func (p *Parser) parsePrimaryExpression() (Expression, error) {
	switch p.current.Type {
	case IDENT, COUNT, SUM, AVG, MIN, MAX:
		// Could be identifier or function call
		name := p.current.Literal
		p.nextToken()

		if p.current.Type == LPAREN {
			// Function call
			p.nextToken()

			var args []Expression
			distinct := false

			// Check for DISTINCT keyword in aggregate functions
			if p.current.Type == DISTINCT {
				distinct = true
				p.nextToken()
			}

			if p.current.Type != RPAREN {
				argList, err := p.parseExpressionList()
				if err != nil {
					return nil, err
				}
				args = argList
			}

			if ok, err := p.expectToken(RPAREN); !ok {
				return nil, err
			}

			return &FunctionCall{
				Name:      name,
				Arguments: args,
				Distinct:  distinct,
			}, nil
		}

		// Check for qualified identifier (table.column)
		if p.current.Type == DOT {
			p.nextToken() // consume DOT
			if p.current.Type == IDENT {
				column := p.current.Literal
				p.nextToken()
				return &QualifiedIdentifier{
					Table:  name,
					Column: column,
				}, nil
			} else {
				return nil, fmt.Errorf("expected column name after DOT")
			}
		}

		return &Identifier{Value: name}, nil

	case INT:
		value, err := strconv.Atoi(p.current.Literal)
		if err != nil {
			return nil, err
		}
		p.nextToken()
		return &Literal{Value: value}, nil

	case FLOAT:
		value, err := strconv.ParseFloat(p.current.Literal, 64)
		if err != nil {
			return nil, err
		}
		p.nextToken()
		return &Literal{Value: value}, nil

	case STRING:
		value := p.current.Literal
		p.nextToken()
		return &Literal{Value: value}, nil

	case BOOLEAN:
		value := strings.ToUpper(p.current.Literal) == "TRUE"
		p.nextToken()
		return &Literal{Value: value}, nil

	case NULL:
		p.nextToken()
		return &Literal{Value: nil}, nil

	case ASTERISK:
		p.nextToken()
		return &Identifier{Value: "*"}, nil

	case LPAREN:
		p.nextToken()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		if ok, err := p.expectToken(RPAREN); !ok {
			return nil, err
		}
		return expr, nil

	default:
		return nil, fmt.Errorf("unexpected token %s at line %d, column %d",
			p.current.Type.String(), p.current.Line, p.current.Column)
	}
}

// ==================== Helper Functions ====================

func (p *Parser) parseSelectList() ([]Expression, error) {
	var columns []Expression

	for {
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		if expr == nil {
			break
		}

		// Check for alias
		if p.current.Type == AS {
			p.nextToken()
			if p.current.Type == IDENT || p.current.Type == STRING {
				alias := p.current.Literal
				if p.current.Type == STRING {
					// Remove surrounding quotes if present
					if len(alias) > 1 && (alias[0] == '"' && alias[len(alias)-1] == '"') {
						alias = alias[1 : len(alias)-1]
					}
				}
				p.nextToken()
				expr = &AliasExpression{Expr: expr, Alias: alias}
			}
		}

		columns = append(columns, expr)

		if p.current.Type == COMMA {
			p.nextToken()
		} else {
			break
		}
	}

	return columns, nil
}

func (p *Parser) parseFromClause() (*FromClause, error) {
	p.nextToken() // consume FROM

	if p.current.Type != IDENT {
		return nil, fmt.Errorf("expected table name or alias after FROM")
	}

	fromClause := &FromClause{
		Table: p.current.Literal,
	}
	p.nextToken()

	// Check for alias
	if p.current.Type == AS {
		p.nextToken()
		if p.current.Type == IDENT {
			fromClause.Alias = p.current.Literal
			p.nextToken()
		}
	} else if p.current.Type == IDENT && !IsKeyword(p.current.Type) {
		// Implicit alias
		fromClause.Alias = p.current.Literal
		p.nextToken()
	}

	return fromClause, nil
}

func (p *Parser) parseJoinClause() (*JoinClause, error) {
	joinType := ""

	// Parse join type
	if p.current.Type == INNER {
		joinType = "INNER"
		p.nextToken()
	} else if p.current.Type == LEFT {
		joinType = "LEFT"
		p.nextToken()
		if p.current.Type == OUTER {
			joinType = "LEFT OUTER"
			p.nextToken()
		}
	} else if p.current.Type == RIGHT {
		joinType = "RIGHT"
		p.nextToken()
		if p.current.Type == OUTER {
			joinType = "RIGHT OUTER"
			p.nextToken()
		}
	} else if p.current.Type == FULL {
		joinType = "FULL"
		p.nextToken()
		if p.current.Type == OUTER {
			joinType = "FULL OUTER"
			p.nextToken()
		}
	}

	if ok, err := p.expectToken(JOIN); !ok {
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("expected JOIN after join type")
	}

	if p.current.Type != IDENT {
		return nil, fmt.Errorf("expected table name or alias after JOIN")
	}

	joinClause := &JoinClause{
		Type:  joinType,
		Table: p.current.Literal,
	}
	p.nextToken()

	// Check for alias
	if p.current.Type == AS {
		p.nextToken()
		if p.current.Type == IDENT {
			joinClause.Alias = p.current.Literal
			p.nextToken()
		}
	} else if p.current.Type == IDENT && !IsKeyword(p.current.Type) {
		// Implicit alias
		joinClause.Alias = p.current.Literal
		p.nextToken()
	}

	// Parse join condition
	if p.current.Type == ON {
		p.nextToken()
		condition, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		joinClause.Condition = condition
	}

	return joinClause, nil
}

func (p *Parser) parseOrderByClause() ([]*OrderByClause, error) {
	var orderBy []*OrderByClause

	for {
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		if expr == nil {
			break
		}

		orderClause := &OrderByClause{
			Expression: expr,
		}

		// Check for ASC/DESC
		if p.current.Type == IDENT {
			direction := strings.ToUpper(p.current.Literal)
			if direction == "ASC" || direction == "DESC" {
				orderClause.Direction = direction
				p.nextToken()
			}
		}

		orderBy = append(orderBy, orderClause)

		if p.current.Type == COMMA {
			p.nextToken()
		} else {
			break
		}
	}

	return orderBy, nil
}

func (p *Parser) parseExpressionList() ([]Expression, error) {
	var expressions []Expression

	for {
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		if expr == nil {
			break
		}

		expressions = append(expressions, expr)

		if p.current.Type == COMMA {
			p.nextToken()
		} else {
			break
		}
	}

	return expressions, nil
}

func (p *Parser) parseColumnList() ([]string, error) {
	var columns []string

	for {
		if p.current.Type != IDENT {
			return nil, fmt.Errorf("expected column name")
		}

		columns = append(columns, p.current.Literal)
		p.nextToken()

		if p.current.Type == COMMA {
			p.nextToken()
		} else {
			break
		}
	}

	return columns, nil
}

func (p *Parser) isJoinKeyword(tokenType TokenType) bool {
	return tokenType == JOIN || tokenType == INNER || tokenType == LEFT ||
		tokenType == RIGHT || tokenType == FULL
}

// Reset resets the parser state
func (p *Parser) Reset(input string) {
	p.lexer = NewAdvancedLexer(input)
	p.nextToken()
}

// ParseExpression parses a single expression (useful for WHERE clauses, etc.)
func (p *Parser) ParseExpression() (Expression, error) {
	return p.parseExpression()
}

// ParseStatement parses a single statement
func (p *Parser) ParseStatement() (Statement, error) {
	return p.parseStatement()
}
