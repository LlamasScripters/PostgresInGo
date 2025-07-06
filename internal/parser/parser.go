package parser

import (
	"fmt"
	"strconv"
	"strings"
)

// Parser represents the SQL parser
type Parser struct {
	lexer   *AdvancedLexer
	errors  []string
	current Token
}

// NewParser creates a new SQL parser
func NewParser(input string) *Parser {
	lexer := NewAdvancedLexer(input)
	p := &Parser{
		lexer:  lexer,
		errors: []string{},
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

		s := p.parseStatement()
		if s != nil {
			stmt.Statements = append(stmt.Statements, s)
		}

		// Skip semicolon if present
		if p.current.Type == SEMICOLON {
			p.nextToken()
		}
	}

	if len(p.errors) > 0 {
		return nil, fmt.Errorf("parse errors: %s", strings.Join(p.errors, "; "))
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
func (p *Parser) expectToken(expected TokenType) bool {
	if p.current.Type == expected {
		p.nextToken()
		return true
	}
	p.addError(fmt.Sprintf("expected %s, got %s at line %d, column %d",
		expected.String(), p.current.Type.String(), p.current.Line, p.current.Column))
	return false
}

// addError adds an error to the error list
func (p *Parser) addError(msg string) {
	p.errors = append(p.errors, msg)
}

// parseStatement parses a top-level SQL statement
func (p *Parser) parseStatement() Statement {
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
		p.addError(fmt.Sprintf("unexpected token %s at line %d, column %d",
			p.current.Type.String(), p.current.Line, p.current.Column))
		p.nextToken() // Skip invalid token
		return nil
	}
}

// ==================== CREATE Statements ====================

func (p *Parser) parseCreateStatement() Statement {
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
		p.addError("expected INDEX after UNIQUE")
		return nil
	case INDEX:
		return p.parseCreateIndex(false)
	default:
		p.addError(fmt.Sprintf("unexpected token after CREATE: %s", p.current.Type.String()))
		return nil
	}
}

func (p *Parser) parseCreateDatabase() *CreateDatabaseStatement {
	p.nextToken() // consume DATABASE

	if p.current.Type != IDENT {
		p.addError("expected database name")
		return nil
	}

	name := p.current.Literal
	p.nextToken()

	return &CreateDatabaseStatement{Name: name}
}

func (p *Parser) parseCreateTable() *CreateTableStatement {
	p.nextToken() // consume TABLE

	if p.current.Type != IDENT {
		p.addError("expected table name")
		return nil
	}

	name := p.current.Literal
	p.nextToken()

	if !p.expectToken(LPAREN) {
		return nil
	}

	var columns []*ColumnDefinition
	var constraints []*ConstraintDefinition

	for p.current.Type != RPAREN && !p.isAtEnd() {
		if p.current.Type == CONSTRAINT || p.current.Type == PRIMARY ||
			p.current.Type == FOREIGN || p.current.Type == UNIQUE || p.current.Type == CHECK {
			constraint := p.parseConstraintDefinition()
			if constraint != nil {
				constraints = append(constraints, constraint)
			}
		} else {
			column := p.parseColumnDefinition()
			if column != nil {
				columns = append(columns, column)
			}
		}

		if p.current.Type == COMMA {
			p.nextToken()
		} else if p.current.Type != RPAREN {
			p.addError("expected ',' or ')' in table definition")
			break
		}
	}

	if !p.expectToken(RPAREN) {
		return nil
	}

	return &CreateTableStatement{
		Name:        name,
		Columns:     columns,
		Constraints: constraints,
	}
}

func (p *Parser) parseCreateIndex(unique bool) *CreateIndexStatement {
	p.nextToken() // consume INDEX

	if p.current.Type != IDENT {
		p.addError("expected index name")
		return nil
	}

	name := p.current.Literal
	p.nextToken()

	if !p.expectToken(ON) {
		return nil
	}

	if p.current.Type != IDENT {
		p.addError("expected table name")
		return nil
	}

	table := p.current.Literal
	p.nextToken()

	if !p.expectToken(LPAREN) {
		return nil
	}

	var columns []string
	for p.current.Type != RPAREN && !p.isAtEnd() {
		if p.current.Type != IDENT {
			p.addError("expected column name")
			break
		}

		columns = append(columns, p.current.Literal)
		p.nextToken()

		if p.current.Type == COMMA {
			p.nextToken()
		} else if p.current.Type != RPAREN {
			p.addError("expected ',' or ')' in index column list")
			break
		}
	}

	if !p.expectToken(RPAREN) {
		return nil
	}

	return &CreateIndexStatement{
		Name:    name,
		Table:   table,
		Columns: columns,
		Unique:  unique,
	}
}

func (p *Parser) parseCreateView() *CreateViewStatement {
	p.nextToken() // consume VIEW

	if p.current.Type != IDENT {
		p.addError("expected view name")
		return nil
	}

	viewName := p.current.Literal
	p.nextToken()

	var columns []string

	// Check for optional column list
	if p.current.Type == LPAREN {
		p.nextToken() // consume (

		for {
			if p.current.Type != IDENT {
				p.addError("expected column name")
				return nil
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
				p.addError("expected ',' or ')' in column list")
				return nil
			}
		}
	}

	// Expect AS keyword
	if p.current.Type != AS {
		p.addError("expected AS after view name")
		return nil
	}
	p.nextToken() // consume AS

	// Parse the SELECT statement
	if p.current.Type != SELECT {
		p.addError("expected SELECT statement after AS")
		return nil
	}

	selectStmt := p.parseSelectStatement()
	if selectStmt == nil {
		return nil
	}

	// Extract the raw SQL definition
	definition := selectStmt.String()

	return &CreateViewStatement{
		Name:       viewName,
		Columns:    columns,
		Query:      selectStmt,
		Definition: definition,
	}
}

// ==================== DROP Statements ====================

func (p *Parser) parseDropStatement() Statement {
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
		p.addError(fmt.Sprintf("unexpected token after DROP: %s", p.current.Type.String()))
		return nil
	}
}

func (p *Parser) parseDropDatabase() *DropDatabaseStatement {
	p.nextToken() // consume DATABASE

	if p.current.Type != IDENT {
		p.addError("expected database name")
		return nil
	}

	name := p.current.Literal
	p.nextToken()

	return &DropDatabaseStatement{Name: name}
}

func (p *Parser) parseDropTable() *DropTableStatement {
	p.nextToken() // consume TABLE

	if p.current.Type != IDENT {
		p.addError("expected table name")
		return nil
	}

	name := p.current.Literal
	p.nextToken()

	return &DropTableStatement{Name: name}
}

func (p *Parser) parseDropIndex() *DropIndexStatement {
	p.nextToken() // consume INDEX

	if p.current.Type != IDENT {
		p.addError("expected index name")
		return nil
	}

	name := p.current.Literal
	p.nextToken()

	return &DropIndexStatement{Name: name}
}

func (p *Parser) parseDropView() *DropViewStatement {
	p.nextToken() // consume VIEW

	if p.current.Type != IDENT {
		p.addError("expected view name")
		return nil
	}

	name := p.current.Literal
	p.nextToken()

	return &DropViewStatement{Name: name}
}

// ==================== DML Statements ====================

func (p *Parser) parseSelectStatement() *SelectStatement {
	p.nextToken() // consume SELECT

	stmt := &SelectStatement{}

	// Check for DISTINCT
	if p.current.Type == DISTINCT {
		stmt.Distinct = true
		p.nextToken()
	}

	// Parse column list
	stmt.Columns = p.parseSelectList()

	// Parse FROM clause
	if p.current.Type == FROM {
		stmt.From = p.parseFromClause()
	}

	// Parse JOINs
	for p.isJoinKeyword(p.current.Type) {
		join := p.parseJoinClause()
		if join != nil {
			stmt.Joins = append(stmt.Joins, join)
		}
	}

	// Parse WHERE clause
	if p.current.Type == WHERE {
		p.nextToken()
		stmt.Where = p.parseExpression()
	}

	// Parse GROUP BY clause
	if p.current.Type == GROUP {
		p.nextToken()
		if p.expectToken(BY) {
			stmt.GroupBy = p.parseExpressionList()
		}
	}

	// Parse HAVING clause
	if p.current.Type == HAVING {
		p.nextToken()
		stmt.Having = p.parseExpression()
	}

	// Parse ORDER BY clause
	if p.current.Type == ORDER {
		p.nextToken()
		if p.expectToken(BY) {
			stmt.OrderBy = p.parseOrderByClause()
		}
	}

	// Parse LIMIT clause
	if p.current.Type == LIMIT {
		p.nextToken()
		stmt.Limit = p.parseExpression()
	}

	// Parse OFFSET clause
	if p.current.Type == OFFSET {
		p.nextToken()
		stmt.Offset = p.parseExpression()
	}

	return stmt
}

func (p *Parser) parseInsertStatement() *InsertStatement {
	p.nextToken() // consume INSERT

	if !p.expectToken(INTO) {
		return nil
	}

	if p.current.Type != IDENT {
		p.addError("expected table name")
		return nil
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
				p.addError("expected column name")
				break
			}

			stmt.Columns = append(stmt.Columns, p.current.Literal)
			p.nextToken()

			if p.current.Type == COMMA {
				p.nextToken()
			} else if p.current.Type != RPAREN {
				p.addError("expected ',' or ')' in column list")
				break
			}
		}

		if !p.expectToken(RPAREN) {
			return nil
		}
	}

	if !p.expectToken(VALUES) {
		return nil
	}

	// Parse value lists
	for {
		if !p.expectToken(LPAREN) {
			return nil
		}

		var values []Expression
		for p.current.Type != RPAREN && !p.isAtEnd() {
			expr := p.parseExpression()
			if expr != nil {
				values = append(values, expr)
			}

			if p.current.Type == COMMA {
				p.nextToken()
			} else if p.current.Type != RPAREN {
				p.addError("expected ',' or ')' in value list")
				break
			}
		}

		if !p.expectToken(RPAREN) {
			return nil
		}

		stmt.Values = append(stmt.Values, values)

		if p.current.Type == COMMA {
			p.nextToken()
		} else {
			break
		}
	}

	return stmt
}

func (p *Parser) parseUpdateStatement() *UpdateStatement {
	p.nextToken() // consume UPDATE

	if p.current.Type != IDENT {
		p.addError("expected table name")
		return nil
	}

	stmt := &UpdateStatement{
		Table: p.current.Literal,
	}
	p.nextToken()

	if !p.expectToken(SET) {
		return nil
	}

	// Parse assignments
	for {
		if p.current.Type != IDENT {
			p.addError("expected column name")
			break
		}

		column := p.current.Literal
		p.nextToken()

		if !p.expectToken(ASSIGN) {
			break
		}

		value := p.parseExpression()
		if value == nil {
			break
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
		stmt.Where = p.parseExpression()
	}

	return stmt
}

func (p *Parser) parseDeleteStatement() *DeleteStatement {
	p.nextToken() // consume DELETE

	if !p.expectToken(FROM) {
		return nil
	}

	if p.current.Type != IDENT {
		p.addError("expected table name")
		return nil
	}

	stmt := &DeleteStatement{
		Table: p.current.Literal,
	}
	p.nextToken()

	// Parse WHERE clause
	if p.current.Type == WHERE {
		p.nextToken()
		stmt.Where = p.parseExpression()
	}

	return stmt
}

// ==================== Column and Constraint Parsing ====================

func (p *Parser) parseColumnDefinition() *ColumnDefinition {
	if p.current.Type != IDENT {
		p.addError("expected column name")
		return nil
	}

	column := &ColumnDefinition{
		Name: p.current.Literal,
	}
	p.nextToken()

	// Parse data type
	column.DataType = p.parseDataType()

	// Parse column constraints
	for {
		switch p.current.Type {
		case NOT:
			p.nextToken()
			if p.expectToken(NULL) {
				nullable := false
				column.Nullable = &nullable
			}
		case NULL:
			p.nextToken()
			nullable := true
			column.Nullable = &nullable
		case DEFAULT:
			p.nextToken()
			column.Default = p.parseExpression()
		case AUTO_INCREMENT:
			p.nextToken()
			column.AutoIncrement = true
		case PRIMARY:
			p.nextToken()
			if p.expectToken(KEY) {
				column.PrimaryKey = true
			}
		case UNIQUE:
			p.nextToken()
			column.Unique = true
		default:
			return column
		}
	}
}

func (p *Parser) parseDataType() DataTypeDefinition {
	if !IsDataType(p.current.Type) {
		p.addError(fmt.Sprintf("expected data type, got %s", p.current.Type.String()))
		return DataTypeDefinition{}
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

		if !p.expectToken(RPAREN) {
			return dataType
		}
	}

	return dataType
}

func (p *Parser) parseConstraintDefinition() *ConstraintDefinition {
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
		if p.expectToken(KEY) {
			constraint.Type = "PRIMARY KEY"
			if p.expectToken(LPAREN) {
				constraint.Columns = p.parseColumnList()
				p.expectToken(RPAREN)
			}
		}
	case FOREIGN:
		p.nextToken()
		if p.expectToken(KEY) {
			constraint.Type = "FOREIGN KEY"
			if p.expectToken(LPAREN) {
				constraint.Columns = p.parseColumnList()
				if p.expectToken(RPAREN) && p.expectToken(REFERENCES) {
					if p.current.Type == IDENT {
						constraint.RefTable = p.current.Literal
						p.nextToken()
						if p.expectToken(LPAREN) {
							constraint.RefColumns = p.parseColumnList()
							p.expectToken(RPAREN)
						}
					}
				}
			}
		}
	case UNIQUE:
		p.nextToken()
		constraint.Type = "UNIQUE"
		if p.expectToken(LPAREN) {
			constraint.Columns = p.parseColumnList()
			p.expectToken(RPAREN)
		}
	case CHECK:
		p.nextToken()
		constraint.Type = "CHECK"
		if p.expectToken(LPAREN) {
			constraint.CheckExpr = p.parseExpression()
			p.expectToken(RPAREN)
		}
	default:
		p.addError(fmt.Sprintf("unexpected constraint type: %s", p.current.Type.String()))
		return nil
	}

	return constraint
}

// ==================== Expression Parsing ====================

func (p *Parser) parseExpression() Expression {
	return p.parseOrExpression()
}

func (p *Parser) parseOrExpression() Expression {
	expr := p.parseAndExpression()

	for p.current.Type == OR {
		operator := p.current.Literal
		p.nextToken()
		right := p.parseAndExpression()
		expr = &BinaryExpression{
			Left:     expr,
			Operator: operator,
			Right:    right,
		}
	}

	return expr
}

func (p *Parser) parseAndExpression() Expression {
	expr := p.parseEqualityExpression()

	for p.current.Type == AND {
		operator := p.current.Literal
		p.nextToken()
		right := p.parseEqualityExpression()
		expr = &BinaryExpression{
			Left:     expr,
			Operator: operator,
			Right:    right,
		}
	}

	return expr
}

func (p *Parser) parseEqualityExpression() Expression {
	expr := p.parseComparisonExpression()

	for p.current.Type == EQ || p.current.Type == ASSIGN || p.current.Type == NOT_EQ {
		operator := p.current.Literal
		p.nextToken()
		right := p.parseComparisonExpression()
		expr = &BinaryExpression{
			Left:     expr,
			Operator: operator,
			Right:    right,
		}
	}

	return expr
}

func (p *Parser) parseComparisonExpression() Expression {
	expr := p.parseAdditiveExpression()

	for IsComparisonOperator(p.current.Type) {
		operator := p.current.Literal
		p.nextToken()
		right := p.parseAdditiveExpression()
		expr = &BinaryExpression{
			Left:     expr,
			Operator: operator,
			Right:    right,
		}
	}

	return expr
}

func (p *Parser) parseAdditiveExpression() Expression {
	expr := p.parseMultiplicativeExpression()

	for p.current.Type == PLUS || p.current.Type == MINUS {
		operator := p.current.Literal
		p.nextToken()
		right := p.parseMultiplicativeExpression()
		expr = &BinaryExpression{
			Left:     expr,
			Operator: operator,
			Right:    right,
		}
	}

	return expr
}

func (p *Parser) parseMultiplicativeExpression() Expression {
	expr := p.parseUnaryExpression()

	for p.current.Type == MULTIPLY || p.current.Type == DIVIDE || p.current.Type == MODULO {
		operator := p.current.Literal
		p.nextToken()
		right := p.parseUnaryExpression()
		expr = &BinaryExpression{
			Left:     expr,
			Operator: operator,
			Right:    right,
		}
	}

	return expr
}

func (p *Parser) parseUnaryExpression() Expression {
	if p.current.Type == NOT || p.current.Type == MINUS {
		operator := p.current.Literal
		p.nextToken()
		operand := p.parseUnaryExpression()
		return &UnaryExpression{
			Operator: operator,
			Operand:  operand,
		}
	}

	return p.parsePrimaryExpression()
}

func (p *Parser) parsePrimaryExpression() Expression {
	switch p.current.Type {
	case IDENT:
		// Could be identifier or function call
		name := p.current.Literal
		p.nextToken()

		if p.current.Type == LPAREN {
			// Function call
			p.nextToken()

			var args []Expression
			if p.current.Type != RPAREN {
				args = p.parseExpressionList()
			}

			if !p.expectToken(RPAREN) {
				return nil
			}

			return &FunctionCall{
				Name:      name,
				Arguments: args,
			}
		}

		return &Identifier{Value: name}

	case INT:
		value, err := strconv.Atoi(p.current.Literal)
		if err != nil {
			p.addError(fmt.Sprintf("invalid integer: %s", p.current.Literal))
			return nil
		}
		p.nextToken()
		return &Literal{Value: value}

	case FLOAT:
		value, err := strconv.ParseFloat(p.current.Literal, 64)
		if err != nil {
			p.addError(fmt.Sprintf("invalid float: %s", p.current.Literal))
			return nil
		}
		p.nextToken()
		return &Literal{Value: value}

	case STRING:
		value := p.current.Literal
		p.nextToken()
		return &Literal{Value: value}

	case BOOLEAN:
		value := strings.ToUpper(p.current.Literal) == "TRUE"
		p.nextToken()
		return &Literal{Value: value}

	case NULL:
		p.nextToken()
		return &Literal{Value: nil}

	case ASTERISK:
		p.nextToken()
		return &Identifier{Value: "*"}

	case LPAREN:
		p.nextToken()
		expr := p.parseExpression()
		if !p.expectToken(RPAREN) {
			return nil
		}
		return expr

	default:
		p.addError(fmt.Sprintf("unexpected token in expression: %s", p.current.Type.String()))
		return nil
	}
}

// ==================== Helper Functions ====================

func (p *Parser) parseSelectList() []Expression {
	var columns []Expression

	for {
		expr := p.parseExpression()
		if expr == nil {
			break
		}

		// Check for alias
		if p.current.Type == AS {
			p.nextToken()
			if p.current.Type == IDENT {
				// For now, we'll just use the expression as-is
				// In a full implementation, you'd create an AliasExpression
				p.nextToken()
			}
		}

		columns = append(columns, expr)

		if p.current.Type == COMMA {
			p.nextToken()
		} else {
			break
		}
	}

	return columns
}

func (p *Parser) parseFromClause() *FromClause {
	p.nextToken() // consume FROM

	if p.current.Type != IDENT {
		p.addError("expected table name after FROM")
		return nil
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

	return fromClause
}

func (p *Parser) parseJoinClause() *JoinClause {
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

	if !p.expectToken(JOIN) {
		return nil
	}

	if p.current.Type != IDENT {
		p.addError("expected table name after JOIN")
		return nil
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
		joinClause.Condition = p.parseExpression()
	}

	return joinClause
}

func (p *Parser) parseOrderByClause() []*OrderByClause {
	var orderBy []*OrderByClause

	for {
		expr := p.parseExpression()
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

	return orderBy
}

func (p *Parser) parseExpressionList() []Expression {
	var expressions []Expression

	for {
		expr := p.parseExpression()
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

	return expressions
}

func (p *Parser) parseColumnList() []string {
	var columns []string

	for {
		if p.current.Type != IDENT {
			p.addError("expected column name")
			break
		}

		columns = append(columns, p.current.Literal)
		p.nextToken()

		if p.current.Type == COMMA {
			p.nextToken()
		} else {
			break
		}
	}

	return columns
}

func (p *Parser) isJoinKeyword(tokenType TokenType) bool {
	return tokenType == JOIN || tokenType == INNER || tokenType == LEFT ||
		tokenType == RIGHT || tokenType == FULL
}

// GetErrors returns the list of parse errors
func (p *Parser) GetErrors() []string {
	return p.errors
}

// HasErrors checks if there are any parse errors
func (p *Parser) HasErrors() bool {
	return len(p.errors) > 0
}

// Reset resets the parser state
func (p *Parser) Reset(input string) {
	p.lexer = NewAdvancedLexer(input)
	p.errors = []string{}
	p.nextToken()
}

// ParseExpression parses a single expression (useful for WHERE clauses, etc.)
func (p *Parser) ParseExpression() (Expression, error) {
	expr := p.parseExpression()
	if len(p.errors) > 0 {
		return nil, fmt.Errorf("parse errors: %s", strings.Join(p.errors, "; "))
	}
	return expr, nil
}

// ParseStatement parses a single statement
func (p *Parser) ParseStatement() (Statement, error) {
	stmt := p.parseStatement()
	if len(p.errors) > 0 {
		return nil, fmt.Errorf("parse errors: %s", strings.Join(p.errors, "; "))
	}
	return stmt, nil
}
