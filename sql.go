// Package sql provides SQL AST types for implementing custom database backends.
//
// This package defines the abstract syntax tree (AST) representation of SQL statements
// used by orm1. Backend implementations translate these AST nodes into database-specific
// SQL dialects.
//
// Most users will not need to use this package directly. It is primarily for developers
// implementing custom database backends.
package orm1

import (
	"fmt"
	"strings"
)

// SQL is a marker interface for SQL AST nodes
type SQL interface {
	isSQL()
}

// SQLN represents a simple SQL name (column or table without qualifier).
type SQLN struct{ Part string }

// SQLQN represents a qualified SQL name (schema.table or table.column).
type SQLQN struct{ Part1, Part2 string }

// SQLText represents literal SQL text.
type SQLText struct{ Text string }

// SQLParam represents a parameterized value in a SQL statement.
type SQLParam struct{ Value any }

// SQLAll represents an AND conjunction of SQL conditions.
type SQLAll struct{ Els []SQL }

// SQLAny represents an OR disjunction of SQL conditions.
type SQLAny struct{ Els []SQL }

// SQLEq represents an equality comparison (=).
type SQLEq struct{ Left, Right SQL }

// SQLLt represents a less-than comparison (<).
type SQLLt struct{ Left, Right SQL }

// SQLGt represents a greater-than comparison (>).
type SQLGt struct{ Left, Right SQL }

// SQLIsNull represents an IS NULL check.
type SQLIsNull struct{ Operand SQL }

// SQLIsNotNull represents an IS NOT NULL check.
type SQLIsNotNull struct{ Operand SQL }

// SQLFragment represents parsed SQL with interpolated parameters.
type SQLFragment struct{ Els []SQL }

// Marker method implementations
func (SQLN) isSQL()         {}
func (SQLQN) isSQL()        {}
func (SQLText) isSQL()      {}
func (SQLParam) isSQL()     {}
func (SQLAll) isSQL()       {}
func (SQLAny) isSQL()       {}
func (SQLEq) isSQL()        {}
func (SQLLt) isSQL()        {}
func (SQLGt) isSQL()        {}
func (SQLIsNull) isSQL()    {}
func (SQLIsNotNull) isSQL() {}
func (SQLFragment) isSQL()  {}

// Join represents a SQL JOIN clause with its condition.
type Join struct {
	Type  string // "JOIN" | "LEFT JOIN"
	Table SQL
	Alias SQL
	On    SQL
}

// OrderBy represents a SQL ORDER BY clause with direction and NULL handling.
// NullsLast: true means NULLS LAST, false means NULLS FIRST.
// Always set to ensure consistent behavior across databases (PostgreSQL/Oracle standard).
type OrderBy struct {
	Expr      SQL
	Ascending bool
	NullsLast bool
}

// SQLQuery represents a complex SELECT query with joins, conditions, and ordering.
// Used by EntityQuery for flexible querying.
type SQLQuery struct {
	Select    []SQL
	FromTable SQL
	FromAlias SQL
	Joins     []Join
	Where     *SQL
	OrderBys  []OrderBy
	GroupBy   []SQL
	Having    *SQL
	Limit     *SQL
	Offset    *SQL
}

// ParseSQL parses SQL with qmark (?) style parameters
// Replaces ? with SQLParam containing the actual values
func ParseSQL(sql string, params ...any) SQL {
	// Pre-allocate tokens slice with correct capacity
	// N params means N params + (N+1) text segments = 2N+1 tokens
	tokens := make([]SQL, 0, 2*len(params)+1)
	paramIdx := 0
	var textBuilder strings.Builder
	inSingleQuote := false
	inDoubleQuote := false

	for i := 0; i < len(sql); i++ {
		ch := sql[i]

		if ch == '\'' && !inDoubleQuote {
			inSingleQuote = !inSingleQuote
			textBuilder.WriteByte(ch)
		} else if ch == '"' && !inSingleQuote {
			inDoubleQuote = !inDoubleQuote
			textBuilder.WriteByte(ch)
		} else if ch == '?' && !inSingleQuote && !inDoubleQuote {
			// Only treat ? as parameter placeholder if not inside quotes
			if textBuilder.Len() > 0 {
				tokens = append(tokens, SQLText{Text: textBuilder.String()})
				textBuilder.Reset()
			}
			if paramIdx >= len(params) {
				panic(fmt.Sprintf("Not enough parameters: expected at least %d, got %d", paramIdx+1, len(params)))
			}
			tokens = append(tokens, SQLParam{Value: params[paramIdx]})
			paramIdx++
		} else {
			textBuilder.WriteByte(ch)
		}
	}

	if textBuilder.Len() > 0 {
		tokens = append(tokens, SQLText{Text: textBuilder.String()})
	}

	if paramIdx != len(params) {
		panic(fmt.Sprintf("Too many parameters: expected %d, got %d", paramIdx, len(params)))
	}

	return SQLFragment{Els: tokens}
}
