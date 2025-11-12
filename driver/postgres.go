package driver

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	sqlast "github.com/hanpama/orm1/sql"
)

// postgreSQLBackend implements Backend for PostgreSQL databases.
type postgreSQLBackend struct {
	db         *sql.DB
	tx         *sql.Tx
	paramIndex int
	argsBuffer []any  // Reusable buffer for query parameters
	sqlBuffer  []byte // Reusable buffer for SQL generation
}

// newPostgreSQLBackend creates a new PostgreSQL backend.
func newPostgreSQLBackend(db *sql.DB) Backend {
	return &postgreSQLBackend{
		db: db,
	}
}

// resetArgsBuffer resets the argument buffer to length 0, growing capacity if needed.
// Uses 2x growth factor to reduce reallocation frequency.
func (b *postgreSQLBackend) resetArgsBuffer(minCapacity int) {
	if cap(b.argsBuffer) < minCapacity {
		b.argsBuffer = make([]any, 0, minCapacity*2)
	} else {
		b.argsBuffer = b.argsBuffer[:0]
	}
}

// resetSQLBuffer resets the SQL buffer to length 0, growing capacity if needed.
// Uses 2x growth factor to reduce reallocation frequency.
func (b *postgreSQLBackend) resetSQLBuffer(minCapacity int) {
	b.sqlBuffer = b.sqlBuffer[:0]
	if cap(b.sqlBuffer) < minCapacity {
		b.sqlBuffer = make([]byte, 0, minCapacity*2)
	}
}

// writeString appends a string to the SQL buffer.
func (b *postgreSQLBackend) writeString(s string) {
	b.sqlBuffer = append(b.sqlBuffer, s...)
}

// writeByte appends a byte to the SQL buffer.
func (b *postgreSQLBackend) writeByte(c byte) {
	b.sqlBuffer = append(b.sqlBuffer, c)
}

// quoteIdentifier quotes a SQL identifier by escaping " as "" and wrapping in "
func (b *postgreSQLBackend) quoteIdentifier(identifier string) {
	b.writeByte('"')
	for i := 0; i < len(identifier); i++ {
		if identifier[i] == '"' {
			b.writeString(`""`)
		} else {
			b.writeByte(identifier[i])
		}
	}
	b.writeByte('"')
}

// quoteTable quotes a table reference, optionally with schema
func (b *postgreSQLBackend) quoteTable(schema, table string) {
	if schema != "" {
		b.quoteIdentifier(schema)
		b.writeByte('.')
	}
	b.quoteIdentifier(table)
}

// sqlString returns the current SQL buffer as a string.
func (b *postgreSQLBackend) sqlString() string {
	return string(b.sqlBuffer)
}

// queryContext executes a query using either the transaction or the database connection.
func (b *postgreSQLBackend) queryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if b.tx != nil {
		return b.tx.QueryContext(ctx, query, args...)
	}
	return b.db.QueryContext(ctx, query, args...)
}

// execContext executes a command using either the transaction or the database connection.
func (b *postgreSQLBackend) execContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if b.tx != nil {
		return b.tx.ExecContext(ctx, query, args...)
	}
	return b.db.ExecContext(ctx, query, args...)
}

// queryRowContext executes a query that returns at most one row using either the transaction or the database connection.
func (b *postgreSQLBackend) queryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if b.tx != nil {
		return b.tx.QueryRowContext(ctx, query, args...)
	}
	return b.db.QueryRowContext(ctx, query, args...)
}

// postgreSQLDriver implements Driver for PostgreSQL.
type postgreSQLDriver struct {
	db *sql.DB
}

// NewPostgres creates a Driver for PostgreSQL.
// This driver creates a new backend instance for each session.
func NewPostgres(db *sql.DB) Driver {
	return &postgreSQLDriver{db: db}
}

// CreateBackend creates a new PostgreSQL backend instance.
func (d *postgreSQLDriver) CreateBackend() Backend {
	return newPostgreSQLBackend(d.db)
}

func (d *postgreSQLDriver) Close() error {
	return d.db.Close()
}

// renderSQL renders a SQL AST node to the SQL buffer and appends parameters to args
func (b *postgreSQLBackend) renderSQL(sql sqlast.SQL, args *[]any) {
	switch v := sql.(type) {
	case sqlast.SQLN:
		b.quoteIdentifier(v.Part)
	case sqlast.SQLQN:
		if v.Part1 == "" {
			b.quoteIdentifier(v.Part2)
		} else {
			b.quoteIdentifier(v.Part1)
			b.writeByte('.')
			b.quoteIdentifier(v.Part2)
		}
	case sqlast.SQLText:
		b.writeString(v.Text)
	case sqlast.SQLParam:
		b.paramIndex++
		*args = append(*args, v.Value)
		b.writeByte('$')
		b.writeString(strconv.Itoa(b.paramIndex))
	case sqlast.SQLAll:
		b.writeByte('(')
		for i, el := range v.Els {
			if i > 0 {
				b.writeString(" AND ")
			}
			b.renderSQL(el, args)
		}
		b.writeByte(')')
	case sqlast.SQLAny:
		b.writeByte('(')
		for i, el := range v.Els {
			if i > 0 {
				b.writeString(" OR ")
			}
			b.renderSQL(el, args)
		}
		b.writeByte(')')
	case sqlast.SQLEq:
		b.renderSQL(v.Left, args)
		b.writeString(" = ")
		b.renderSQL(v.Right, args)
	case sqlast.SQLLt:
		b.renderSQL(v.Left, args)
		b.writeString(" < ")
		b.renderSQL(v.Right, args)
	case sqlast.SQLGt:
		b.renderSQL(v.Left, args)
		b.writeString(" > ")
		b.renderSQL(v.Right, args)
	case sqlast.SQLIsNull:
		b.renderSQL(v.Operand, args)
		b.writeString(" IS NULL")
	case sqlast.SQLIsNotNull:
		b.renderSQL(v.Operand, args)
		b.writeString(" IS NOT NULL")
	case sqlast.SQLFragment:
		for _, el := range v.Els {
			b.renderSQL(el, args)
		}
	}
}

// renderSQLQuery renders a SQLQuery to a SQL string with parameter values
func (b *postgreSQLBackend) renderSQLQuery(stmt sqlast.SQLQuery) (string, []any) {
	b.paramIndex = 0
	b.resetArgsBuffer(32)
	b.resetSQLBuffer(512)

	if len(stmt.Select) > 0 {
		b.writeString("SELECT ")
		for i, sel := range stmt.Select {
			if i > 0 {
				b.writeString(", ")
			}
			b.renderSQL(sel, &b.argsBuffer)
		}
	}

	b.writeString(" FROM ")
	b.renderSQL(stmt.FromTable, &b.argsBuffer)
	b.writeString(" AS ")
	b.renderSQL(stmt.FromAlias, &b.argsBuffer)

	for _, join := range stmt.Joins {
		b.writeByte(' ')
		b.writeString(join.Type)
		b.writeByte(' ')
		b.renderSQL(join.Table, &b.argsBuffer)
		b.writeString(" AS ")
		b.renderSQL(join.Alias, &b.argsBuffer)
		b.writeString(" ON ")
		b.renderSQL(join.On, &b.argsBuffer)
	}

	if stmt.Where != nil {
		b.writeString(" WHERE ")
		b.renderSQL(*stmt.Where, &b.argsBuffer)
	}

	if len(stmt.GroupBy) > 0 {
		b.writeString(" GROUP BY ")
		for i, gb := range stmt.GroupBy {
			if i > 0 {
				b.writeString(", ")
			}
			b.renderSQL(gb, &b.argsBuffer)
		}
	}

	if stmt.Having != nil {
		b.writeString(" HAVING ")
		b.renderSQL(*stmt.Having, &b.argsBuffer)
	}

	if len(stmt.OrderBys) > 0 {
		b.writeString(" ORDER BY ")
		for i, ob := range stmt.OrderBys {
			if i > 0 {
				b.writeString(", ")
			}
			b.renderSQL(ob.Expr, &b.argsBuffer)

			if ob.Ascending {
				b.writeString(" ASC")
			} else {
				b.writeString(" DESC")
			}

			// PostgreSQL default: ASC → NULLS LAST, DESC → NULLS FIRST
			// Only render NULLS clause if it differs from the default
			if ob.Ascending && !ob.NullsLast {
				// ASC with NULLS FIRST (non-default)
				b.writeString(" NULLS FIRST")
			} else if !ob.Ascending && ob.NullsLast {
				// DESC with NULLS LAST (non-default)
				b.writeString(" NULLS LAST")
			}
			// Skip NULLS clause when it matches PostgreSQL default
		}
	}

	if stmt.Limit != nil {
		b.writeString(" LIMIT ")
		b.renderSQL(*stmt.Limit, &b.argsBuffer)
	}

	if stmt.Offset != nil {
		b.writeString(" OFFSET ")
		b.renderSQL(*stmt.Offset, &b.argsBuffer)
	}

	return b.sqlString(), b.argsBuffer
}

// renderSelect renders a CTE-based SELECT with JOIN for PostgreSQL
// Query format: WITH "$k" (col1, col2) AS (VALUES ($1, $2), ($3, $4))
//
//	SELECT table.* FROM table JOIN "$k" ON table.col1 = "$k".col1 AND table.col2 = "$k".col2
func (b *postgreSQLBackend) renderSelect(stmt SelectOp, chunk [][]any) (string, []any) {
	b.paramIndex = 0
	numKeyColumns := len(stmt.KeyColumns)
	b.resetArgsBuffer(len(chunk) * numKeyColumns)

	b.resetSQLBuffer(512)

	b.writeString("WITH \"$k\" (")
	for i, col := range stmt.KeyColumns {
		if i > 0 {
			b.writeString(", ")
		}
		b.quoteIdentifier(col)
	}
	b.writeString(") AS (VALUES ")

	for idx, row := range chunk {
		if idx > 0 {
			b.writeString(", ")
		}
		b.writeByte('(')
		for j := range stmt.KeyColumns {
			if j > 0 {
				b.writeString(", ")
			}
			b.paramIndex++
			if idx == 0 {
				// First row: use COALESCE to infer type from table schema
				b.writeString("COALESCE((NULL::")
				b.quoteTable(stmt.FromSchema, stmt.FromTable)
				b.writeString(").")
				b.quoteIdentifier(stmt.KeyColumns[j])
				b.writeString(", $")
				b.writeString(strconv.Itoa(b.paramIndex))
				b.writeByte(')')
			} else {
				// Subsequent rows: just use placeholder
				b.writeByte('$')
				b.writeString(strconv.Itoa(b.paramIndex))
			}
			b.argsBuffer = append(b.argsBuffer, row[j])
		}
		b.writeByte(')')
	}
	b.writeString(") ")

	b.writeString("SELECT ")
	for i, col := range stmt.Select {
		if i > 0 {
			b.writeString(", ")
		}
		b.quoteIdentifier(stmt.FromTable)
		b.writeByte('.')
		b.quoteIdentifier(col)
	}

	b.writeString(" FROM ")
	b.quoteTable(stmt.FromSchema, stmt.FromTable)

	b.writeString(" JOIN \"$k\" ON ")
	for j, col := range stmt.KeyColumns {
		if j > 0 {
			b.writeString(" AND ")
		}
		b.quoteIdentifier(stmt.FromTable)
		b.writeByte('.')
		b.quoteIdentifier(col)
		b.writeString(" = \"$k\".")
		b.quoteIdentifier(col)
	}

	return b.sqlString(), b.argsBuffer
}

// renderInsert renders a CTE-based INSERT SELECT pattern with RETURNING
// Query format: WITH new_rows AS (SELECT col1, col2 FROM table WHERE FALSE UNION ALL VALUES ($1, $2))
//
//	INSERT INTO table SELECT * FROM new_rows RETURNING ...
func (b *postgreSQLBackend) renderInsert(stmt InsertOp, chunk [][]any) (string, []any) {
	b.paramIndex = 0
	numColumns := len(stmt.Insert)
	b.resetArgsBuffer(len(chunk) * numColumns)

	b.resetSQLBuffer(512)

	b.writeString("WITH \"$r\" (")
	for i, col := range stmt.Insert {
		if i > 0 {
			b.writeString(", ")
		}
		b.quoteIdentifier(col)
	}
	b.writeString(") AS (VALUES ")

	for idx, row := range chunk {
		if idx > 0 {
			b.writeString(", ")
		}
		b.writeByte('(')
		for j := range stmt.Insert {
			if j > 0 {
				b.writeString(", ")
			}
			b.paramIndex++
			if idx == 0 {
				// First row: use COALESCE to infer type from table schema
				b.writeString("COALESCE((NULL::")
				b.quoteTable(stmt.IntoSchema, stmt.IntoTable)
				b.writeString(").")
				b.quoteIdentifier(stmt.Insert[j])
				b.writeString(", $")
				b.writeString(strconv.Itoa(b.paramIndex))
				b.writeByte(')')
			} else {
				// Subsequent rows: just use placeholder
				b.writeByte('$')
				b.writeString(strconv.Itoa(b.paramIndex))
			}
			b.argsBuffer = append(b.argsBuffer, row[j])
		}
		b.writeByte(')')
	}
	b.writeString(") ")

	b.writeString("INSERT INTO ")
	b.quoteTable(stmt.IntoSchema, stmt.IntoTable)
	b.writeString(" (")
	for i, col := range stmt.Insert {
		if i > 0 {
			b.writeString(", ")
		}
		b.quoteIdentifier(col)
	}
	b.writeString(") SELECT ")
	for i, col := range stmt.Insert {
		if i > 0 {
			b.writeString(", ")
		}
		b.quoteIdentifier(col)
	}
	b.writeString(" FROM \"$r\"")

	if len(stmt.Returning) > 0 {
		b.writeString(" RETURNING ")
		for i, col := range stmt.Returning {
			if i > 0 {
				b.writeString(", ")
			}
			b.quoteIdentifier(stmt.IntoTable)
			b.writeByte('.')
			b.quoteIdentifier(col)
		}
	}

	return b.sqlString(), b.argsBuffer
}

// renderUpdate renders a CTE-based UPDATE FROM pattern with UNION for type inference
// Query format: WITH "$v" AS (SELECT ... FROM table WHERE FALSE UNION ALL VALUES (...))
//
//	UPDATE table SET col = "$v".col FROM "$v" WHERE table.id = "$v".id
func (b *postgreSQLBackend) renderUpdate(stmt UpdateOp, setChunk, whereChunk [][]any) (string, []any) {
	b.paramIndex = 0

	numColumns := len(stmt.Sets) + len(stmt.Where)
	b.resetArgsBuffer(len(setChunk) * numColumns)

	b.resetSQLBuffer(512)

	b.writeString("WITH \"$v\" (")
	for i, col := range stmt.Sets {
		if i > 0 {
			b.writeString(", ")
		}
		b.quoteIdentifier(col)
	}
	for _, col := range stmt.Where {
		b.writeString(", ")
		b.quoteIdentifier(col)
	}
	b.writeString(") AS (VALUES ")

	for idx := range setChunk {
		if idx > 0 {
			b.writeString(", ")
		}
		b.writeByte('(')
		for j, val := range setChunk[idx] {
			if j > 0 {
				b.writeString(", ")
			}
			b.paramIndex++
			if idx == 0 {
				// First row: use COALESCE to infer type from table schema
				b.writeString("COALESCE((NULL::")
				b.quoteTable(stmt.Schema, stmt.Table)
				b.writeString(").")
				b.quoteIdentifier(stmt.Sets[j])
				b.writeString(", $")
				b.writeString(strconv.Itoa(b.paramIndex))
				b.writeByte(')')
			} else {
				// Subsequent rows: just use placeholder
				b.writeByte('$')
				b.writeString(strconv.Itoa(b.paramIndex))
			}
			b.argsBuffer = append(b.argsBuffer, val)
		}
		for j, val := range whereChunk[idx] {
			b.writeString(", ")
			b.paramIndex++
			if idx == 0 {
				// First row: use COALESCE to infer type from table schema
				b.writeString("COALESCE((NULL::")
				b.quoteTable(stmt.Schema, stmt.Table)
				b.writeString(").")
				b.quoteIdentifier(stmt.Where[j])
				b.writeString(", $")
				b.writeString(strconv.Itoa(b.paramIndex))
				b.writeByte(')')
			} else {
				// Subsequent rows: just use placeholder
				b.writeByte('$')
				b.writeString(strconv.Itoa(b.paramIndex))
			}
			b.argsBuffer = append(b.argsBuffer, val)
		}
		b.writeByte(')')
	}
	b.writeString(") ")

	b.writeString("UPDATE ")
	b.quoteTable(stmt.Schema, stmt.Table)
	b.writeString(" SET ")
	for i, col := range stmt.Sets {
		if i > 0 {
			b.writeString(", ")
		}
		b.quoteIdentifier(col)
		b.writeString(" = \"$v\".")
		b.quoteIdentifier(col)
	}

	b.writeString(" FROM \"$v\"")

	b.writeString(" WHERE ")
	for i, col := range stmt.Where {
		if i > 0 {
			b.writeString(" AND ")
		}
		b.quoteIdentifier(stmt.Table)
		b.writeByte('.')
		b.quoteIdentifier(col)
		b.writeString(" = \"$v\".")
		b.quoteIdentifier(col)
	}

	return b.sqlString(), b.argsBuffer
}

// renderDelete renders a CTE-based DELETE with UNION pattern
// Query format: WITH keys AS (SELECT id FROM table WHERE FALSE UNION ALL VALUES ($1), ($2))
//
//	DELETE FROM table WHERE id IN (SELECT id FROM keys)
func (b *postgreSQLBackend) renderDelete(stmt DeleteOp, chunk [][]any) (string, []any) {
	b.paramIndex = 0
	numKeyColumns := len(stmt.KeyColumns)
	b.resetArgsBuffer(len(chunk) * numKeyColumns)

	b.resetSQLBuffer(512)

	b.writeString("WITH \"$k\" (")
	for i, col := range stmt.KeyColumns {
		if i > 0 {
			b.writeString(", ")
		}
		b.quoteIdentifier(col)
	}
	b.writeString(") AS (VALUES ")

	for idx, row := range chunk {
		if idx > 0 {
			b.writeString(", ")
		}
		b.writeByte('(')
		for j := range stmt.KeyColumns {
			if j > 0 {
				b.writeString(", ")
			}
			b.paramIndex++
			if idx == 0 {
				// First row: use COALESCE to infer type from table schema
				b.writeString("COALESCE((NULL::")
				b.quoteTable(stmt.FromSchema, stmt.FromTable)
				b.writeString(").")
				b.quoteIdentifier(stmt.KeyColumns[j])
				b.writeString(", $")
				b.writeString(strconv.Itoa(b.paramIndex))
				b.writeByte(')')
			} else {
				// Subsequent rows: just use placeholder
				b.writeByte('$')
				b.writeString(strconv.Itoa(b.paramIndex))
			}
			b.argsBuffer = append(b.argsBuffer, row[j])
		}
		b.writeByte(')')
	}
	b.writeString(") ")

	b.writeString("DELETE FROM ")
	b.quoteTable(stmt.FromSchema, stmt.FromTable)
	b.writeString(" WHERE ")

	if numKeyColumns == 1 {
		b.quoteIdentifier(stmt.KeyColumns[0])
		b.writeString(" IN (SELECT ")
		b.quoteIdentifier(stmt.KeyColumns[0])
		b.writeString(" FROM \"$k\")")
	} else {
		b.writeByte('(')
		for i, col := range stmt.KeyColumns {
			if i > 0 {
				b.writeString(", ")
			}
			b.quoteIdentifier(col)
		}
		b.writeString(") IN (SELECT ")
		for i, col := range stmt.KeyColumns {
			if i > 0 {
				b.writeString(", ")
			}
			b.quoteIdentifier(col)
		}
		b.writeString(" FROM \"$k\")")
	}

	return b.sqlString(), b.argsBuffer
}

// renderRawSQL renders a raw SQL fragment (for FetchRaw)
func (b *postgreSQLBackend) renderRawSQL(fragment sqlast.SQL) (string, []any) {
	b.paramIndex = 0
	b.resetArgsBuffer(16)
	b.resetSQLBuffer(512)
	b.renderSQL(fragment, &b.argsBuffer)
	return b.sqlString(), b.argsBuffer
}

func (b *postgreSQLBackend) Select(ctx context.Context, stmt SelectOp) (Rows, error) {
	if len(stmt.Keys) == 0 {
		return nil, nil
	}

	// Convert Keys to [][]any for rendering
	values := make([][]any, len(stmt.Keys))
	for i, k := range stmt.Keys {
		values[i] = make([]any, k.Length())
		for j := 0; j < k.Length(); j++ {
			values[i][j] = k.At(j)
		}
	}

	query, args := b.renderSelect(stmt, values)
	return b.queryContext(ctx, query, args...)
}

func (b *postgreSQLBackend) Insert(ctx context.Context, stmt InsertOp) (Rows, error) {
	if len(stmt.Values) == 0 {
		return nil, nil
	}

	query, args := b.renderInsert(stmt, stmt.Values)
	return b.queryContext(ctx, query, args...)
}

func (b *postgreSQLBackend) Update(ctx context.Context, stmt UpdateOp) error {
	if len(stmt.SetValues) == 0 {
		return nil
	}

	query, args := b.renderUpdate(stmt, stmt.SetValues, stmt.WhereValues)
	_, err := b.execContext(ctx, query, args...)
	return err
}

func (b *postgreSQLBackend) Delete(ctx context.Context, stmt DeleteOp) (int64, error) {
	if len(stmt.Keys) == 0 {
		return 0, nil
	}

	// Convert Keys to [][]any for rendering
	values := make([][]any, len(stmt.Keys))
	for i, k := range stmt.Keys {
		values[i] = make([]any, k.Length())
		for j := 0; j < k.Length(); j++ {
			values[i][j] = k.At(j)
		}
	}

	query, args := b.renderDelete(stmt, values)

	result, err := b.execContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	affected, _ := result.RowsAffected()
	return affected, nil
}

func (b *postgreSQLBackend) FetchQuery(ctx context.Context, stmt sqlast.SQLQuery) (Rows, error) {
	query, args := b.renderSQLQuery(stmt)
	return b.queryContext(ctx, query, args...)
}

func (b *postgreSQLBackend) CountQuery(ctx context.Context, stmt sqlast.SQLQuery) (int64, error) {
	innerStmt := stmt
	innerStmt.Limit = nil
	innerStmt.Offset = nil

	innerQuery, args := b.renderSQLQuery(innerStmt)
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS count_subquery", innerQuery)

	var count int64
	err := b.queryRowContext(ctx, countQuery, args...).Scan(&count)
	return count, err
}

func (b *postgreSQLBackend) FetchRaw(ctx context.Context, fragment sqlast.SQL) (Rows, error) {
	query, args := b.renderRawSQL(fragment)
	return b.queryContext(ctx, query, args...)
}

func (b *postgreSQLBackend) ExecRaw(ctx context.Context, fragment sqlast.SQL) (int64, error) {
	query, args := b.renderRawSQL(fragment)

	result, err := b.execContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (b *postgreSQLBackend) Begin(ctx context.Context) error {
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	b.tx = tx
	return nil
}

func (b *postgreSQLBackend) Commit(ctx context.Context) error {
	if b.tx == nil {
		return fmt.Errorf("no transaction to commit")
	}
	err := b.tx.Commit()
	b.tx = nil
	return err
}

func (b *postgreSQLBackend) Rollback(ctx context.Context) error {
	if b.tx == nil {
		return fmt.Errorf("no transaction to rollback")
	}
	err := b.tx.Rollback()
	b.tx = nil
	return err
}
