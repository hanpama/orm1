package driver

import (
	"context"
	"database/sql"
	"fmt"

	sqlast "github.com/hanpama/orm1/sql"
)

// sqliteBackend implements Backend for SQLite databases.
type sqliteBackend struct {
	db         *sql.DB
	tx         *sql.Tx
	paramIndex int
	argsBuffer []any  // Reusable buffer for query parameters
	sqlBuffer  []byte // Reusable buffer for SQL generation
}

// newSQLiteBackend creates a new SQLite backend.
func newSQLiteBackend(db *sql.DB) Backend {
	return &sqliteBackend{
		db: db,
	}
}

func (d *sqliteDriver) Close() error {
	return d.db.Close()
}

// resetArgsBuffer resets the argument buffer to length 0, growing capacity if needed.
// Uses 2x growth factor to reduce reallocation frequency.
func (b *sqliteBackend) resetArgsBuffer(minCapacity int) {
	if cap(b.argsBuffer) < minCapacity {
		b.argsBuffer = make([]any, 0, minCapacity*2)
	} else {
		b.argsBuffer = b.argsBuffer[:0]
	}
}

// resetSQLBuffer resets the SQL buffer to length 0, growing capacity if needed.
// Uses 2x growth factor to reduce reallocation frequency.
func (b *sqliteBackend) resetSQLBuffer(minCapacity int) {
	b.sqlBuffer = b.sqlBuffer[:0]
	if cap(b.sqlBuffer) < minCapacity {
		b.sqlBuffer = make([]byte, 0, minCapacity*2)
	}
}

// writeString appends a string to the SQL buffer.
func (b *sqliteBackend) writeString(s string) {
	b.sqlBuffer = append(b.sqlBuffer, s...)
}

// writeByte appends a byte to the SQL buffer.
func (b *sqliteBackend) writeByte(c byte) {
	b.sqlBuffer = append(b.sqlBuffer, c)
}

// quoteIdentifier quotes a SQL identifier by escaping " as "" and wrapping in "
func (b *sqliteBackend) quoteIdentifier(identifier string) {
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

// sqlString returns the current SQL buffer as a string.
func (b *sqliteBackend) sqlString() string {
	return string(b.sqlBuffer)
}

// queryContext executes a query using either the transaction or the database connection.
func (b *sqliteBackend) queryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if b.tx != nil {
		return b.tx.QueryContext(ctx, query, args...)
	}
	return b.db.QueryContext(ctx, query, args...)
}

// execContext executes a command using either the transaction or the database connection.
func (b *sqliteBackend) execContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if b.tx != nil {
		return b.tx.ExecContext(ctx, query, args...)
	}
	return b.db.ExecContext(ctx, query, args...)
}

// queryRowContext executes a query that returns at most one row using either the transaction or the database connection.
func (b *sqliteBackend) queryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if b.tx != nil {
		return b.tx.QueryRowContext(ctx, query, args...)
	}
	return b.db.QueryRowContext(ctx, query, args...)
}

// sqliteDriver implements Driver for SQLite.
type sqliteDriver struct {
	db *sql.DB
}

// NewSQLite creates a Driver for SQLite.
// This driver creates a new backend instance for each session.
func NewSQLite(db *sql.DB) Driver {
	return &sqliteDriver{db: db}
}

// CreateBackend creates a new SQLite backend instance.
func (d *sqliteDriver) CreateBackend() Backend {
	return newSQLiteBackend(d.db)
}

// renderSQL renders a SQL AST node to the SQL buffer and appends parameters to args
func (b *sqliteBackend) renderSQL(sql sqlast.SQL, args *[]any) {
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
		b.writeByte('?')
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
func (b *sqliteBackend) renderSQLQuery(stmt sqlast.SQLQuery) (string, []any) {
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

			// SQLite default: ASC → NULLS FIRST, DESC → NULLS LAST (opposite of PostgreSQL)
			// Always render NULLS clause to match PostgreSQL/Oracle standard behavior
			if ob.NullsLast {
				b.writeString(" NULLS LAST")
			} else {
				b.writeString(" NULLS FIRST")
			}
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

// renderSelect renders a CTE-based SELECT with JOIN pattern
// Query format: WITH keys (col1, col2) AS (VALUES (?,?), (?,?)) SELECT table.col1, table.col2 FROM table JOIN keys ON ...
func (b *sqliteBackend) renderSelect(stmt SelectOp, chunk [][]any) (string, []any) {
	b.paramIndex = 0
	numKeyColumns := len(stmt.KeyColumns)
	b.resetArgsBuffer(len(chunk) * numKeyColumns)

	b.resetSQLBuffer(512)

	b.writeString("WITH keys (")
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
			b.writeByte('?')
			b.paramIndex++
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
	b.quoteIdentifier(stmt.FromTable)

	b.writeString(" JOIN keys ON ")
	for j := range stmt.KeyColumns {
		if j > 0 {
			b.writeString(" AND ")
		}
		b.quoteIdentifier(stmt.FromTable)
		b.writeByte('.')
		b.quoteIdentifier(stmt.KeyColumns[j])
		b.writeString(" = keys.")
		b.quoteIdentifier(stmt.KeyColumns[j])
	}

	return b.sqlString(), b.argsBuffer
}

// renderInsert renders a CTE-based INSERT SELECT pattern with RETURNING
// Query format: WITH new_rows (col1, col2) AS (VALUES (?,?), (?,?)) INSERT INTO table SELECT ... FROM new_rows RETURNING ...
func (b *sqliteBackend) renderInsert(stmt InsertOp, chunk [][]any) (string, []any) {
	b.paramIndex = 0
	numColumns := len(stmt.Insert)
	b.resetArgsBuffer(len(chunk) * numColumns)

	b.resetSQLBuffer(512)

	b.writeString("WITH new_rows (")
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
			b.writeByte('?')
			b.paramIndex++
			b.argsBuffer = append(b.argsBuffer, row[j])
		}
		b.writeByte(')')
	}
	b.writeString(") ")

	b.writeString("INSERT INTO ")
	b.quoteIdentifier(stmt.IntoTable)
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
	b.writeString(" FROM new_rows")

	if len(stmt.Returning) > 0 {
		b.writeString(" RETURNING ")
		for i, col := range stmt.Returning {
			if i > 0 {
				b.writeString(", ")
			}
			b.quoteIdentifier(col)
		}
	}

	return b.sqlString(), b.argsBuffer
}

// renderUpdate renders a CTE-based UPDATE FROM pattern
// Query format: WITH new_values (id, name, email) AS (VALUES (?,?,?), (?,?,?))
//
//	UPDATE table SET name = new_values.name FROM new_values WHERE table.id = new_values.id
func (b *sqliteBackend) renderUpdate(stmt UpdateOp, setChunk, whereChunk [][]any) (string, []any) {
	b.paramIndex = 0

	numColumns := len(stmt.Sets) + len(stmt.Where)
	b.resetArgsBuffer(len(setChunk) * numColumns)

	b.resetSQLBuffer(512)

	b.writeString("WITH new_values (")
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
			b.writeByte('?')
			b.paramIndex++
			b.argsBuffer = append(b.argsBuffer, val)
		}
		for _, val := range whereChunk[idx] {
			b.writeString(", ")
			b.writeByte('?')
			b.paramIndex++
			b.argsBuffer = append(b.argsBuffer, val)
		}
		b.writeByte(')')
	}
	b.writeString(") ")

	b.writeString("UPDATE ")
	b.quoteIdentifier(stmt.Table)
	b.writeString(" SET ")
	for i, col := range stmt.Sets {
		if i > 0 {
			b.writeString(", ")
		}
		b.quoteIdentifier(col)
		b.writeString(" = new_values.")
		b.quoteIdentifier(col)
	}

	b.writeString(" FROM new_values")

	b.writeString(" WHERE ")
	for i, col := range stmt.Where {
		if i > 0 {
			b.writeString(" AND ")
		}
		b.quoteIdentifier(stmt.Table)
		b.writeByte('.')
		b.quoteIdentifier(col)
		b.writeString(" = new_values.")
		b.quoteIdentifier(col)
	}

	return b.sqlString(), b.argsBuffer
}

// renderDelete renders a CTE-based DELETE with subquery pattern
// Query format: WITH keys (id) AS (VALUES (?), (?)) DELETE FROM table WHERE id IN (SELECT id FROM keys)
func (b *sqliteBackend) renderDelete(stmt DeleteOp, chunk [][]any) (string, []any) {
	b.paramIndex = 0
	numKeyColumns := len(stmt.KeyColumns)
	b.resetArgsBuffer(len(chunk) * numKeyColumns)

	b.resetSQLBuffer(512)

	b.writeString("WITH keys (")
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
			b.writeByte('?')
			b.paramIndex++
			b.argsBuffer = append(b.argsBuffer, row[j])
		}
		b.writeByte(')')
	}
	b.writeString(") ")

	b.writeString("DELETE FROM ")
	b.quoteIdentifier(stmt.FromTable)
	b.writeString(" WHERE ")

	if numKeyColumns == 1 {
		b.quoteIdentifier(stmt.KeyColumns[0])
		b.writeString(" IN (SELECT ")
		b.quoteIdentifier(stmt.KeyColumns[0])
		b.writeString(" FROM keys)")
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
		b.writeString(" FROM keys)")
	}

	return b.sqlString(), b.argsBuffer
}

// renderRawSQL renders a raw SQL fragment (for FetchRaw)
func (b *sqliteBackend) renderRawSQL(fragment sqlast.SQL) (string, []any) {
	b.paramIndex = 0
	b.resetArgsBuffer(16)
	b.resetSQLBuffer(512)
	b.renderSQL(fragment, &b.argsBuffer)
	return b.sqlString(), b.argsBuffer
}

func (b *sqliteBackend) Select(ctx context.Context, stmt SelectOp) (Rows, error) {
	if len(stmt.Keys) == 0 {
		// Return empty result set for empty keys
		return &emptyRows{}, nil
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

func (b *sqliteBackend) Insert(ctx context.Context, stmt InsertOp) (Rows, error) {
	if len(stmt.Values) == 0 {
		// Return empty result set for empty batch
		return &emptyRows{}, nil
	}
	query, args := b.renderInsert(stmt, stmt.Values)
	return b.queryContext(ctx, query, args...)
}

func (b *sqliteBackend) Update(ctx context.Context, stmt UpdateOp) error {
	if len(stmt.SetValues) == 0 {
		// No-op for empty batch
		return nil
	}
	query, args := b.renderUpdate(stmt, stmt.SetValues, stmt.WhereValues)
	_, err := b.execContext(ctx, query, args...)
	return err
}

func (b *sqliteBackend) Delete(ctx context.Context, stmt DeleteOp) error {
	if len(stmt.Keys) == 0 {
		// No-op for empty batch
		return nil
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

	_, err := b.execContext(ctx, query, args...)
	return err
}

func (b *sqliteBackend) FetchQuery(ctx context.Context, stmt sqlast.SQLQuery) (Rows, error) {
	query, args := b.renderSQLQuery(stmt)
	return b.queryContext(ctx, query, args...)
}

func (b *sqliteBackend) CountQuery(ctx context.Context, stmt sqlast.SQLQuery) (int64, error) {
	innerStmt := stmt
	innerStmt.Limit = nil
	innerStmt.Offset = nil

	innerQuery, args := b.renderSQLQuery(innerStmt)
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS count_subquery", innerQuery)

	var count int64
	err := b.queryRowContext(ctx, countQuery, args...).Scan(&count)
	return count, err
}

func (b *sqliteBackend) FetchRaw(ctx context.Context, fragment sqlast.SQL) (Rows, error) {
	query, args := b.renderRawSQL(fragment)
	return b.queryContext(ctx, query, args...)
}

func (b *sqliteBackend) ExecRaw(ctx context.Context, fragment sqlast.SQL) (int64, error) {
	query, args := b.renderRawSQL(fragment)

	result, err := b.execContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (b *sqliteBackend) Begin(ctx context.Context) error {
	return b.BeginTx(ctx, nil)
}

func (b *sqliteBackend) BeginTx(ctx context.Context, opts *TxOptions) error {
	// SQLite only supports SERIALIZABLE isolation level
	// ReadOnly is not enforced by SQLite
	var sqlOpts *sql.TxOptions
	if opts != nil {
		sqlOpts = &sql.TxOptions{
			Isolation: convertToSQLIsolationLevel(opts.Isolation),
			ReadOnly:  opts.ReadOnly,
		}
	}
	tx, err := b.db.BeginTx(ctx, sqlOpts)
	if err != nil {
		return err
	}
	b.tx = tx
	return nil
}

func (b *sqliteBackend) Commit(ctx context.Context) error {
	if b.tx == nil {
		return fmt.Errorf("no transaction to commit")
	}
	err := b.tx.Commit()
	b.tx = nil
	return err
}

func (b *sqliteBackend) Rollback(ctx context.Context) error {
	if b.tx == nil {
		return fmt.Errorf("no transaction to rollback")
	}
	err := b.tx.Rollback()
	b.tx = nil
	return err
}

func (b *sqliteBackend) Savepoint(ctx context.Context, name string) error {
	if b.tx == nil {
		return fmt.Errorf("no transaction to create savepoint")
	}
	query := fmt.Sprintf("SAVEPOINT %s", name)
	_, err := b.tx.ExecContext(ctx, query)
	return err
}

func (b *sqliteBackend) ReleaseSavepoint(ctx context.Context, name string) error {
	if b.tx == nil {
		return fmt.Errorf("no transaction to release savepoint")
	}
	query := fmt.Sprintf("RELEASE SAVEPOINT %s", name)
	_, err := b.tx.ExecContext(ctx, query)
	return err
}

func (b *sqliteBackend) RollbackToSavepoint(ctx context.Context, name string) error {
	if b.tx == nil {
		return fmt.Errorf("no transaction to rollback savepoint")
	}
	query := fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", name)
	_, err := b.tx.ExecContext(ctx, query)
	return err
}
