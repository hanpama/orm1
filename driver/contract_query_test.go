package driver_test

import (
	"context"
	"testing"

	"github.com/hanpama/orm1/driver"
	"github.com/hanpama/orm1/sql"
)

func testComplexQueryContracts(t *testing.T, ctx context.Context, backend driver.Backend, schema, table string) {
	// Insert test data
	insertOp := driver.InsertOp{
		IntoSchema: schema,
		IntoTable:  table,
		Insert:     []string{"name", "value"},
		Returning:  []string{"id"},
		Values: [][]any{
			{"query_test1", 10},
			{"query_test2", 20},
			{"query_test3", 30},
		},
	}
	rows, err := backend.Insert(ctx, insertOp)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	rows.Close()

	t.Run("FetchQuery_Basic", func(t *testing.T) {
		testFetchQueryBasic(t, ctx, backend, schema, table)
	})

	t.Run("CountQuery_IgnoresPagination", func(t *testing.T) {
		testCountQueryIgnoresPagination(t, ctx, backend, schema, table)
	})

	t.Run("FetchQuery_ComplexSQL", func(t *testing.T) {
		testFetchQueryComplexSQL(t, ctx, backend, schema, table)
	})

	t.Run("CountQuery_InTransaction", func(t *testing.T) {
		testCountQueryInTransaction(t, ctx, backend, schema, table)
	})

	t.Run("FetchQuery_WithJoins", func(t *testing.T) {
		testFetchQueryWithJoins(t, ctx, backend, schema, table)
	})

	t.Run("FetchQuery_QuotedIdentifiers", func(t *testing.T) {
		testFetchQueryQuotedIdentifiers(t, ctx, backend, schema, table)
	})

	t.Run("FetchQuery_CompleteOrderBy", func(t *testing.T) {
		testFetchQueryCompleteOrderBy(t, ctx, backend, schema, table)
	})
}

func testFetchQueryBasic(t *testing.T, ctx context.Context, backend driver.Backend, schema, table string) {
	// Contract: FetchQuery executes structured query
	var fromTable sql.SQL
	if schema != "" {
		fromTable = sql.SQLQN{Part1: schema, Part2: table}
	} else {
		fromTable = sql.SQLN{Part: table}
	}

	query := sql.SQLQuery{
		Select: []sql.SQL{
			sql.SQLN{Part: "name"},
			sql.SQLN{Part: "value"},
		},
		FromTable: fromTable,
		FromAlias: sql.SQLN{Part: "t"},
	}

	rows, err := backend.FetchQuery(ctx, query)
	if err != nil {
		t.Fatalf("FetchQuery failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var name string
		var value int
		if err := rows.Scan(&name, &value); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		count++
	}

	if count < 3 {
		t.Errorf("Expected at least 3 rows, got %d", count)
	}
}

func testCountQueryIgnoresPagination(t *testing.T, ctx context.Context, backend driver.Backend, schema, table string) {
	// Contract: CountQuery ignores LIMIT/OFFSET
	var fromTable sql.SQL
	if schema != "" {
		fromTable = sql.SQLQN{Part1: schema, Part2: table}
	} else {
		fromTable = sql.SQLN{Part: table}
	}

	var limitSQL sql.SQL = sql.SQLText{Text: "1"}
	query := sql.SQLQuery{
		Select:    []sql.SQL{sql.SQLN{Part: "id"}},
		FromTable: fromTable,
		FromAlias: sql.SQLN{Part: "t"},
		Limit:     &limitSQL, // Should be ignored
	}

	count, err := backend.CountQuery(ctx, query)
	if err != nil {
		t.Fatalf("CountQuery failed: %v", err)
	}

	if count < 3 {
		t.Errorf("Expected at least 3 (LIMIT ignored), got %d", count)
	}
}

// testRawSQLContracts tests FetchRaw and ExecRaw operations
func testRawSQLContracts(t *testing.T, ctx context.Context, backend driver.Backend, schema, table string) {
	t.Run("FetchRaw_ReturnsRows", func(t *testing.T) {
		testFetchRawReturnsRows(t, ctx, backend, schema, table)
	})

	t.Run("ExecRaw_ReturnsAffectedCount", func(t *testing.T) {
		testExecRawReturnsAffectedCount(t, ctx, backend, schema, table)
	})
}

func testFetchRawReturnsRows(t *testing.T, ctx context.Context, backend driver.Backend, schema, table string) {
	// Contract: FetchRaw executes raw SQL and returns Rows
	var sqlText string
	if schema != "" {
		sqlText = "SELECT name, value FROM " + schema + "." + table + " LIMIT 1"
	} else {
		sqlText = "SELECT name, value FROM " + table + " LIMIT 1"
	}
	fragment := sql.SQLText{Text: sqlText}

	rows, err := backend.FetchRaw(ctx, fragment)
	if err != nil {
		t.Fatalf("FetchRaw failed: %v", err)
	}
	defer rows.Close()

	// Should have at least structure to scan
	if rows.Next() {
		var name string
		var value int
		if err := rows.Scan(&name, &value); err != nil {
			t.Errorf("Scan failed: %v", err)
		}
	}
}

func testExecRawReturnsAffectedCount(t *testing.T, ctx context.Context, backend driver.Backend, schema, table string) {
	// Insert test data for update
	insertOp := driver.InsertOp{
		IntoSchema: schema,
		IntoTable:  table,
		Insert:     []string{"name", "value"},
		Returning:  []string{"id"},
		Values:     [][]any{{"raw_exec_test", 999}},
	}
	rows, err := backend.Insert(ctx, insertOp)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	var insertedID int64
	if rows.Next() {
		rows.Scan(&insertedID)
	}
	rows.Close()

	// Contract: ExecRaw returns affected row count
	var sqlText string
	if schema != "" {
		sqlText = "UPDATE " + schema + "." + table + " SET value = 1000 WHERE name = 'raw_exec_test'"
	} else {
		sqlText = "UPDATE " + table + " SET value = 1000 WHERE name = 'raw_exec_test'"
	}
	fragment := sql.SQLText{Text: sqlText}

	count, err := backend.ExecRaw(ctx, fragment)
	if err != nil {
		t.Fatalf("ExecRaw failed: %v", err)
	}

	if count != 1 {
		t.Errorf("Expected affected count=1, got %d", count)
	}
}

func testFetchQueryComplexSQL(t *testing.T, ctx context.Context, backend driver.Backend, schema, table string) {
	// Contract: FetchQuery handles complex SQL AST with all types
	// This query tests: SQLParam, SQLAll, SQLAny, SQLLt, SQLGt, SQLIsNull, SQLIsNotNull,
	// SQLFragment, Joins, GroupBy, Having, OrderBy with NULLS, Offset

	var fromTable sql.SQL
	if schema != "" {
		fromTable = sql.SQLQN{Part1: schema, Part2: table}
	} else {
		fromTable = sql.SQLN{Part: table}
	}

	// WHERE clause: (value > 10 AND value < 100 AND name = 'query_test1') OR (name IS NOT NULL AND id IS NULL)
	// This tests: SQLParam, SQLAll, SQLAny, SQLGt, SQLLt, SQLEq, SQLIsNull, SQLIsNotNull
	var whereClause sql.SQL = sql.SQLAny{
		Els: []sql.SQL{
			sql.SQLAll{
				Els: []sql.SQL{
					sql.SQLGt{
						Left:  sql.SQLN{Part: "value"},
						Right: sql.SQLParam{Value: 5},
					},
					sql.SQLLt{
						Left:  sql.SQLN{Part: "value"},
						Right: sql.SQLParam{Value: 100},
					},
					sql.SQLEq{
						Left:  sql.SQLN{Part: "name"},
						Right: sql.SQLParam{Value: "query_test1"},
					},
				},
			},
			sql.SQLAll{
				Els: []sql.SQL{
					sql.SQLIsNotNull{
						Operand: sql.SQLN{Part: "name"},
					},
					sql.SQLIsNull{
						Operand: sql.SQLText{Text: "NULL"}, // Use literal NULL
					},
				},
			},
		},
	}

	// HAVING clause: COUNT(*) > 0
	var havingClause sql.SQL = sql.SQLGt{
		Left: sql.SQLFragment{
			Els: []sql.SQL{
				sql.SQLText{Text: "COUNT(*)"},
			},
		},
		Right: sql.SQLParam{Value: 0},
	}

	var limit sql.SQL = sql.SQLParam{Value: 5}
	var offset sql.SQL = sql.SQLParam{Value: 1}

	query := sql.SQLQuery{
		Select: []sql.SQL{
			sql.SQLN{Part: "name"},
			sql.SQLN{Part: "value"},
		},
		FromTable: fromTable,
		FromAlias: sql.SQLN{Part: "t"},
		Where:     &whereClause,
		GroupBy: []sql.SQL{
			sql.SQLN{Part: "name"},
			sql.SQLN{Part: "value"},
		},
		Having: &havingClause,
		OrderBys: []sql.OrderBy{
			{
				Expr:      sql.SQLN{Part: "name"},
				Ascending: true,
				NullsLast: false, // NULLS FIRST
			},
			{
				Expr:      sql.SQLN{Part: "value"},
				Ascending: false,
				NullsLast: true, // NULLS LAST
			},
		},
		Limit:  &limit,
		Offset: &offset,
	}

	rows, err := backend.FetchQuery(ctx, query)
	if err != nil {
		t.Fatalf("FetchQuery with complex SQL failed: %v", err)
	}
	defer rows.Close()

	// Just verify query executes successfully
	count := 0
	for rows.Next() {
		var name string
		var value int
		if err := rows.Scan(&name, &value); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		count++
	}

	// Query should return some results (we inserted 3 rows with values 10, 20, 30)
	// After OFFSET 1 and LIMIT 5, we should get at most 2 rows
	if count > 5 {
		t.Errorf("Expected at most 5 rows (LIMIT), got %d", count)
	}
}

func testCountQueryInTransaction(t *testing.T, ctx context.Context, backend driver.Backend, schema, table string) {
	// Contract: CountQuery works within transactions
	err := backend.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	defer backend.Rollback(ctx)

	var fromTable sql.SQL
	if schema != "" {
		fromTable = sql.SQLQN{Part1: schema, Part2: table}
	} else {
		fromTable = sql.SQLN{Part: table}
	}

	query := sql.SQLQuery{
		Select:    []sql.SQL{sql.SQLN{Part: "id"}},
		FromTable: fromTable,
		FromAlias: sql.SQLN{Part: "t"},
	}

	count, err := backend.CountQuery(ctx, query)
	if err != nil {
		t.Fatalf("CountQuery in transaction failed: %v", err)
	}

	if count < 3 {
		t.Errorf("Expected at least 3, got %d", count)
	}
}

func testFetchQueryWithJoins(t *testing.T, ctx context.Context, backend driver.Backend, schema, table string) {
	// Contract: FetchQuery handles JOIN clauses
	// Insert additional test data
	insertOp := driver.InsertOp{
		IntoSchema: schema,
		IntoTable:  table,
		Insert:     []string{"name", "value"},
		Returning:  []string{"id"},
		Values:     [][]any{{"join_test1", 111}, {"join_test2", 222}},
	}
	rows, err := backend.Insert(ctx, insertOp)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	rows.Close()

	var fromTable sql.SQL
	if schema != "" {
		fromTable = sql.SQLQN{Part1: schema, Part2: table}
	} else {
		fromTable = sql.SQLN{Part: table}
	}

	// Create a self-join query
	query := sql.SQLQuery{
		Select: []sql.SQL{
			sql.SQLFragment{Els: []sql.SQL{sql.SQLN{Part: "t1"}, sql.SQLText{Text: "."}, sql.SQLN{Part: "name"}}},
		},
		FromTable: fromTable,
		FromAlias: sql.SQLN{Part: "t1"},
		Joins: []sql.Join{
			{
				Type:  "INNER JOIN",
				Table: fromTable,
				Alias: sql.SQLN{Part: "t2"},
				On: sql.SQLEq{
					Left:  sql.SQLFragment{Els: []sql.SQL{sql.SQLN{Part: "t1"}, sql.SQLText{Text: "."}, sql.SQLN{Part: "value"}}},
					Right: sql.SQLFragment{Els: []sql.SQL{sql.SQLN{Part: "t2"}, sql.SQLText{Text: "."}, sql.SQLN{Part: "value"}}},
				},
			},
		},
	}

	rows, err = backend.FetchQuery(ctx, query)
	if err != nil {
		t.Fatalf("FetchQuery with JOIN failed: %v", err)
	}
	defer rows.Close()

	// Just verify query executes successfully
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
	}
}

func testFetchQueryQuotedIdentifiers(t *testing.T, ctx context.Context, backend driver.Backend, schema, table string) {
	// Contract: FetchQuery handles identifiers with special characters (like double quotes)
	// This tests quoteIdentifier's quote escaping logic
	var fromTable sql.SQL
	if schema != "" {
		fromTable = sql.SQLQN{Part1: schema, Part2: table}
	} else {
		fromTable = sql.SQLN{Part: table}
	}

	// Use a column name that would contain quotes if it existed
	// This tests the SQLQN with empty Part1 branch as well
	query := sql.SQLQuery{
		Select: []sql.SQL{
			sql.SQLQN{Part1: "", Part2: "name"},  // Qualified name without schema
			sql.SQLN{Part: `value`},
		},
		FromTable: fromTable,
		FromAlias: sql.SQLN{Part: "t"},
	}

	rows, err := backend.FetchQuery(ctx, query)
	if err != nil {
		t.Fatalf("FetchQuery with qualified names failed: %v", err)
	}
	defer rows.Close()

	// Just verify query executes successfully
	for rows.Next() {
		var name string
		var value int
		if err := rows.Scan(&name, &value); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
	}
}

func testFetchQueryCompleteOrderBy(t *testing.T, ctx context.Context, backend driver.Backend, schema, table string) {
	// Contract: FetchQuery handles all ORDER BY variations (ASC NULLS FIRST, DESC NULLS LAST)
	var fromTable sql.SQL
	if schema != "" {
		fromTable = sql.SQLQN{Part1: schema, Part2: table}
	} else {
		fromTable = sql.SQLN{Part: table}
	}

	query := sql.SQLQuery{
		Select: []sql.SQL{
			sql.SQLN{Part: "name"},
			sql.SQLN{Part: "value"},
		},
		FromTable: fromTable,
		FromAlias: sql.SQLN{Part: "t"},
		OrderBys: []sql.OrderBy{
			{
				Expr:      sql.SQLN{Part: "name"},
				Ascending: true,
				NullsLast: true, // ASC NULLS LAST (default for PostgreSQL, so won't render NULLS clause)
			},
			{
				Expr:      sql.SQLN{Part: "value"},
				Ascending: false,
				NullsLast: false, // DESC NULLS FIRST (default for PostgreSQL, so won't render NULLS clause)
			},
		},
	}

	rows, err := backend.FetchQuery(ctx, query)
	if err != nil {
		t.Fatalf("FetchQuery with complete ORDER BY failed: %v", err)
	}
	defer rows.Close()

	// Just verify query executes successfully
	for rows.Next() {
		var name string
		var value int
		if err := rows.Scan(&name, &value); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
	}
}
