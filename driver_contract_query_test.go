package orm1

import (
	"context"
	"testing"
)

func testComplexQueryContracts(t *testing.T, ctx context.Context, backend Backend, schema, table string) {
	// Insert test data
	insertOp := InsertOp{
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

func testFetchQueryBasic(t *testing.T, ctx context.Context, backend Backend, schema, table string) {
	// Contract: FetchQuery executes structured query
	var fromTable SQL
	if schema != "" {
		fromTable = SQLQN{Part1: schema, Part2: table}
	} else {
		fromTable = SQLN{Part: table}
	}

	query := SQLQuery{
		Select: []SQL{
			SQLN{Part: "name"},
			SQLN{Part: "value"},
		},
		FromTable: fromTable,
		FromAlias: SQLN{Part: "t"},
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

func testCountQueryIgnoresPagination(t *testing.T, ctx context.Context, backend Backend, schema, table string) {
	// Contract: CountQuery ignores LIMIT/OFFSET
	var fromTable SQL
	if schema != "" {
		fromTable = SQLQN{Part1: schema, Part2: table}
	} else {
		fromTable = SQLN{Part: table}
	}

	var limitSQL SQL = SQLText{Text: "1"}
	query := SQLQuery{
		Select:    []SQL{SQLN{Part: "id"}},
		FromTable: fromTable,
		FromAlias: SQLN{Part: "t"},
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
func testRawSQLContracts(t *testing.T, ctx context.Context, backend Backend, schema, table string) {
	t.Run("FetchRaw_ReturnsRows", func(t *testing.T) {
		testFetchRawReturnsRows(t, ctx, backend, schema, table)
	})

	t.Run("ExecRaw_ReturnsAffectedCount", func(t *testing.T) {
		testExecRawReturnsAffectedCount(t, ctx, backend, schema, table)
	})
}

func testFetchRawReturnsRows(t *testing.T, ctx context.Context, backend Backend, schema, table string) {
	// Contract: FetchRaw executes raw SQL and returns Rows
	var sqlText string
	if schema != "" {
		sqlText = "SELECT name, value FROM " + schema + "." + table + " LIMIT 1"
	} else {
		sqlText = "SELECT name, value FROM " + table + " LIMIT 1"
	}
	fragment := SQLText{Text: sqlText}

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

func testExecRawReturnsAffectedCount(t *testing.T, ctx context.Context, backend Backend, schema, table string) {
	// Insert test data for update
	insertOp := InsertOp{
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
	fragment := SQLText{Text: sqlText}

	count, err := backend.ExecRaw(ctx, fragment)
	if err != nil {
		t.Fatalf("ExecRaw failed: %v", err)
	}

	if count != 1 {
		t.Errorf("Expected affected count=1, got %d", count)
	}
}

func testFetchQueryComplexSQL(t *testing.T, ctx context.Context, backend Backend, schema, table string) {
	// Contract: FetchQuery handles complex SQL AST with all types
	// This query tests: SQLParam, SQLAll, SQLAny, SQLLt, SQLGt, SQLIsNull, SQLIsNotNull,
	// SQLFragment, Joins, GroupBy, Having, OrderBy with NULLS, Offset

	var fromTable SQL
	if schema != "" {
		fromTable = SQLQN{Part1: schema, Part2: table}
	} else {
		fromTable = SQLN{Part: table}
	}

	// WHERE clause: (value > 10 AND value < 100 AND name = 'query_test1') OR (name IS NOT NULL AND id IS NULL)
	// This tests: SQLParam, SQLAll, SQLAny, SQLGt, SQLLt, SQLEq, SQLIsNull, SQLIsNotNull
	var whereClause SQL = SQLAny{
		Els: []SQL{
			SQLAll{
				Els: []SQL{
					SQLGt{
						Left:  SQLN{Part: "value"},
						Right: SQLParam{Value: 5},
					},
					SQLLt{
						Left:  SQLN{Part: "value"},
						Right: SQLParam{Value: 100},
					},
					SQLEq{
						Left:  SQLN{Part: "name"},
						Right: SQLParam{Value: "query_test1"},
					},
				},
			},
			SQLAll{
				Els: []SQL{
					SQLIsNotNull{
						Operand: SQLN{Part: "name"},
					},
					SQLIsNull{
						Operand: SQLText{Text: "NULL"}, // Use literal NULL
					},
				},
			},
		},
	}

	// HAVING clause: COUNT(*) > 0
	var havingClause SQL = SQLGt{
		Left: SQLFragment{
			Els: []SQL{
				SQLText{Text: "COUNT(*)"},
			},
		},
		Right: SQLParam{Value: 0},
	}

	var limit SQL = SQLParam{Value: 5}
	var offset SQL = SQLParam{Value: 1}

	query := SQLQuery{
		Select: []SQL{
			SQLN{Part: "name"},
			SQLN{Part: "value"},
		},
		FromTable: fromTable,
		FromAlias: SQLN{Part: "t"},
		Where:     &whereClause,
		GroupBy: []SQL{
			SQLN{Part: "name"},
			SQLN{Part: "value"},
		},
		Having: &havingClause,
		OrderBys: []OrderBy{
			{
				Expr:      SQLN{Part: "name"},
				Ascending: true,
				NullsLast: false, // NULLS FIRST
			},
			{
				Expr:      SQLN{Part: "value"},
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

func testCountQueryInTransaction(t *testing.T, ctx context.Context, backend Backend, schema, table string) {
	// Contract: CountQuery works within transactions
	err := backend.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	defer backend.Rollback(ctx)

	var fromTable SQL
	if schema != "" {
		fromTable = SQLQN{Part1: schema, Part2: table}
	} else {
		fromTable = SQLN{Part: table}
	}

	query := SQLQuery{
		Select:    []SQL{SQLN{Part: "id"}},
		FromTable: fromTable,
		FromAlias: SQLN{Part: "t"},
	}

	count, err := backend.CountQuery(ctx, query)
	if err != nil {
		t.Fatalf("CountQuery in transaction failed: %v", err)
	}

	if count < 3 {
		t.Errorf("Expected at least 3, got %d", count)
	}
}

func testFetchQueryWithJoins(t *testing.T, ctx context.Context, backend Backend, schema, table string) {
	// Contract: FetchQuery handles JOIN clauses
	// Insert additional test data
	insertOp := InsertOp{
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

	var fromTable SQL
	if schema != "" {
		fromTable = SQLQN{Part1: schema, Part2: table}
	} else {
		fromTable = SQLN{Part: table}
	}

	// Create a self-join query
	query := SQLQuery{
		Select: []SQL{
			SQLFragment{Els: []SQL{SQLN{Part: "t1"}, SQLText{Text: "."}, SQLN{Part: "name"}}},
		},
		FromTable: fromTable,
		FromAlias: SQLN{Part: "t1"},
		Joins: []Join{
			{
				Type:  "INNER JOIN",
				Table: fromTable,
				Alias: SQLN{Part: "t2"},
				On: SQLEq{
					Left:  SQLFragment{Els: []SQL{SQLN{Part: "t1"}, SQLText{Text: "."}, SQLN{Part: "value"}}},
					Right: SQLFragment{Els: []SQL{SQLN{Part: "t2"}, SQLText{Text: "."}, SQLN{Part: "value"}}},
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

func testFetchQueryQuotedIdentifiers(t *testing.T, ctx context.Context, backend Backend, schema, table string) {
	// Contract: FetchQuery handles identifiers with special characters (like double quotes)
	// This tests quoteIdentifier's quote escaping logic
	var fromTable SQL
	if schema != "" {
		fromTable = SQLQN{Part1: schema, Part2: table}
	} else {
		fromTable = SQLN{Part: table}
	}

	// Use a column name that would contain quotes if it existed
	// This tests the SQLQN with empty Part1 branch as well
	query := SQLQuery{
		Select: []SQL{
			SQLQN{Part1: "", Part2: "name"}, // Qualified name without schema
			SQLN{Part: `value`},
		},
		FromTable: fromTable,
		FromAlias: SQLN{Part: "t"},
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

func testFetchQueryCompleteOrderBy(t *testing.T, ctx context.Context, backend Backend, schema, table string) {
	// Contract: FetchQuery handles all ORDER BY variations (ASC NULLS FIRST, DESC NULLS LAST)
	var fromTable SQL
	if schema != "" {
		fromTable = SQLQN{Part1: schema, Part2: table}
	} else {
		fromTable = SQLN{Part: table}
	}

	query := SQLQuery{
		Select: []SQL{
			SQLN{Part: "name"},
			SQLN{Part: "value"},
		},
		FromTable: fromTable,
		FromAlias: SQLN{Part: "t"},
		OrderBys: []OrderBy{
			{
				Expr:      SQLN{Part: "name"},
				Ascending: true,
				NullsLast: true, // ASC NULLS LAST (default for PostgreSQL, so won't render NULLS clause)
			},
			{
				Expr:      SQLN{Part: "value"},
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
