package orm1

import (
	"context"

	"github.com/hanpama/orm1/sql"
)

// Page represents pagination metadata
type Page struct {
	Cursors         []Key
	HasPreviousPage bool
	HasNextPage     bool
}

// Paginate performs cursor-based pagination on the query.
// after/before are cursors (primary keys) for pagination boundaries.
// first/last control the number of results (use nil for no limit).
// Use Offset() method to set offset for the query.
// NOTE: Paginate ignores any offset set via Offset() method when after/before cursors are used.
func (q *EntityQuery[T]) Paginate(ctx context.Context, after Key, first *int, before Key, last *int) (*Page, error) {
	// Optimization #1: pre-allocate capacity for PK fields
	pkCount := len(q.mapping.PrimaryKey)
	orderBy := make([]sql.OrderBy, len(q.orderByOpts), len(q.orderByOpts)+pkCount)
	copy(orderBy, q.orderByOpts)
	for _, fieldName := range q.mapping.PrimaryKey {
		field := q.mapping.FieldMap[fieldName]
		orderBy = append(orderBy, sql.OrderBy{
			Expr:      sql.SQLQN{Part1: q.alias, Part2: field.Column},
			Ascending: true,
			NullsLast: true,
		})
	}

	if last != nil {
		// Reverse all ordering for backward pagination
		for i := range orderBy {
			orderBy[i] = sql.OrderBy{
				Expr:      orderBy[i].Expr,
				Ascending: !orderBy[i].Ascending,
				NullsLast: !orderBy[i].NullsLast,
			}
		}
	}

	// Optimization #3: pre-allocate capacity for cursor filters (max 2)
	cursorFilters := make([]sql.SQL, 0, 2)
	var afterRow []any
	var beforeRow []any
	var err error

	if after != nil && after.Length() > 0 {
		afterRow, err = q.fetchCursorRow(ctx, orderBy, after)
		if err != nil {
			return nil, err
		}
		if afterRow != nil {
			predicate := formatCursorPredicate(orderBy, afterRow, last == nil)
			cursorFilters = append(cursorFilters, predicate)
		}
	}

	if before != nil && before.Length() > 0 {
		beforeRow, err = q.fetchCursorRow(ctx, orderBy, before)
		if err != nil {
			return nil, err
		}
		if beforeRow != nil {
			predicate := formatCursorPredicate(orderBy, beforeRow, last != nil)
			cursorFilters = append(cursorFilters, predicate)
		}
	}

	// Optimization #4: pre-allocate capacity for filter slices
	var whereFilters []sql.SQL
	var havingFilters []sql.SQL

	if len(q.groupByExprs) > 0 {
		havingFilters = make([]sql.SQL, 0, len(q.havingConds)+len(cursorFilters))
		havingFilters = append(havingFilters, q.havingConds...)
		havingFilters = append(havingFilters, cursorFilters...)
		whereFilters = q.whereConds
	} else {
		whereFilters = make([]sql.SQL, 0, len(q.whereConds)+len(cursorFilters))
		whereFilters = append(whereFilters, q.whereConds...)
		whereFilters = append(whereFilters, cursorFilters...)
		havingFilters = q.havingConds
	}

	var limit *int
	if last != nil {
		limit = last
	} else if first != nil {
		limit = first
	}

	// +1 to limit to check if there are more pages
	var limitSQL, offsetSQL *sql.SQL

	if limit != nil {
		limitPlusOne := *limit + 1
		var l sql.SQL = sql.SQLParam{Value: limitPlusOne}
		limitSQL = &l
	}

	// Offset is only used when not using cursor-based pagination
	if q.offset != nil && (after == nil || after.Length() == 0) && (before == nil || before.Length() == 0) {
		var o sql.SQL = sql.SQLParam{Value: *q.offset}
		offsetSQL = &o
	}

	var whereClause *sql.SQL
	if len(whereFilters) > 0 {
		var w sql.SQL = sql.SQLAll{Els: whereFilters}
		whereClause = &w
	}

	var havingClause *sql.SQL
	if len(havingFilters) > 0 {
		var h sql.SQL = sql.SQLAll{Els: havingFilters}
		havingClause = &h
	}

	selectStmt := &sql.SQLQuery{
		Select:    q.buildPrimaryKeyColumns(),
		FromTable: sql.SQLQN{Part1: q.mapping.Schema, Part2: q.mapping.Table},
		FromAlias: sql.SQLText{Text: q.alias},
		Joins:     q.buildJoins(),
		Where:     whereClause,
		GroupBy:   q.buildGroupBy(),
		Having:    havingClause,
		OrderBys:  orderBy,
		Limit:     limitSQL,
		Offset:    offsetSQL,
	}

	rows, err := q.session.backend.FetchQuery(ctx, *selectStmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Optimization #5: reuse scan buffers outside loop
	pkCount = len(q.mapping.PrimaryKey)
	values := make([]any, pkCount)
	scanDest := make([]any, pkCount)
	for i := range scanDest {
		scanDest[i] = &values[i]
	}

	expectedCapacity := 11 // default estimate
	if limit != nil {
		expectedCapacity = *limit + 1
	}
	keyRows := make([]Key, 0, expectedCapacity)

	for rows.Next() {
		if err := rows.Scan(scanDest...); err != nil {
			return nil, err
		}
		// Copy values to avoid reuse
		keyCopy := make([]any, pkCount)
		copy(keyCopy, values)
		keyRows = append(keyRows, NewKey(keyCopy...))
	}

	var cursors []Key
	var hasPreviousPage, hasNextPage bool

	if limit != nil && len(keyRows) > *limit {
		cursors = keyRows[:*limit]
		if last != nil {
			hasPreviousPage = true
			hasNextPage = (beforeRow != nil) || (q.offset != nil && *q.offset > 0)
		} else {
			hasPreviousPage = (afterRow != nil) || (q.offset != nil && *q.offset > 0)
			hasNextPage = true
		}
	} else {
		cursors = keyRows
		if last != nil {
			// For backward pagination: hasPreviousPage means there are items before the current page
			// Since we reversed the order, "before" means there were items after the Before cursor
			hasPreviousPage = (afterRow != nil) || (q.offset != nil && *q.offset > 0)
			hasNextPage = (beforeRow != nil) || (q.offset != nil && *q.offset > 0)
		} else {
			hasPreviousPage = (afterRow != nil) || (q.offset != nil && *q.offset > 0)
			hasNextPage = false
		}
	}

	if last != nil {
		for i, j := 0, len(cursors)-1; i < j; i, j = i+1, j-1 {
			cursors[i], cursors[j] = cursors[j], cursors[i]
		}
	}

	return &Page{
		Cursors:         cursors,
		HasPreviousPage: hasPreviousPage,
		HasNextPage:     hasNextPage,
	}, nil
}

// fetchCursorRow fetches the order by column values for a given cursor (primary key)
func (q *EntityQuery[T]) fetchCursorRow(ctx context.Context, orderBys []sql.OrderBy, cursor Key) ([]any, error) {
	// Optimization #6: pre-allocate pkFilters capacity
	pkFilters := make([]sql.SQL, 0, len(q.mapping.PrimaryKey))
	for i, fieldName := range q.mapping.PrimaryKey {
		if i >= cursor.Length() {
			break
		}
		field := q.mapping.FieldMap[fieldName]
		pkFilters = append(pkFilters, sql.SQLEq{
			Left:  sql.SQLQN{Part1: q.alias, Part2: field.Column},
			Right: sql.SQLParam{Value: cursor.At(i)},
		})
	}

	var whereFilters []sql.SQL
	var havingFilters []sql.SQL

	if len(q.groupByExprs) > 0 {
		whereFilters = q.whereConds
		havingFilters = make([]sql.SQL, 0, len(q.havingConds)+len(pkFilters))
		havingFilters = append(havingFilters, q.havingConds...)
		havingFilters = append(havingFilters, pkFilters...)
	} else {
		whereFilters = make([]sql.SQL, 0, len(q.whereConds)+len(pkFilters))
		whereFilters = append(whereFilters, q.whereConds...)
		whereFilters = append(whereFilters, pkFilters...)
		havingFilters = q.havingConds
	}

	selectCols := make([]sql.SQL, 0, len(orderBys))
	for _, ob := range orderBys {
		selectCols = append(selectCols, ob.Expr)
	}

	var whereClause *sql.SQL
	if len(whereFilters) > 0 {
		var w sql.SQL = sql.SQLAll{Els: whereFilters}
		whereClause = &w
	}

	var havingClause *sql.SQL
	if len(havingFilters) > 0 {
		var h sql.SQL = sql.SQLAll{Els: havingFilters}
		havingClause = &h
	}

	selectStmt := &sql.SQLQuery{
		Select:    selectCols,
		FromTable: sql.SQLQN{Part1: q.mapping.Schema, Part2: q.mapping.Table},
		FromAlias: sql.SQLText{Text: q.alias},
		Joins:     q.buildJoins(),
		Where:     whereClause,
		GroupBy:   q.buildGroupBy(),
		Having:    havingClause,
	}

	rows, err := q.session.backend.FetchQuery(ctx, *selectStmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if rows.Next() {
		result := make([]any, len(orderBys))
		scanDest := make([]any, len(orderBys))
		for i := range result {
			scanDest[i] = &result[i]
		}
		if err := rows.Scan(scanDest...); err != nil {
			return nil, err
		}
		return result, nil
	}

	return nil, nil
}

// formatCursorPredicate generates complex cursor comparison predicates
// This handles multi-column ordering with NULL handling
func formatCursorPredicate(orderBys []sql.OrderBy, values []any, isForward bool) sql.SQL {
	// Optimization #8: pre-allocate orPredicates capacity
	orPredicates := make([]sql.SQL, 0, len(orderBys))

	for i := range orderBys {
		andPredicates := make([]sql.SQL, 0, i+1)

		for j := 0; j <= i; j++ {
			sort := orderBys[j]
			cursorValue := values[j]

			if i != j {
				// For earlier columns: must be equal
				if cursorValue == nil {
					// Cursor value is NULL: column must be NULL
					andPredicates = append(andPredicates, sql.SQLIsNull{Operand: sort.Expr})
				} else {
					// Cursor value is not NULL: (col = $N OR (col IS NULL AND $N IS NULL))
					// But we know $N is not NULL, so: (col = $N OR FALSE) = (col = $N)
					andPredicates = append(andPredicates, sql.SQLEq{
						Left:  sort.Expr,
						Right: sql.SQLParam{Value: cursorValue},
					})
				}
			} else {
				// For the last column: comparison based on direction
				if cursorValue == nil {
					// Cursor value is NULL: handle based on NULLS FIRST/LAST
					if sort.NullsLast {
						// NULL is sorted last
						if isForward {
							// Forward from NULL (last): no more rows
							andPredicates = append(andPredicates, sql.SQLText{Text: "FALSE"})
						} else {
							// Backward from NULL (last): get all non-NULL rows
							andPredicates = append(andPredicates, sql.SQLIsNotNull{Operand: sort.Expr})
						}
					} else {
						// NULL is sorted first
						if isForward {
							// Forward from NULL (first): get all non-NULL rows
							andPredicates = append(andPredicates, sql.SQLIsNotNull{Operand: sort.Expr})
						} else {
							// Backward from NULL (first): no more rows
							andPredicates = append(andPredicates, sql.SQLText{Text: "FALSE"})
						}
					}
				} else {
					// Cursor value is not NULL: (col > $N OR col IS NULL)
					var comp sql.SQL
					if sort.Ascending == isForward {
						comp = sql.SQLGt{Left: sort.Expr, Right: sql.SQLParam{Value: cursorValue}}
					} else {
						comp = sql.SQLLt{Left: sort.Expr, Right: sql.SQLParam{Value: cursorValue}}
					}

					// NULL handling: should col IS NULL be included?
					if sort.NullsLast == isForward {
						// NULLs come after cursor, include them
						andPredicates = append(andPredicates, sql.SQLAny{Els: []sql.SQL{comp, sql.SQLIsNull{Operand: sort.Expr}}})
					} else {
						// NULLs come before cursor, exclude them
						andPredicates = append(andPredicates, comp)
					}
				}
			}
		}

		orPredicates = append(orPredicates, sql.SQLAll{Els: andPredicates})
	}

	return sql.SQLAny{Els: orPredicates}
}
