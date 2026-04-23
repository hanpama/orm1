package orm1

import (
	"context"
	"fmt"
	"reflect"
)

// EntityQuery provides a type-safe query builder for entities.
// It uses generics to ensure compile-time type safety for query results.
// Supports WHERE, JOIN, ORDER BY, GROUP BY, HAVING, and pagination.
type EntityQuery[T any] struct {
	session      *Session
	mapping      *EntityMapping
	alias        string
	joins        map[string]*Join
	whereConds   []SQL
	havingConds  []SQL
	orderByOpts  []OrderBy
	groupByExprs []SQL
	offset       *int
}

// NewEntityQuery creates a new query builder for entity type T.
// The alias is used as the table alias in the generated SQL.
func NewEntityQuery[T any](session *Session, alias string) *EntityQuery[T] {
	var zero T
	entityType := reflect.TypeOf(zero)
	if entityType.Kind() == reflect.Ptr {
		entityType = entityType.Elem()
	}

	mapping, err := session.getMapping(entityType)
	if err != nil {
		panic(err)
	}

	return &EntityQuery[T]{
		session:      session,
		mapping:      mapping,
		alias:        alias,
		joins:        make(map[string]*Join),
		whereConds:   []SQL{},
		havingConds:  []SQL{},
		orderByOpts:  []OrderBy{},
		groupByExprs: []SQL{},
	}
}

// Join adds an INNER JOIN to the query.
// table is the table name, alias is its query alias, on is the join condition.
func (q *EntityQuery[T]) Join(table string, alias string, on string, params ...any) *EntityQuery[T] {
	q.joins[alias] = &Join{
		Type:  "JOIN",
		Table: SQLText{Text: table},
		Alias: SQLText{Text: alias},
		On:    ParseSQL(on, params...),
	}
	return q
}

// LeftJoin adds a LEFT OUTER JOIN to the query.
// table is the table name, alias is its query alias, on is the join condition.
func (q *EntityQuery[T]) LeftJoin(table string, alias string, on string, params ...any) *EntityQuery[T] {
	q.joins[alias] = &Join{
		Type:  "LEFT JOIN",
		Table: SQLText{Text: table},
		Alias: SQLText{Text: alias},
		On:    ParseSQL(on, params...),
	}
	return q
}

// Where adds a WHERE condition to the query.
// Multiple Where calls are combined with AND.
func (q *EntityQuery[T]) Where(condition string, params ...any) *EntityQuery[T] {
	q.whereConds = append(q.whereConds, ParseSQL(fmt.Sprintf("(%s)", condition), params...))
	return q
}

// Having adds a HAVING condition to the query.
// Multiple Having calls are combined with AND.
func (q *EntityQuery[T]) Having(condition string, params ...any) *EntityQuery[T] {
	q.havingConds = append(q.havingConds, ParseSQL(fmt.Sprintf("(%s)", condition), params...))
	return q
}

// GroupByPrimaryKey adds a GROUP BY clause using the entity's primary key columns.
func (q *EntityQuery[T]) GroupByPrimaryKey() *EntityQuery[T] {
	for _, fieldName := range q.mapping.PrimaryKey {
		field := q.mapping.FieldMap[fieldName]
		q.groupByExprs = append(q.groupByExprs, SQLQN{Part1: q.alias, Part2: field.Column})
	}
	return q
}

// Offset sets the offset for the query.
func (q *EntityQuery[T]) Offset(n int) *EntityQuery[T] {
	q.offset = &n
	return q
}

// OrderBy sets the ORDER BY clause for the query.
func (q *EntityQuery[T]) OrderBy(orderBys ...OrderBy) *EntityQuery[T] {
	q.orderByOpts = orderBys
	return q
}

// Asc creates an ascending OrderBy clause with NULLS LAST (PostgreSQL/Oracle standard).
// This ensures consistent behavior across all databases (PostgreSQL, SQLite, Oracle).
func (q *EntityQuery[T]) Asc(expr string, params ...any) OrderBy {
	return OrderBy{
		Expr:      ParseSQL(expr, params...),
		Ascending: true,
		NullsLast: true,
	}
}

// Desc creates a descending OrderBy clause with NULLS FIRST (PostgreSQL/Oracle standard).
// This ensures consistent behavior across all databases (PostgreSQL, SQLite, Oracle).
func (q *EntityQuery[T]) Desc(expr string, params ...any) OrderBy {
	return OrderBy{
		Expr:      ParseSQL(expr, params...),
		Ascending: false,
		NullsLast: false,
	}
}

// AscNullsLast creates an ascending OrderBy clause with NULL values sorted last.
func (q *EntityQuery[T]) AscNullsLast(expr string, params ...any) OrderBy {
	return OrderBy{
		Expr:      ParseSQL(expr, params...),
		Ascending: true,
		NullsLast: true,
	}
}

// AscNullsFirst creates an ascending OrderBy clause with NULL values sorted first.
func (q *EntityQuery[T]) AscNullsFirst(expr string, params ...any) OrderBy {
	return OrderBy{
		Expr:      ParseSQL(expr, params...),
		Ascending: true,
		NullsLast: false,
	}
}

// DescNullsLast creates a descending OrderBy clause with NULL values sorted last.
func (q *EntityQuery[T]) DescNullsLast(expr string, params ...any) OrderBy {
	return OrderBy{
		Expr:      ParseSQL(expr, params...),
		Ascending: false,
		NullsLast: true,
	}
}

// DescNullsFirst creates a descending OrderBy clause with NULL values sorted first.
func (q *EntityQuery[T]) DescNullsFirst(expr string, params ...any) OrderBy {
	return OrderBy{
		Expr:      ParseSQL(expr, params...),
		Ascending: false,
		NullsLast: false,
	}
}

func (q *EntityQuery[T]) buildSelectColumns() []SQL {
	selectCols := make([]SQL, len(q.mapping.AllFields))
	for i, fieldName := range q.mapping.AllFields {
		field := q.mapping.FieldMap[fieldName]
		selectCols[i] = SQLQN{Part1: q.alias, Part2: field.Column}
	}
	return selectCols
}

func (q *EntityQuery[T]) buildPrimaryKeyColumns() []SQL {
	selectCols := make([]SQL, 0, len(q.mapping.PrimaryKey))
	for _, fieldName := range q.mapping.PrimaryKey {
		field := q.mapping.FieldMap[fieldName]
		selectCols = append(selectCols, SQLQN{Part1: q.alias, Part2: field.Column})
	}
	return selectCols
}

func (q *EntityQuery[T]) buildJoins() []Join {
	joins := make([]Join, 0, len(q.joins))
	for _, join := range q.joins {
		joins = append(joins, *join)
	}
	return joins
}

func (q *EntityQuery[T]) buildWhereClause() *SQL {
	if len(q.whereConds) == 0 {
		return nil
	}
	var w SQL = SQLAll{Els: q.whereConds}
	return &w
}

func (q *EntityQuery[T]) buildHavingClause() *SQL {
	if len(q.havingConds) == 0 {
		return nil
	}
	var h SQL = SQLAll{Els: q.havingConds}
	return &h
}

func (q *EntityQuery[T]) buildGroupBy() []SQL {
	if len(q.groupByExprs) == 0 {
		return nil
	}
	return q.groupByExprs
}

// fetch executes the query and returns matching entities with optional limit.
// All returned entities are marked as persisted.
func (q *EntityQuery[T]) fetch(ctx context.Context, limit *int) ([]*T, error) {
	var limitSQL, offsetSQL *SQL

	if limit != nil {
		var l SQL = SQLParam{Value: *limit}
		limitSQL = &l
	}

	if q.offset != nil {
		var o SQL = SQLParam{Value: *q.offset}
		offsetSQL = &o
	}

	selectStmt := &SQLQuery{
		Select:    q.buildSelectColumns(),
		FromTable: SQLQN{Part1: q.mapping.Schema, Part2: q.mapping.Table},
		FromAlias: SQLText{Text: q.alias},
		Joins:     q.buildJoins(),
		Where:     q.buildWhereClause(),
		GroupBy:   q.buildGroupBy(),
		Having:    q.buildHavingClause(),
		OrderBys:  q.orderByOpts,
		Limit:     limitSQL,
		Offset:    offsetSQL,
	}

	rows, err := q.session.backend.FetchQuery(ctx, *selectStmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Pre-allocate slices with expected capacity from limit
	expectedCapacity := 0
	if limit != nil {
		expectedCapacity = *limit
	}

	// Check if entity has children before allocating trackedEntities
	hasChildren := len(q.mapping.ChildMap) > 0

	entities := make([]any, 0, expectedCapacity)

	for rows.Next() {
		entityPtr := reflect.New(q.mapping.EntityType).Interface()
		if err := q.session.scanEntity(q.mapping, entityPtr, rows, q.mapping.AllFields); err != nil {
			return nil, err
		}

		q.session.markPersisted(entityPtr)
		entities = append(entities, entityPtr)
	}

	if hasChildren && len(entities) > 0 {
		err = q.session.loadChildren(ctx, q.mapping, entities)
		if err != nil {
			return nil, err
		}
	}

	result := make([]*T, 0, len(entities))
	for _, entity := range entities {
		result = append(result, entity.(*T))
	}

	return result, nil
}

// FetchAll executes the query and returns all matching entities.
// All returned entities are marked as persisted.
func (q *EntityQuery[T]) FetchAll(ctx context.Context) ([]*T, error) {
	return q.fetch(ctx, nil)
}

// FetchMany executes the query and returns up to limit matching entities.
// All returned entities are marked as persisted.
func (q *EntityQuery[T]) FetchMany(ctx context.Context, limit int) ([]*T, error) {
	return q.fetch(ctx, &limit)
}

// FetchOne executes the query and returns the first matching entity.
// Returns nil if no entity matches the query.
func (q *EntityQuery[T]) FetchOne(ctx context.Context) (*T, error) {
	results, err := q.FetchMany(ctx, 1)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, nil
	}
	return results[0], nil
}

// Count returns the number of entities matching the query.
func (q *EntityQuery[T]) Count(ctx context.Context) (int64, error) {
	selectStmt := &SQLQuery{
		Select:    q.buildPrimaryKeyColumns(),
		FromTable: SQLQN{Part1: q.mapping.Schema, Part2: q.mapping.Table},
		FromAlias: SQLText{Text: q.alias},
		Joins:     q.buildJoins(),
		Where:     q.buildWhereClause(),
		GroupBy:   q.buildGroupBy(),
		Having:    q.buildHavingClause(),
	}

	return q.session.backend.CountQuery(ctx, *selectStmt)
}
