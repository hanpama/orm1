// Package driver provides database driver interfaces and implementations for orm1.
//
// This package defines the abstraction layer between orm1's ORM logic and
// database-specific implementations. It includes interfaces for drivers and
// backends, as well as concrete implementations for supported databases.
package orm1

import (
	"context"
	"database/sql"
	"fmt"
)

// convertToSQLIsolationLevel converts orm1 isolation levels to database/sql isolation levels.
func convertToSQLIsolationLevel(level IsolationLevel) sql.IsolationLevel {
	switch level {
	case LevelDefault:
		return sql.LevelDefault
	case LevelReadUncommitted:
		return sql.LevelReadUncommitted
	case LevelReadCommitted:
		return sql.LevelReadCommitted
	case LevelWriteCommitted:
		return sql.LevelWriteCommitted
	case LevelRepeatableRead:
		return sql.LevelRepeatableRead
	case LevelSnapshot:
		return sql.LevelSnapshot
	case LevelSerializable:
		return sql.LevelSerializable
	case LevelLinearizable:
		return sql.LevelLinearizable
	default:
		panic(fmt.Sprintf("invalid isolation level: %d", level))
	}
}

// Rows is the interface for iterating over query results.
type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Close() error
	Columns() ([]string, error)
}

// emptyRows is a Rows implementation that returns no results.
// Used when operations are called with empty input (e.g., Select with no keys).
type emptyRows struct{}

func (e *emptyRows) Next() bool                 { return false }
func (e *emptyRows) Scan(dest ...any) error     { return nil }
func (e *emptyRows) Close() error               { return nil }
func (e *emptyRows) Columns() ([]string, error) { return nil, nil }

// Driver creates Backend instances for each session.
// Implementations must create a new backend instance per call to CreateBackend,
// as backends are not safe for concurrent use (they track transaction state).
type Driver interface {
	CreateBackend() Backend
	Close() error
}

// Backend defines the interface for database-specific implementations.
// Custom backends can be implemented to support different databases.
//
// The backend receives operation commands (SelectOp, InsertOp, etc.) from the Session
// and translates them into database-specific SQL. For complex queries, it also handles
// SQL AST types from the sql package (SQLQuery, SQL).
//
// Note: Backend instances are not safe for concurrent use. Each session
// should have its own backend instance (created via Driver).
type Backend interface {
	// Entity CRUD operations
	Select(ctx context.Context, op SelectOp) (Rows, error)
	Insert(ctx context.Context, op InsertOp) (Rows, error)
	Update(ctx context.Context, op UpdateOp) error
	Delete(ctx context.Context, op DeleteOp) error

	// Complex query operations (uses SQL AST)
	FetchQuery(ctx context.Context, stmt SQLQuery) (Rows, error)
	CountQuery(ctx context.Context, stmt SQLQuery) (int64, error)

	// Raw SQL operations (uses SQL AST)
	FetchRaw(ctx context.Context, fragment SQL) (Rows, error)
	ExecRaw(ctx context.Context, fragment SQL) (int64, error)

	// Transaction control
	Begin(ctx context.Context) error
	BeginTx(ctx context.Context, opts *TxOptions) error
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error

	// Nested transaction control (SAVEPOINT support)
	Savepoint(ctx context.Context, name string) error
	ReleaseSavepoint(ctx context.Context, name string) error
	RollbackToSavepoint(ctx context.Context, name string) error
}
