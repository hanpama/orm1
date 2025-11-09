// Package driver provides database driver interfaces and implementations for orm1.
//
// This package defines the abstraction layer between orm1's ORM logic and
// database-specific implementations. It includes interfaces for drivers and
// backends, as well as concrete implementations for supported databases.
package driver

import (
	"context"

	"github.com/hanpama/orm1/sql"
)

// Rows is the interface for iterating over query results.
type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Close() error
	Columns() ([]string, error)
}

// Driver creates Backend instances for each session.
// Implementations must create a new backend instance per call to CreateBackend,
// as backends are not safe for concurrent use (they track transaction state).
type Driver interface {
	CreateBackend() Backend
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
	Update(ctx context.Context, op UpdateOp) (Rows, error)
	Delete(ctx context.Context, op DeleteOp) (int64, error)

	// Complex query operations (uses SQL AST)
	FetchQuery(ctx context.Context, stmt sql.SQLQuery) (Rows, error)
	CountQuery(ctx context.Context, stmt sql.SQLQuery) (int64, error)

	// Raw SQL operations (uses SQL AST)
	FetchRaw(ctx context.Context, fragment sql.SQL) (Rows, error)
	ExecRaw(ctx context.Context, fragment sql.SQL) (int64, error)

	// Transaction control
	Begin(ctx context.Context) error
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}
