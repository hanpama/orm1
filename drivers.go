package orm1

import (
	"database/sql"
)

// SessionBackend is the interface used by Session to execute database operations.
type SessionBackend = Backend

// NewPostgreSQLDriver creates a driver for PostgreSQL databases.
//
// This is a convenience wrapper around NewPostgres.
// The driver creates a new backend instance for each session.
//
// Example:
//
//	db, _ := sql.Open("postgres", "...")
//	driver := orm1.NewPostgreSQLDriver(db)
//	factory := orm1.NewSessionFactoryWithDriver(driver)
func NewPostgreSQLDriver(db *sql.DB) Driver {
	return NewPostgres(db)
}

// NewSQLiteDriver creates a driver for SQLite databases.
//
// This is a convenience wrapper around NewSQLite.
// The driver creates a new backend instance for each session.
//
// Example:
//
//	db, _ := sql.Open("sqlite3", "...")
//	driver := orm1.NewSQLiteDriver(db)
//	factory := orm1.NewSessionFactoryWithDriver(driver)
func NewSQLiteDriver(db *sql.DB) Driver {
	return NewSQLite(db)
}
