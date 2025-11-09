package orm1

import (
	"database/sql"

	"github.com/hanpama/orm1/driver"
)

// Driver creates Backend instances for database operations.
// This is a type alias for driver.Driver, allowing users to reference
// the Driver interface from the orm1 package without importing driver.
type Driver = driver.Driver

// Rows is the interface for iterating over query results.
// This is a type alias for driver.Rows.
type Rows = driver.Rows

// SessionBackend defines the interface for database-specific implementations.
// This is a type alias for driver.Backend.
type SessionBackend = driver.Backend

// NewPostgreSQLDriver creates a driver for PostgreSQL databases.
//
// This is a convenience wrapper around driver.NewPostgres.
// The driver creates a new backend instance for each session.
//
// Example:
//
//	db, _ := sql.Open("postgres", "...")
//	driver := orm1.NewPostgreSQLDriver(db)
//	factory := orm1.NewSessionFactoryWithDriver(driver)
func NewPostgreSQLDriver(db *sql.DB) Driver {
	return driver.NewPostgres(db)
}

// NewSQLiteDriver creates a driver for SQLite databases.
//
// This is a convenience wrapper around driver.NewSQLite.
// The driver creates a new backend instance for each session.
//
// Example:
//
//	db, _ := sql.Open("sqlite3", "...")
//	driver := orm1.NewSQLiteDriver(db)
//	factory := orm1.NewSessionFactoryWithDriver(driver)
func NewSQLiteDriver(db *sql.DB) Driver {
	return driver.NewSQLite(db)
}
