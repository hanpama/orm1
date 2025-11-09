package orm1_test

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/hanpama/orm1"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/mattn/go-sqlite3"
)

// Backend represents a test database backend type
type Backend string

const (
	SQLite     Backend = "sqlite"
	PostgreSQL Backend = "postgresql"
)

// AllBackends returns all supported backends for testing
func AllBackends() []Backend {
	return []Backend{SQLite, PostgreSQL}
}

// DBSetup encapsulates database connection and cleanup
type DBSetup struct {
	DB      *sql.DB
	Driver  orm1.Driver
	Cleanup func()
}

// SetupDB creates a test database connection for the specified backend
func SetupDB(t *testing.T, backend Backend) *DBSetup {
	switch backend {
	case SQLite:
		return setupSQLite(t)
	case PostgreSQL:
		return setupPostgreSQL(t)
	default:
		t.Fatalf("unsupported backend: %s", backend)
		return nil
	}
}

func setupSQLite(t *testing.T) *DBSetup {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}

	cleanup := func() {
		db.Close()
	}

	return &DBSetup{
		DB:      db,
		Driver:  orm1.NewSQLiteDriver(db),
		Cleanup: cleanup,
	}
}

func setupPostgreSQL(t *testing.T) *DBSetup {
	db, err := sql.Open("pgx", "postgres://testuser:testpass@localhost:25432/testdb?sslmode=disable")
	if err != nil {
		t.Fatalf("failed to open postgresql: %v", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		t.Skipf("PostgreSQL not available: %v", err)
	}

	cleanup := func() {
		db.Close()
	}

	return &DBSetup{
		DB:      db,
		Driver:  orm1.NewPostgreSQLDriver(db),
		Cleanup: cleanup,
	}
}

// ExecSchema executes a schema creation statement with backend-specific adjustments
func (setup *DBSetup) ExecSchema(t *testing.T, backend Backend, sqliteSchema, postgresSchema string) {
	var schema string
	switch backend {
	case SQLite:
		schema = sqliteSchema
	case PostgreSQL:
		schema = postgresSchema
	default:
		t.Fatalf("unknown backend type")
	}

	_, err := setup.DB.Exec(schema)
	if err != nil {
		t.Fatalf("failed to execute schema: %v", err)
	}
}

// DropTables drops the specified tables (PostgreSQL only, no-op for SQLite)
func (setup *DBSetup) DropTables(backend Backend, tables ...string) {
	if backend == PostgreSQL {
		for _, table := range tables {
			setup.DB.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", table))
		}
	}
}

// NewSessionFactory creates a new SessionFactory with the driver configured.
func (setup *DBSetup) NewSessionFactory() *orm1.SessionFactory {
	return orm1.NewSessionFactoryWithDriver(setup.Driver)
}
