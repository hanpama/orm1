package orm1_test

import (
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/hanpama/orm1"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/mattn/go-sqlite3"
)

var (
	sqliteDriver   orm1.Driver
	postgresDriver orm1.Driver
)

func TestMain(m *testing.M) {
	setupAllTables()
	code := m.Run()
	cleanupAllTables()
	os.Exit(code)
}

// setupAllTables initializes both SQLite and PostgreSQL databases with all test tables
func setupAllTables() {
	// Create in-memory SQLite database
	sqliteDB, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		panic(fmt.Sprintf("failed to open sqlite: %v", err))
	}

	// Read and execute SQLite schema
	sqliteSchema, err := os.ReadFile("testdata/sqlite.sql")
	if err != nil {
		panic(fmt.Sprintf("failed to read sqlite schema: %v", err))
	}

	if _, err := sqliteDB.Exec(string(sqliteSchema)); err != nil {
		panic(fmt.Sprintf("failed to create sqlite schema: %v", err))
	}

	// Create PostgreSQL connection
	postgresDB, err := sql.Open("pgx", "postgres://testuser:testpass@localhost:25432/testdb?sslmode=disable")
	if err != nil {
		panic(fmt.Sprintf("failed to open postgresql: %v", err))
	}

	if err := postgresDB.Ping(); err != nil {
		postgresDB.Close()
		panic(fmt.Sprintf("PostgreSQL not available: %v", err))
	}

	// Read and execute PostgreSQL schema
	postgresSchema, err := os.ReadFile("testdata/postgresql.sql")
	if err != nil {
		panic(fmt.Sprintf("failed to read postgresql schema: %v", err))
	}

	if _, err := postgresDB.Exec(string(postgresSchema)); err != nil {
		panic(fmt.Sprintf("failed to create postgresql schema: %v", err))
	}

	// Set search path to test schema
	_, err = postgresDB.Exec("SET search_path TO orm1_test")
	if err != nil {
		panic(fmt.Sprintf("failed to set search path: %v", err))
	}

	// Initialize drivers
	sqliteDriver = orm1.NewSQLiteDriver(sqliteDB)
	postgresDriver = orm1.NewPostgreSQLDriver(postgresDB)
}

// cleanupAllTables closes all database connections
func cleanupAllTables() {
	if sqliteDriver != nil {
		sqliteDriver.Close()
		sqliteDriver = nil
	}
	if postgresDriver != nil {
		postgresDriver.Close()
		postgresDriver = nil
	}
}
