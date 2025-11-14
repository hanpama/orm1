package driver_test

import (
	"database/sql"
	"testing"

	"github.com/hanpama/orm1/driver"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestPostgreSQL(t *testing.T) {
	// Setup database connection
	db, err := sql.Open("pgx", "postgres://testuser:testpass@localhost:25432/testdb?sslmode=disable")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Skipf("PostgreSQL not available: %v", err)
	}

	// Setup schema and table
	schema := "driver_test"
	table := "test_entity"

	ddl := `
		DROP SCHEMA IF EXISTS driver_test CASCADE;
		CREATE SCHEMA driver_test;
		SET search_path TO driver_test;

		CREATE TABLE test_entity (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			value INTEGER
		);
	`

	if _, err := db.Exec(ddl); err != nil {
		t.Fatalf("failed to create schema: %v", err)
	}

	// Create driver
	drv := driver.NewPostgres(db)
	defer drv.Close()

	// Run all driver contracts
	RunDriverContracts(t, drv, schema, table)
}
