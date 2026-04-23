package orm1

import (
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestSQLite(t *testing.T) {
	// Setup database connection
	db, err := sql.Open("sqlite3", "file::memory:?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Skipf("SQLite not available: %v", err)
	}

	// Setup table with explicit schema (SQLite default schema is "main")
	schema := "main"
	table := "test_entity"

	ddl := `
		CREATE TABLE main.test_entity (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL,
			value INTEGER
		);
	`

	if _, err := db.Exec(ddl); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Create driver
	drv := NewSQLite(db)
	defer drv.Close()

	// Run all driver contracts
	RunDriverContracts(t, drv, schema, table)
}
