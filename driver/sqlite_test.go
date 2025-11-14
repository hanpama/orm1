package driver_test

import (
	"database/sql"
	"testing"

	"github.com/hanpama/orm1/driver"
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

	// Setup table (SQLite doesn't use schemas)
	schema := ""
	table := "test_entity"

	ddl := `
		CREATE TABLE test_entity (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL,
			value INTEGER
		);
	`

	if _, err := db.Exec(ddl); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Create driver
	drv := driver.NewSQLite(db)
	defer drv.Close()

	// Run all driver contracts
	RunDriverContracts(t, drv, schema, table)
}
