package shared

import (
	"database/sql"
)

// SetupSimpleSchema creates tables for simple CRUD benchmarks
func SetupSimpleSchema(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL,
			email TEXT NOT NULL,
			age INTEGER NOT NULL
		)
	`)
	return err
}

// SetupAggregateSchema creates tables for aggregate (parent-child) benchmarks
func SetupAggregateSchema(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS orders (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			customer TEXT NOT NULL,
			total REAL NOT NULL
		);

		CREATE TABLE IF NOT EXISTS order_items (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			order_id INTEGER NOT NULL,
			product TEXT NOT NULL,
			quantity INTEGER NOT NULL,
			price REAL NOT NULL
		);
	`)
	return err
}

// CleanupTables truncates all tables
func CleanupTables(db *sql.DB) error {
	_, err := db.Exec(`
		DELETE FROM users;
		DELETE FROM order_items;
		DELETE FROM orders;
	`)
	return err
}
