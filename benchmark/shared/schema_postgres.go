package shared

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// SetupPostgresSchema creates tables for PostgreSQL benchmarks
func SetupPostgresSchema(db *sql.DB) error {
	// Drop tables if they exist
	_, err := db.Exec(`
		DROP TABLE IF EXISTS order_notes;
		DROP TABLE IF EXISTS order_items;
		DROP TABLE IF EXISTS orders;
		DROP TABLE IF EXISTS users;
	`)
	if err != nil {
		return err
	}

	// Create users table
	_, err = db.Exec(`
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL,
			age INTEGER NOT NULL
		)
	`)
	if err != nil {
		return err
	}

	// Create orders and order_items tables
	_, err = db.Exec(`
		CREATE TABLE orders (
			id SERIAL PRIMARY KEY,
			customer TEXT NOT NULL,
			total DECIMAL(10,2) NOT NULL
		);

		CREATE TABLE order_items (
			id SERIAL PRIMARY KEY,
			order_id INTEGER NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
			product TEXT NOT NULL,
			quantity INTEGER NOT NULL,
			price DECIMAL(10,2) NOT NULL
		);

		CREATE TABLE order_notes (
			id SERIAL PRIMARY KEY,
			order_id INTEGER NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
			content TEXT NOT NULL
		);
	`)
	return err
}

// GetPostgresDB returns a PostgreSQL database connection for benchmarks
// Use environment variable: POSTGRES_URL or default to docker-compose setup
func GetPostgresDB() (*sql.DB, error) {
	dsn := os.Getenv("POSTGRES_URL")
	if dsn == "" {
		// Default to docker-compose settings (port 25432)
		dsn = "host=localhost port=25432 user=testuser password=testpass dbname=testdb sslmode=disable"
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgres with pgx: %w", err)
	}

	// Test connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping postgres: %w", err)
	}

	return db, nil
}

// CleanupPostgresTables truncates all tables
func CleanupPostgresTables(db *sql.DB) error {
	_, err := db.Exec(`
		TRUNCATE TABLE order_notes, order_items, orders, users RESTART IDENTITY CASCADE;
	`)
	return err
}
