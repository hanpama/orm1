package orm1_test

import (
	"context"
	"testing"

	"github.com/hanpama/orm1"
)

type RawUser struct {
	ID   int64
	Name string
	Age  int64
}

type RawUserWithNull struct {
	ID   int64
	Name string
	Age  *int64
}

func setupRawTestDB(t *testing.T, backend Backend) *DBSetup {
	setup := SetupDB(t, backend)

	// Drop existing tables for PostgreSQL
	setup.DropTables(backend, "users")

	setup.ExecSchema(t, backend, `
		CREATE TABLE users (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL,
			age INTEGER
		);
	`, `
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			age INTEGER
		);
	`)

	return setup
}

func createRawTestSession(setup *DBSetup) *orm1.Session {
	factory := setup.NewSessionFactory()
	return factory.CreateSession()
}

func TestRawQueryScanAll(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupRawTestDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert test data
			_, err := setup.DB.Exec(`INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', 25), ('Charlie', 35)`)
			if err != nil {
				t.Fatal(err)
			}

			session := createRawTestSession(setup)

			// Test ScanAll with parameter interpolation
			rawQuery := orm1.NewRawQuery(session, "SELECT id, name, age FROM users WHERE age > ? ORDER BY age", 25)

			var results []*RawUser
			err = rawQuery.ScanAll(ctx, &results)
			if err != nil {
				t.Fatal(err)
			}

			if len(results) != 2 {
				t.Fatalf("Expected 2 results, got %d", len(results))
			}

			// Check first row
			if results[0].Name != "Alice" {
				t.Errorf("Expected name 'Alice', got %v", results[0].Name)
			}
			if results[0].Age != 30 {
				t.Errorf("Expected age 30, got %v", results[0].Age)
			}

			// Check second row
			if results[1].Name != "Charlie" {
				t.Errorf("Expected name 'Charlie', got %v", results[1].Name)
			}
			if results[1].Age != 35 {
				t.Errorf("Expected age 35, got %v", results[1].Age)
			}
		})
	}
}

func TestRawQueryScanOne(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupRawTestDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert test data
			_, err := setup.DB.Exec(`INSERT INTO users (name, age) VALUES ('Alice', 30)`)
			if err != nil {
				t.Fatal(err)
			}

			session := createRawTestSession(setup)

			// Test ScanOne
			rawQuery := orm1.NewRawQuery(session, "SELECT id, name, age FROM users WHERE name = ?", "Alice")

			var result RawUser
			err = rawQuery.ScanOne(ctx, &result)
			if err != nil {
				t.Fatal(err)
			}

			if result.Name != "Alice" {
				t.Errorf("Expected name 'Alice', got %v", result.Name)
			}
			if result.Age != 30 {
				t.Errorf("Expected age 30, got %v", result.Age)
			}
		})
	}
}

func TestRawQueryScanOneEmpty(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupRawTestDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			session := createRawTestSession(setup)

			// Test ScanOne with no results
			rawQuery := orm1.NewRawQuery(session, "SELECT id, name, age FROM users WHERE name = ?", "NonExistent")

			var result RawUser
			err := rawQuery.ScanOne(ctx, &result)
			if err != nil {
				t.Fatal(err)
			}

			// When no rows, struct should remain zero-valued
			if result.Name != "" {
				t.Errorf("Expected empty name, got %v", result.Name)
			}
		})
	}
}

func TestRawQueryMultipleParams(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupRawTestDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert test data
			_, err := setup.DB.Exec(`INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', 25), ('Charlie', 35)`)
			if err != nil {
				t.Fatal(err)
			}

			session := createRawTestSession(setup)

			// Test with multiple parameters
			rawQuery := orm1.NewRawQuery(session, "SELECT id, name, age FROM users WHERE age >= ? AND age <= ? ORDER BY age", 25, 30)

			var results []*RawUser
			err = rawQuery.ScanAll(ctx, &results)
			if err != nil {
				t.Fatal(err)
			}

			if len(results) != 2 {
				t.Fatalf("Expected 2 results, got %d", len(results))
			}

			if results[0].Name != "Bob" {
				t.Errorf("Expected name 'Bob', got %v", results[0].Name)
			}
			if results[1].Name != "Alice" {
				t.Errorf("Expected name 'Alice', got %v", results[1].Name)
			}
		})
	}
}

func TestRawQueryWithNullValues(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupRawTestDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert test data with NULL age
			_, err := setup.DB.Exec(`INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', NULL)`)
			if err != nil {
				t.Fatal(err)
			}

			session := createRawTestSession(setup)

			// Test query that includes NULL values
			rawQuery := orm1.NewRawQuery(session, "SELECT id, name, age FROM users ORDER BY id")

			var results []*RawUserWithNull
			err = rawQuery.ScanAll(ctx, &results)
			if err != nil {
				t.Fatal(err)
			}

			if len(results) != 2 {
				t.Fatalf("Expected 2 results, got %d", len(results))
			}

			// Check that NULL is handled properly
			if results[0].Age == nil || *results[0].Age != 30 {
				t.Errorf("Expected age 30, got %v", results[0].Age)
			}
			if results[1].Age != nil {
				t.Errorf("Expected age nil, got %v", results[1].Age)
			}
		})
	}
}

func TestRawQueryRows(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupRawTestDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert test data
			_, err := setup.DB.Exec(`INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', 25)`)
			if err != nil {
				t.Fatal(err)
			}

			session := createRawTestSession(setup)

			// Test Rows for manual iteration
			rawQuery := orm1.NewRawQuery(session, "SELECT id, name, age FROM users ORDER BY age")

			rows, err := rawQuery.Rows(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()

			var users []RawUser
			for rows.Next() {
				var user RawUser
				if err := rows.Scan(&user.ID, &user.Name, &user.Age); err != nil {
					t.Fatal(err)
				}
				users = append(users, user)
			}

			if len(users) != 2 {
				t.Fatalf("Expected 2 users, got %d", len(users))
			}

			if users[0].Name != "Bob" {
				t.Errorf("Expected first user 'Bob', got %v", users[0].Name)
			}
			if users[1].Name != "Alice" {
				t.Errorf("Expected second user 'Alice', got %v", users[1].Name)
			}
		})
	}
}

func TestRawQueryExec(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupRawTestDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert test data
			_, err := setup.DB.Exec(`INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', 25), ('Charlie', 35)`)
			if err != nil {
				t.Fatal(err)
			}

			session := createRawTestSession(setup)

			// Test UPDATE with Exec
			rawQuery := orm1.NewRawQuery(session, "UPDATE users SET age = ? WHERE name = ?", 31, "Alice")
			affected, err := rawQuery.Exec(ctx)
			if err != nil {
				t.Fatal(err)
			}

			if affected != 1 {
				t.Errorf("Expected 1 row affected, got %d", affected)
			}

			// Verify the update
			var result RawUser
			verifyQuery := orm1.NewRawQuery(session, "SELECT id, name, age FROM users WHERE name = ?", "Alice")
			err = verifyQuery.ScanOne(ctx, &result)
			if err != nil {
				t.Fatal(err)
			}

			if result.Age != 31 {
				t.Errorf("Expected age 31 after update, got %d", result.Age)
			}
		})
	}
}

func TestRawQueryExecDelete(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupRawTestDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert test data
			_, err := setup.DB.Exec(`INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', 25), ('Charlie', 35)`)
			if err != nil {
				t.Fatal(err)
			}

			session := createRawTestSession(setup)

			// Test DELETE with Exec
			rawQuery := orm1.NewRawQuery(session, "DELETE FROM users WHERE age < ?", 30)
			affected, err := rawQuery.Exec(ctx)
			if err != nil {
				t.Fatal(err)
			}

			if affected != 1 {
				t.Errorf("Expected 1 row affected, got %d", affected)
			}

			// Verify the delete
			var results []*RawUser
			verifyQuery := orm1.NewRawQuery(session, "SELECT id, name, age FROM users ORDER BY age")
			err = verifyQuery.ScanAll(ctx, &results)
			if err != nil {
				t.Fatal(err)
			}

			if len(results) != 2 {
				t.Fatalf("Expected 2 users remaining, got %d", len(results))
			}
		})
	}
}

func TestRawQueryExecInsert(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupRawTestDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()
			session := createRawTestSession(setup)

			// Test INSERT with Exec (use ? placeholders for all backends)
			rawQuery := orm1.NewRawQuery(session, "INSERT INTO users (name, age) VALUES (?, ?)", "Dave", 40)

			affected, err := rawQuery.Exec(ctx)
			if err != nil {
				t.Fatal(err)
			}

			if affected != 1 {
				t.Errorf("Expected 1 row affected, got %d", affected)
			}

			// Verify the insert
			var results []*RawUser
			verifyQuery := orm1.NewRawQuery(session, "SELECT id, name, age FROM users WHERE name = ?", "Dave")
			err = verifyQuery.ScanAll(ctx, &results)
			if err != nil {
				t.Fatal(err)
			}

			if len(results) != 1 {
				t.Fatalf("Expected 1 user, got %d", len(results))
			}

			if results[0].Name != "Dave" || results[0].Age != 40 {
				t.Errorf("Expected Dave with age 40, got %s with age %d", results[0].Name, results[0].Age)
			}
		})
	}
}
