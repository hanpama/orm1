package orm1_test

import (
	"context"
	"testing"

	"github.com/hanpama/orm1"
	"github.com/hanpama/orm1/driver"
)

// RawSimpleAuto - struct for raw query results (no ORM tags)
type RawSimpleAuto struct {
	ID       int64
	Name     string
	Nullable *string
}

func TestRawQueryScanAll(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			registry := orm1.NewRegistry()
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			// Insert test data using raw query
			insertQuery := orm1.NewRawQuery(session, "INSERT INTO simple_auto (name, nullable) VALUES (?, ?), (?, ?), (?, ?)",
				"Alice", "nullable1", "Bob", "nullable2", "Charlie", nil)
			_, err := insertQuery.Exec(ctx)
			if err != nil {
				t.Fatalf("Insert failed: %v", err)
			}

			// Test ScanAll with parameter interpolation
			rawQuery := orm1.NewRawQuery(session, "SELECT id, name, nullable FROM simple_auto WHERE name IN (?, ?, ?) ORDER BY name", "Alice", "Bob", "Charlie")

			var results []*RawSimpleAuto
			err = rawQuery.ScanAll(ctx, &results)
			if err != nil {
				t.Fatalf("ScanAll failed: %v", err)
			}

			if len(results) != 3 {
				t.Fatalf("Expected 3 results, got %d", len(results))
			}

			// Check first row (Alice)
			if results[0].Name != "Alice" {
				t.Errorf("Expected name 'Alice', got %v", results[0].Name)
			}
			if results[0].Nullable == nil || *results[0].Nullable != "nullable1" {
				t.Errorf("Expected nullable 'nullable1', got %v", results[0].Nullable)
			}

			// Check second row (Bob)
			if results[1].Name != "Bob" {
				t.Errorf("Expected name 'Bob', got %v", results[1].Name)
			}

			// Cleanup
			cleanupQuery := orm1.NewRawQuery(session, "DELETE FROM simple_auto WHERE name IN (?, ?, ?)", "Alice", "Bob", "Charlie")
			cleanupQuery.Exec(ctx)
		})
	}
}

func TestRawQueryScanOne(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			registry := orm1.NewRegistry()
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			// Insert test data
			insertQuery := orm1.NewRawQuery(session, "INSERT INTO simple_auto (name, nullable) VALUES (?, ?)", "Alice", "test_value")
			_, err := insertQuery.Exec(ctx)
			if err != nil {
				t.Fatalf("Insert failed: %v", err)
			}

			// Test ScanOne
			rawQuery := orm1.NewRawQuery(session, "SELECT id, name, nullable FROM simple_auto WHERE name = ?", "Alice")

			var result RawSimpleAuto
			err = rawQuery.ScanOne(ctx, &result)
			if err != nil {
				t.Fatalf("ScanOne failed: %v", err)
			}

			if result.Name != "Alice" {
				t.Errorf("Expected name 'Alice', got %v", result.Name)
			}
			if result.Nullable == nil || *result.Nullable != "test_value" {
				t.Errorf("Expected nullable 'test_value', got %v", result.Nullable)
			}

			// Cleanup
			cleanupQuery := orm1.NewRawQuery(session, "DELETE FROM simple_auto WHERE name = ?", "Alice")
			cleanupQuery.Exec(ctx)
		})
	}
}

func TestRawQueryScanOneEmpty(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			registry := orm1.NewRegistry()
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			// Test ScanOne with no results
			rawQuery := orm1.NewRawQuery(session, "SELECT id, name, nullable FROM simple_auto WHERE name = ?", "NonExistentUser")

			var result RawSimpleAuto
			err := rawQuery.ScanOne(ctx, &result)
			if err != nil {
				t.Fatalf("ScanOne failed: %v", err)
			}

			// When no rows, struct should remain zero-valued
			if result.Name != "" {
				t.Errorf("Expected empty name, got %v", result.Name)
			}
		})
	}
}

func TestRawQueryMultipleParams(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			registry := orm1.NewRegistry()
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			// Insert test data
			insertQuery := orm1.NewRawQuery(session, "INSERT INTO simple_auto (name, nullable) VALUES (?, ?), (?, ?), (?, ?)",
				"Alice", "a", "Bob", "b", "Charlie", "c")
			_, err := insertQuery.Exec(ctx)
			if err != nil {
				t.Fatalf("Insert failed: %v", err)
			}

			// Test with multiple parameters
			rawQuery := orm1.NewRawQuery(session, "SELECT id, name, nullable FROM simple_auto WHERE name >= ? AND name <= ? ORDER BY name", "Alice", "Bob")

			var results []*RawSimpleAuto
			err = rawQuery.ScanAll(ctx, &results)
			if err != nil {
				t.Fatalf("ScanAll failed: %v", err)
			}

			if len(results) != 2 {
				t.Fatalf("Expected 2 results, got %d", len(results))
			}

			if results[0].Name != "Alice" {
				t.Errorf("Expected name 'Alice', got %v", results[0].Name)
			}
			if results[1].Name != "Bob" {
				t.Errorf("Expected name 'Bob', got %v", results[1].Name)
			}

			// Cleanup
			cleanupQuery := orm1.NewRawQuery(session, "DELETE FROM simple_auto WHERE name IN (?, ?, ?)", "Alice", "Bob", "Charlie")
			cleanupQuery.Exec(ctx)
		})
	}
}

func TestRawQueryWithNullValues(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			registry := orm1.NewRegistry()
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			// Insert test data with NULL nullable
			insertQuery := orm1.NewRawQuery(session, "INSERT INTO simple_auto (name, nullable) VALUES (?, ?), (?, ?)",
				"Alice", "has_value", "Bob", nil)
			_, err := insertQuery.Exec(ctx)
			if err != nil {
				t.Fatalf("Insert failed: %v", err)
			}

			// Test query that includes NULL values
			rawQuery := orm1.NewRawQuery(session, "SELECT id, name, nullable FROM simple_auto WHERE name IN (?, ?) ORDER BY name", "Alice", "Bob")

			var results []*RawSimpleAuto
			err = rawQuery.ScanAll(ctx, &results)
			if err != nil {
				t.Fatalf("ScanAll failed: %v", err)
			}

			if len(results) != 2 {
				t.Fatalf("Expected 2 results, got %d", len(results))
			}

			// Check that non-NULL is handled properly
			if results[0].Nullable == nil || *results[0].Nullable != "has_value" {
				t.Errorf("Expected nullable 'has_value', got %v", results[0].Nullable)
			}
			// Check that NULL is handled properly
			if results[1].Nullable != nil {
				t.Errorf("Expected nullable nil, got %v", *results[1].Nullable)
			}

			// Cleanup
			cleanupQuery := orm1.NewRawQuery(session, "DELETE FROM simple_auto WHERE name IN (?, ?)", "Alice", "Bob")
			cleanupQuery.Exec(ctx)
		})
	}
}

func TestRawQueryRows(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			registry := orm1.NewRegistry()
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			// Insert test data
			insertQuery := orm1.NewRawQuery(session, "INSERT INTO simple_auto (name, nullable) VALUES (?, ?), (?, ?)",
				"Alice", "a", "Bob", "b")
			_, err := insertQuery.Exec(ctx)
			if err != nil {
				t.Fatalf("Insert failed: %v", err)
			}

			// Test Rows for manual iteration
			rawQuery := orm1.NewRawQuery(session, "SELECT id, name, nullable FROM simple_auto WHERE name IN (?, ?) ORDER BY name", "Alice", "Bob")

			rows, err := rawQuery.Rows(ctx)
			if err != nil {
				t.Fatalf("Rows failed: %v", err)
			}
			defer rows.Close()

			var users []RawSimpleAuto
			for rows.Next() {
				var user RawSimpleAuto
				if err := rows.Scan(&user.ID, &user.Name, &user.Nullable); err != nil {
					t.Fatalf("Scan failed: %v", err)
				}
				users = append(users, user)
			}

			if len(users) != 2 {
				t.Fatalf("Expected 2 users, got %d", len(users))
			}

			if users[0].Name != "Alice" {
				t.Errorf("Expected first user 'Alice', got %v", users[0].Name)
			}
			if users[1].Name != "Bob" {
				t.Errorf("Expected second user 'Bob', got %v", users[1].Name)
			}

			// Cleanup
			cleanupQuery := orm1.NewRawQuery(session, "DELETE FROM simple_auto WHERE name IN (?, ?)", "Alice", "Bob")
			cleanupQuery.Exec(ctx)
		})
	}
}

func TestRawQueryExec(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			registry := orm1.NewRegistry()
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			// Insert test data
			insertQuery := orm1.NewRawQuery(session, "INSERT INTO simple_auto (name, nullable) VALUES (?, ?), (?, ?), (?, ?)",
				"Alice", "a", "Bob", "b", "Charlie", "c")
			_, err := insertQuery.Exec(ctx)
			if err != nil {
				t.Fatalf("Insert failed: %v", err)
			}

			// Test UPDATE with Exec
			rawQuery := orm1.NewRawQuery(session, "UPDATE simple_auto SET nullable = ? WHERE name = ?", "updated", "Alice")
			affected, err := rawQuery.Exec(ctx)
			if err != nil {
				t.Fatalf("Exec failed: %v", err)
			}

			if affected != 1 {
				t.Errorf("Expected 1 row affected, got %d", affected)
			}

			// Verify the update
			var result RawSimpleAuto
			verifyQuery := orm1.NewRawQuery(session, "SELECT id, name, nullable FROM simple_auto WHERE name = ?", "Alice")
			err = verifyQuery.ScanOne(ctx, &result)
			if err != nil {
				t.Fatalf("ScanOne failed: %v", err)
			}

			if result.Nullable == nil || *result.Nullable != "updated" {
				t.Errorf("Expected nullable 'updated' after update, got %v", result.Nullable)
			}

			// Cleanup
			cleanupQuery := orm1.NewRawQuery(session, "DELETE FROM simple_auto WHERE name IN (?, ?, ?)", "Alice", "Bob", "Charlie")
			cleanupQuery.Exec(ctx)
		})
	}
}

func TestRawQueryExecDelete(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			registry := orm1.NewRegistry()
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			// Insert test data
			insertQuery := orm1.NewRawQuery(session, "INSERT INTO simple_auto (name, nullable) VALUES (?, ?), (?, ?), (?, ?)",
				"Alice", "a", "Bob", "b", "Charlie", "c")
			_, err := insertQuery.Exec(ctx)
			if err != nil {
				t.Fatalf("Insert failed: %v", err)
			}

			// Test DELETE with Exec
			rawQuery := orm1.NewRawQuery(session, "DELETE FROM simple_auto WHERE name = ?", "Bob")
			affected, err := rawQuery.Exec(ctx)
			if err != nil {
				t.Fatalf("Exec failed: %v", err)
			}

			if affected != 1 {
				t.Errorf("Expected 1 row affected, got %d", affected)
			}

			// Verify the delete
			var results []*RawSimpleAuto
			verifyQuery := orm1.NewRawQuery(session, "SELECT id, name, nullable FROM simple_auto WHERE name IN (?, ?, ?) ORDER BY name", "Alice", "Bob", "Charlie")
			err = verifyQuery.ScanAll(ctx, &results)
			if err != nil {
				t.Fatalf("ScanAll failed: %v", err)
			}

			if len(results) != 2 {
				t.Fatalf("Expected 2 users remaining, got %d", len(results))
			}

			// Cleanup
			cleanupQuery := orm1.NewRawQuery(session, "DELETE FROM simple_auto WHERE name IN (?, ?)", "Alice", "Charlie")
			cleanupQuery.Exec(ctx)
		})
	}
}

func TestRawQueryExecInsert(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			registry := orm1.NewRegistry()
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			// Test INSERT with Exec (use ? placeholders for all backends)
			rawQuery := orm1.NewRawQuery(session, "INSERT INTO simple_auto (name, nullable) VALUES (?, ?)", "Dave", "dave_value")

			affected, err := rawQuery.Exec(ctx)
			if err != nil {
				t.Fatalf("Exec failed: %v", err)
			}

			if affected != 1 {
				t.Errorf("Expected 1 row affected, got %d", affected)
			}

			// Verify the insert
			var results []*RawSimpleAuto
			verifyQuery := orm1.NewRawQuery(session, "SELECT id, name, nullable FROM simple_auto WHERE name = ?", "Dave")
			err = verifyQuery.ScanAll(ctx, &results)
			if err != nil {
				t.Fatalf("ScanAll failed: %v", err)
			}

			if len(results) != 1 {
				t.Fatalf("Expected 1 user, got %d", len(results))
			}

			if results[0].Name != "Dave" || results[0].Nullable == nil || *results[0].Nullable != "dave_value" {
				t.Errorf("Expected Dave with nullable 'dave_value', got %s with nullable %v", results[0].Name, results[0].Nullable)
			}

			// Cleanup
			cleanupQuery := orm1.NewRawQuery(session, "DELETE FROM simple_auto WHERE name = ?", "Dave")
			cleanupQuery.Exec(ctx)
		})
	}
}
