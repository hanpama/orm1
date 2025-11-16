package orm1_test

import (
	"context"
	"strings"
	"testing"

	"github.com/hanpama/orm1"
	"github.com/hanpama/orm1/driver"
)

// RawUser - basic struct for E2E testing
type RawUser struct {
	ID        int64
	Name      string
	Email     *string    // Nullable field
	Ignored   string     `orm1:"-"`          // Should be ignored
	CustomCol string     `orm1:"column:alt"` // Custom column name
	Child     *RawUser   `orm1:"child"`      // Should be skipped (child tag)
	Slice     []string   // Should be skipped (slice)
	Pointer   *RawUser   // Should be skipped (struct pointer)
}

// TestRawQuery_ScanOne tests single-row scanning with all field mapping features
func TestRawQuery_ScanOne(t *testing.T) {
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

			// Setup: Insert with custom column name
			email := "user@example.com"
			_, err := orm1.NewRawQuery(session,
				"INSERT INTO raw_users (name, email, alt, ignored) VALUES (?, ?, ?, ?)",
				"Alice", email, "custom_value", "should_be_ignored").Exec(ctx)
			if err != nil {
				t.Fatalf("Insert failed: %v", err)
			}

			// Test: ScanOne with all field types
			var result RawUser
			err = orm1.NewRawQuery(session,
				"SELECT id, name, email, alt, ignored as unmapped FROM raw_users WHERE name = ?",
				"Alice").ScanOne(ctx, &result)
			if err != nil {
				t.Fatalf("ScanOne failed: %v", err)
			}

			// Verify: Regular fields
			if result.Name != "Alice" {
				t.Errorf("Expected name 'Alice', got %v", result.Name)
			}

			// Verify: Nullable field
			if result.Email == nil || *result.Email != email {
				t.Errorf("Expected email '%s', got %v", email, result.Email)
			}

			// Verify: Custom column name
			if result.CustomCol != "custom_value" {
				t.Errorf("Expected CustomCol 'custom_value', got %v", result.CustomCol)
			}

			// Verify: Ignored field remains zero value
			if result.Ignored != "" {
				t.Errorf("Expected Ignored to be empty, got %v", result.Ignored)
			}

			// Verify: Child/Slice/Pointer fields remain zero value (should be filtered)
			if result.Child != nil {
				t.Errorf("Expected Child to be nil, got %v", result.Child)
			}
			if result.Slice != nil {
				t.Errorf("Expected Slice to be nil, got %v", result.Slice)
			}
			if result.Pointer != nil {
				t.Errorf("Expected Pointer to be nil, got %v", result.Pointer)
			}

			// Cleanup
			orm1.NewRawQuery(session, "DELETE FROM raw_users WHERE name = ?", "Alice").Exec(ctx)
		})
	}
}

// TestRawQuery_ScanOne_Empty tests empty result handling
func TestRawQuery_ScanOne_Empty(t *testing.T) {
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

			// Test: ScanOne with no results should return nil without error
			var result RawUser
			err := orm1.NewRawQuery(session,
				"SELECT id, name, email, alt FROM raw_users WHERE name = ?",
				"NonExistent").ScanOne(ctx, &result)
			if err != nil {
				t.Fatalf("ScanOne should succeed with empty result, got error: %v", err)
			}

			// Verify: Result remains zero-valued
			if result.Name != "" {
				t.Errorf("Expected empty result, got name: %v", result.Name)
			}
		})
	}
}

// TestRawQuery_ScanOne_Validation tests input validation
func TestRawQuery_ScanOne_Validation(t *testing.T) {
	ctx := context.Background()
	registry := orm1.NewRegistry()
	factory := orm1.NewSessionFactory(registry, sqliteDriver)
	session := factory.CreateSession()

	query := orm1.NewRawQuery(session, "SELECT 1")

	// Not a pointer
	var notPointer RawUser
	err := query.ScanOne(ctx, notPointer)
	if err == nil || !strings.Contains(err.Error(), "must be a pointer to struct") {
		t.Errorf("Expected 'must be a pointer to struct' error, got: %v", err)
	}

	// Pointer to non-struct
	var notStruct string
	err = query.ScanOne(ctx, &notStruct)
	if err == nil || !strings.Contains(err.Error(), "must be a pointer to struct") {
		t.Errorf("Expected 'must be a pointer to struct' error, got: %v", err)
	}
}

// TestRawQuery_ScanAll tests multi-row scanning with field filtering
func TestRawQuery_ScanAll(t *testing.T) {
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

			// Setup: Insert multiple rows with NULL values
			email1 := "alice@example.com"
			_, err := orm1.NewRawQuery(session,
				"INSERT INTO raw_users (name, email, alt) VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?)",
				"Alice", email1, "alt1",
				"Bob", nil, "alt2",
				"Charlie", "charlie@example.com", "alt3").Exec(ctx)
			if err != nil {
				t.Fatalf("Insert failed: %v", err)
			}

			// Test: ScanAll with custom column, nullable, and unmapped column
			var results []*RawUser
			err = orm1.NewRawQuery(session,
				"SELECT id, name, email, alt, name as unmapped_col FROM raw_users ORDER BY name").ScanAll(ctx, &results)
			if err != nil {
				t.Fatalf("ScanAll failed: %v", err)
			}

			// Verify: Count
			if len(results) != 3 {
				t.Fatalf("Expected 3 results, got %d", len(results))
			}

			// Verify: First row with non-NULL email
			if results[0].Name != "Alice" {
				t.Errorf("Expected name 'Alice', got %v", results[0].Name)
			}
			if results[0].Email == nil || *results[0].Email != email1 {
				t.Errorf("Expected email '%s', got %v", email1, results[0].Email)
			}
			if results[0].CustomCol != "alt1" {
				t.Errorf("Expected CustomCol 'alt1', got %v", results[0].CustomCol)
			}

			// Verify: Second row with NULL email
			if results[1].Name != "Bob" {
				t.Errorf("Expected name 'Bob', got %v", results[1].Name)
			}
			if results[1].Email != nil {
				t.Errorf("Expected email nil, got %v", *results[1].Email)
			}

			// Cleanup
			orm1.NewRawQuery(session, "DELETE FROM raw_users").Exec(ctx)
		})
	}
}

// TestRawQuery_ScanAll_Validation tests input validation
func TestRawQuery_ScanAll_Validation(t *testing.T) {
	ctx := context.Background()
	registry := orm1.NewRegistry()
	factory := orm1.NewSessionFactory(registry, sqliteDriver)
	session := factory.CreateSession()

	query := orm1.NewRawQuery(session, "SELECT 1")

	// Not a pointer
	var notPointer []RawUser
	err := query.ScanAll(ctx, notPointer)
	if err == nil || !strings.Contains(err.Error(), "must be a pointer to slice") {
		t.Errorf("Expected 'must be a pointer to slice' error, got: %v", err)
	}

	// Pointer to non-slice
	var notSlice RawUser
	err = query.ScanAll(ctx, &notSlice)
	if err == nil || !strings.Contains(err.Error(), "must be a pointer to slice") {
		t.Errorf("Expected 'must be a pointer to slice' error, got: %v", err)
	}

	// Slice elements not pointers
	var notPointers []RawUser
	err = query.ScanAll(ctx, &notPointers)
	if err == nil || !strings.Contains(err.Error(), "must be pointers") {
		t.Errorf("Expected 'must be pointers' error, got: %v", err)
	}

	// Slice elements not structs
	var notStructs []*string
	err = query.ScanAll(ctx, &notStructs)
	if err == nil || !strings.Contains(err.Error(), "must be pointers to structs") {
		t.Errorf("Expected 'must be pointers to structs' error, got: %v", err)
	}
}
