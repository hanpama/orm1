package orm1_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/hanpama/orm1"
)

// CompositePerson represents an entity with composite primary key
type CompositePerson struct {
	Key1 int
	Key2 int
	Name string
	Age  int
}

func setupCompositePersonDB(t *testing.T, backend Backend) *DBSetup {
	setup := SetupDB(t, backend)

	// Drop existing tables for PostgreSQL
	setup.DropTables(backend, "composite_persons")

	setup.ExecSchema(t, backend, `
		CREATE TABLE composite_persons (
			key1 INTEGER NOT NULL,
			key2 INTEGER NOT NULL,
			name TEXT NOT NULL,
			age INTEGER NOT NULL,
			PRIMARY KEY (key1, key2)
		);
	`, `
		CREATE TABLE composite_persons (
			key1 INTEGER NOT NULL,
			key2 INTEGER NOT NULL,
			name TEXT NOT NULL,
			age INTEGER NOT NULL,
			PRIMARY KEY (key1, key2)
		);
	`)

	return setup
}

func createCompositePersonTestSession(setup *DBSetup) *orm1.Session {
	factory := setup.NewSessionFactory()
	factory.RegisterEntity(&CompositePerson{},
		orm1.WithTable("composite_persons"),
		orm1.WithPrimaryKey("Key1", "Key2"))

	return factory.CreateSession()
}

func TestCompositeGet(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupCompositePersonDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert test data
			_, err := setup.DB.Exec("INSERT INTO composite_persons (key1, key2, name, age) VALUES (1, 2, 'Alice', 20)")
			if err != nil {
				t.Fatalf("failed to insert: %v", err)
			}

			// Create session and get
			session := createCompositePersonTestSession(setup)
			var found *CompositePerson
			err = session.Get(ctx, &found, orm1.NewKey(1, 2))
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}

			if found == nil {
				t.Fatal("person not found")
			}
			if found.Key1 != 1 || found.Key2 != 2 {
				t.Errorf("expected keys (1,2), got (%d,%d)", found.Key1, found.Key2)
			}
			if found.Name != "Alice" {
				t.Errorf("expected name 'Alice', got '%s'", found.Name)
			}
			if found.Age != 20 {
				t.Errorf("expected age 20, got %d", found.Age)
			}
		})
	}
}

func TestCompositeBatchGet(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupCompositePersonDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert test data
			setup.DB.Exec("INSERT INTO composite_persons (key1, key2, name, age) VALUES (1, 2, 'Alice', 20)")
			setup.DB.Exec("INSERT INTO composite_persons (key1, key2, name, age) VALUES (3, 4, 'Bob', 30)")

			// BatchGet with non-existent key
			session := createCompositePersonTestSession(setup)
			var persons []*CompositePerson
			keys := []orm1.Key{orm1.NewKey(1, 2), orm1.NewKey(-1, -1), orm1.NewKey(3, 4)}
			err := session.BatchGet(ctx, &persons, keys)
			if err != nil {
				t.Fatalf("BatchGet failed: %v", err)
			}

			// Progressive testing: first surface the full structure by comparing against nil
			if diff := cmp.Diff(persons, ([]*CompositePerson)(nil)); diff == "" {
				t.Fatalf("BatchGet returned nil, expected 3 elements (2 non-nil, 1 nil)\nDiff:\n%s", diff)
			}

			// Now we know persons is not nil, check the structure
			// We expect: [Alice, nil, Bob] in the order of requested keys
			t.Logf("Got %d persons (showing full structure):", len(persons))
			for i, p := range persons {
				t.Logf("  [%d]: %+v", i, p)
			}

			// Expected structure based on keys order
			want := []*CompositePerson{
				{Key1: 1, Key2: 2, Name: "Alice", Age: 20},
				nil, // Key (-1, -1) doesn't exist
				{Key1: 3, Key2: 4, Name: "Bob", Age: 30},
			}

			if diff := cmp.Diff(want, persons); diff != "" {
				t.Errorf("BatchGet result mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestCompositeSaveInsert(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupCompositePersonDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Save new entity
			session := createCompositePersonTestSession(setup)
			person := &CompositePerson{
				Key1: 5,
				Key2: 6,
				Name: "Charlie",
				Age:  40,
			}
			err := session.Save(ctx, person)
			if err != nil {
				t.Fatalf("Save failed: %v", err)
			}

			// Verify with new session
			session2 := createCompositePersonTestSession(setup)
			var found *CompositePerson
			err = session2.Get(ctx, &found, orm1.NewKey(5, 6))
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}

			if found == nil {
				t.Fatal("person not found")
			}
			if found.Key1 != 5 || found.Key2 != 6 {
				t.Errorf("expected keys (5,6), got (%d,%d)", found.Key1, found.Key2)
			}
			if found.Name != "Charlie" {
				t.Errorf("expected name 'Charlie', got '%s'", found.Name)
			}
			if found.Age != 40 {
				t.Errorf("expected age 40, got %d", found.Age)
			}
		})
	}
}

func TestCompositeSaveUpdate(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupCompositePersonDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert test data
			setup.DB.Exec("INSERT INTO composite_persons (key1, key2, name, age) VALUES (1, 2, 'Alice', 20)")

			// Get and update
			session := createCompositePersonTestSession(setup)
			var person *CompositePerson
			session.Get(ctx, &person, orm1.NewKey(1, 2))

			person.Name = "Alice2"
			person.Age = 21
			err := session.Save(ctx, person)
			if err != nil {
				t.Fatalf("Save failed: %v", err)
			}

			// Verify with new session
			session2 := createCompositePersonTestSession(setup)
			var found *CompositePerson
			session2.Get(ctx, &found, orm1.NewKey(1, 2))

			if found.Name != "Alice2" {
				t.Errorf("expected name 'Alice2', got '%s'", found.Name)
			}
			if found.Age != 21 {
				t.Errorf("expected age 21, got %d", found.Age)
			}
		})
	}
}

func TestCompositeBatchSave(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupCompositePersonDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert initial data
			setup.DB.Exec("INSERT INTO composite_persons (key1, key2, name, age) VALUES (1, 2, 'Alice', 20)")
			setup.DB.Exec("INSERT INTO composite_persons (key1, key2, name, age) VALUES (3, 4, 'Bob', 30)")

			// Get and modify existing persons
			session := createCompositePersonTestSession(setup)
			var person1, person2 *CompositePerson
			session.Get(ctx, &person1, orm1.NewKey(1, 2))
			session.Get(ctx, &person2, orm1.NewKey(3, 4))

			person1.Name = "Alice2"
			person1.Age = 21
			person2.Name = "Bob2"
			person2.Age = 31

			// Create new persons
			person3 := &CompositePerson{Key1: 7, Key2: 8, Name: "Charlie", Age: 40}
			person4 := &CompositePerson{Key1: 9, Key2: 10, Name: "David", Age: 50}

			// Batch save (mix of updates and inserts)
			err := session.BatchSave(ctx, person1, person2, person3, person4)
			if err != nil {
				t.Fatalf("BatchSave failed: %v", err)
			}

			// Verify with new session
			session2 := createCompositePersonTestSession(setup)
			var found1, found2, found3, found4 *CompositePerson
			session2.Get(ctx, &found1, orm1.NewKey(1, 2))
			session2.Get(ctx, &found2, orm1.NewKey(3, 4))
			session2.Get(ctx, &found3, orm1.NewKey(7, 8))
			session2.Get(ctx, &found4, orm1.NewKey(9, 10))

			if found1.Name != "Alice2" || found1.Age != 21 {
				t.Error("person1 not updated correctly")
			}
			if found2.Name != "Bob2" || found2.Age != 31 {
				t.Error("person2 not updated correctly")
			}
			if found3.Name != "Charlie" || found3.Age != 40 {
				t.Error("person3 not inserted correctly")
			}
			if found4.Name != "David" || found4.Age != 50 {
				t.Error("person4 not inserted correctly")
			}
		})
	}
}

func TestCompositeDelete(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupCompositePersonDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert test data
			setup.DB.Exec("INSERT INTO composite_persons (key1, key2, name, age) VALUES (1, 2, 'Alice', 20)")

			// Get and delete
			session := createCompositePersonTestSession(setup)
			var person *CompositePerson
			session.Get(ctx, &person, orm1.NewKey(1, 2))

			err := session.Delete(ctx, person)
			if err != nil {
				t.Fatalf("Delete failed: %v", err)
			}

			// Verify with new session
			session2 := createCompositePersonTestSession(setup)
			var found *CompositePerson
			session2.Get(ctx, &found, orm1.NewKey(1, 2))

			if found != nil {
				t.Error("person should be deleted")
			}
		})
	}
}

func TestCompositeBatchDelete(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupCompositePersonDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert test data
			setup.DB.Exec("INSERT INTO composite_persons (key1, key2, name, age) VALUES (1, 2, 'Alice', 20)")
			setup.DB.Exec("INSERT INTO composite_persons (key1, key2, name, age) VALUES (3, 4, 'Bob', 30)")

			// Get and batch delete
			session := createCompositePersonTestSession(setup)
			var person1, person2 *CompositePerson
			session.Get(ctx, &person1, orm1.NewKey(1, 2))
			session.Get(ctx, &person2, orm1.NewKey(3, 4))

			err := session.BatchDelete(ctx, person1, person2)
			if err != nil {
				t.Fatalf("BatchDelete failed: %v", err)
			}

			// Verify with new session
			session2 := createCompositePersonTestSession(setup)
			var found1, found2 *CompositePerson
			session2.Get(ctx, &found1, orm1.NewKey(1, 2))
			session2.Get(ctx, &found2, orm1.NewKey(3, 4))

			if found1 != nil {
				t.Error("person1 should be deleted")
			}
			if found2 != nil {
				t.Error("person2 should be deleted")
			}
		})
	}
}

// Helper function to create a string representation of composite key
func keyString(key1, key2 int) string {
	return fmt.Sprintf("%d-%d", key1, key2)
}
