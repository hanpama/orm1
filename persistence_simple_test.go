package orm1_test

import (
	"context"
	"testing"

	"github.com/hanpama/orm1"
)

// SimpleUser represents a user with auto-generated ID
type SimpleUser struct {
	ID    int64 `orm1:"auto"`
	Name  string
	Email string
	Age   int
}

func setupSimpleUserDB(t *testing.T, backend Backend) *DBSetup {
	setup := SetupDB(t, backend)

	// Drop existing tables for PostgreSQL
	setup.DropTables(backend, "users")

	setup.ExecSchema(t, backend, `
		CREATE TABLE users (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL,
			email TEXT NOT NULL,
			age INTEGER NOT NULL
		);
	`, `
		CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL,
			age INTEGER NOT NULL
		);
	`)

	return setup
}

func createSimpleUserTestSession(setup *DBSetup) *orm1.Session {
	factory := setup.NewSessionFactory()
	factory.RegisterEntity(&SimpleUser{}, orm1.WithTable("users"))

	return factory.CreateSession()
}

func TestSimpleGet(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupSimpleUserDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert test data
			var user1ID int64
			err := setup.DB.QueryRow("INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 20) RETURNING id").Scan(&user1ID)
			if err != nil {
				t.Fatalf("failed to insert: %v", err)
			}

			// Create session and get
			session := createSimpleUserTestSession(setup)
			var found *SimpleUser
			err = session.Get(ctx, &found, orm1.NewKey(user1ID))
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}

			if found == nil {
				t.Fatal("user not found")
			}
			if found.ID != user1ID {
				t.Errorf("expected ID %d, got %d", user1ID, found.ID)
			}
			if found.Name != "Alice" {
				t.Errorf("expected name 'Alice', got '%s'", found.Name)
			}
			if found.Email != "alice@example.com" {
				t.Errorf("expected email 'alice@example.com', got '%s'", found.Email)
			}
			if found.Age != 20 {
				t.Errorf("expected age 20, got %d", found.Age)
			}
		})
	}
}

func TestSimpleBatchGet(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupSimpleUserDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert test data
			var id1, id2 int64
			setup.DB.QueryRow("INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 20) RETURNING id").Scan(&id1)
			setup.DB.QueryRow("INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@example.com', 30) RETURNING id").Scan(&id2)

			// BatchGet with non-existent key
			session := createSimpleUserTestSession(setup)
			var users []*SimpleUser
			keys := []orm1.Key{orm1.NewKey(id1), orm1.NewKey(-1), orm1.NewKey(id2)}
			err := session.BatchGet(ctx, &users, keys)
			if err != nil {
				t.Fatalf("BatchGet failed: %v", err)
			}

			if len(users) != 3 {
				t.Fatalf("expected 3 users, got %d", len(users))
			}

			// Count non-nil users
			nonNil := 0
			foundIDs := make(map[int64]bool)
			for _, u := range users {
				if u != nil {
					nonNil++
					foundIDs[u.ID] = true
				}
			}

			if nonNil != 2 {
				t.Errorf("expected 2 non-nil users, got %d", nonNil)
			}
			if !foundIDs[id1] || !foundIDs[id2] {
				t.Error("expected to find both users")
			}
		})
	}
}

func TestSimpleSaveInsert(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupSimpleUserDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Save new entity
			session := createSimpleUserTestSession(setup)
			user := &SimpleUser{
				Name:  "Charlie",
				Email: "charlie@example.com",
				Age:   40,
			}
			err := session.Save(ctx, user)
			if err != nil {
				t.Fatalf("Save failed: %v", err)
			}

			if user.ID == 0 {
				t.Error("expected ID to be set after insert")
			}

			// Verify with new session
			session2 := createSimpleUserTestSession(setup)
			var found *SimpleUser
			err = session2.Get(ctx, &found, orm1.NewKey(user.ID))
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}

			if found == nil {
				t.Fatal("user not found")
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

func TestSimpleSaveUpdate(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupSimpleUserDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert test data
			var userID int64
			setup.DB.QueryRow("INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 20) RETURNING id").Scan(&userID)

			// Get and update
			session := createSimpleUserTestSession(setup)
			var user *SimpleUser
			session.Get(ctx, &user, orm1.NewKey(userID))

			user.Name = "Alice2"
			user.Age = 21
			err := session.Save(ctx, user)
			if err != nil {
				t.Fatalf("Save failed: %v", err)
			}

			// Verify with new session
			session2 := createSimpleUserTestSession(setup)
			var found *SimpleUser
			session2.Get(ctx, &found, orm1.NewKey(userID))

			if found.Name != "Alice2" {
				t.Errorf("expected name 'Alice2', got '%s'", found.Name)
			}
			if found.Age != 21 {
				t.Errorf("expected age 21, got %d", found.Age)
			}
		})
	}
}

func TestSimpleBatchSave(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupSimpleUserDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert initial data
			var id1, id2 int64
			setup.DB.QueryRow("INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 20) RETURNING id").Scan(&id1)
			setup.DB.QueryRow("INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@example.com', 30) RETURNING id").Scan(&id2)

			// Get and modify existing users
			session := createSimpleUserTestSession(setup)
			var user1, user2 *SimpleUser
			session.Get(ctx, &user1, orm1.NewKey(id1))
			session.Get(ctx, &user2, orm1.NewKey(id2))

			user1.Name = "Alice2"
			user1.Age = 21
			user2.Name = "Bob2"
			user2.Age = 31

			// Create new users
			user3 := &SimpleUser{Name: "Charlie", Email: "charlie@example.com", Age: 40}
			user4 := &SimpleUser{Name: "David", Email: "david@example.com", Age: 50}

			// Batch save (mix of updates and inserts)
			err := session.BatchSave(ctx, user1, user2, user3, user4)
			if err != nil {
				t.Fatalf("BatchSave failed: %v", err)
			}

			// Verify with new session
			session2 := createSimpleUserTestSession(setup)
			var found1, found2, found3, found4 *SimpleUser
			session2.Get(ctx, &found1, orm1.NewKey(id1))
			session2.Get(ctx, &found2, orm1.NewKey(id2))
			session2.Get(ctx, &found3, orm1.NewKey(user3.ID))
			session2.Get(ctx, &found4, orm1.NewKey(user4.ID))

			if found1.Name != "Alice2" || found1.Age != 21 {
				t.Error("user1 not updated correctly")
			}
			if found2.Name != "Bob2" || found2.Age != 31 {
				t.Error("user2 not updated correctly")
			}
			if found3.Name != "Charlie" || found3.Age != 40 {
				t.Error("user3 not inserted correctly")
			}
			if found4.Name != "David" || found4.Age != 50 {
				t.Error("user4 not inserted correctly")
			}
		})
	}
}

func TestSimpleDelete(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupSimpleUserDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert test data
			var userID int64
			setup.DB.QueryRow("INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 20) RETURNING id").Scan(&userID)

			// Get and delete
			session := createSimpleUserTestSession(setup)
			var user *SimpleUser
			session.Get(ctx, &user, orm1.NewKey(userID))

			err := session.Delete(ctx, user)
			if err != nil {
				t.Fatalf("Delete failed: %v", err)
			}

			// Verify with new session
			session2 := createSimpleUserTestSession(setup)
			var found *SimpleUser
			session2.Get(ctx, &found, orm1.NewKey(userID))

			if found != nil {
				t.Error("user should be deleted")
			}
		})
	}
}

func TestSimpleBatchDelete(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupSimpleUserDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert test data
			var id1, id2 int64
			setup.DB.QueryRow("INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 20) RETURNING id").Scan(&id1)
			setup.DB.QueryRow("INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@example.com', 30) RETURNING id").Scan(&id2)

			// Get and batch delete
			session := createSimpleUserTestSession(setup)
			var user1, user2 *SimpleUser
			session.Get(ctx, &user1, orm1.NewKey(id1))
			session.Get(ctx, &user2, orm1.NewKey(id2))

			err := session.BatchDelete(ctx, user1, user2)
			if err != nil {
				t.Fatalf("BatchDelete failed: %v", err)
			}

			// Verify with new session
			session2 := createSimpleUserTestSession(setup)
			var found1, found2 *SimpleUser
			session2.Get(ctx, &found1, orm1.NewKey(id1))
			session2.Get(ctx, &found2, orm1.NewKey(id2))

			if found1 != nil {
				t.Error("user1 should be deleted")
			}
			if found2 != nil {
				t.Error("user2 should be deleted")
			}
		})
	}
}
