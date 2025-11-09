package orm1_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/hanpama/orm1"
)

// AssignedUser represents a user with user-assigned UUID primary key
type AssignedUser struct {
	ID    string
	Name  string
	Email string
	Age   int
}

func setupAssignedUserDB(t *testing.T, backend Backend) *DBSetup {
	setup := SetupDB(t, backend)

	// Drop existing tables for PostgreSQL
	setup.DropTables(backend, "assigned_users")

	setup.ExecSchema(t, backend, `
		CREATE TABLE assigned_users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL,
			age INTEGER NOT NULL
		);
	`, `
		CREATE TABLE assigned_users (
			id UUID PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL,
			age INTEGER NOT NULL
		);
	`)

	return setup
}

func createAssignedUserTestSession(setup *DBSetup) *orm1.Session {
	factory := setup.NewSessionFactory()
	factory.RegisterEntity(&AssignedUser{}, orm1.WithTable("assigned_users"))
	return factory.CreateSession()
}

func TestAssignedGet(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupAssignedUserDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert test data
			user1ID := uuid.New().String()
			_, err := setup.DB.Exec("INSERT INTO assigned_users (id, name, email, age) VALUES ($1, 'Alice', 'alice@example.com', 20)", user1ID)
			if err != nil {
				t.Fatalf("failed to insert: %v", err)
			}

			// Create session and get
			session := createAssignedUserTestSession(setup)
			var found *AssignedUser
			err = session.Get(ctx, &found, orm1.NewKey(user1ID))
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}

			if found == nil {
				t.Fatal("user not found")
			}
			if found.ID != user1ID {
				t.Errorf("expected ID %s, got %s", user1ID, found.ID)
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

func TestAssignedBatchGet(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupAssignedUserDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert test data
			id1 := uuid.New().String()
			id2 := uuid.New().String()
			setup.DB.Exec("INSERT INTO assigned_users (id, name, email, age) VALUES ($1, 'Alice', 'alice@example.com', 20)", id1)
			setup.DB.Exec("INSERT INTO assigned_users (id, name, email, age) VALUES ($1, 'Bob', 'bob@example.com', 30)", id2)

			// BatchGet with non-existent key
			session := createAssignedUserTestSession(setup)
			var users []*AssignedUser
			nonExistentID := uuid.New().String()
			keys := []orm1.Key{orm1.NewKey(id1), orm1.NewKey(nonExistentID), orm1.NewKey(id2)}
			err := session.BatchGet(ctx, &users, keys)
			if err != nil {
				t.Fatalf("BatchGet failed: %v", err)
			}

			if len(users) != 3 {
				t.Fatalf("expected 3 users, got %d", len(users))
			}

			// Count non-nil users
			nonNil := 0
			foundIDs := make(map[string]bool)
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

func TestAssignedSaveInsert(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupAssignedUserDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Save new entity with assigned ID
			session := createAssignedUserTestSession(setup)
			userID := uuid.New().String()
			user := &AssignedUser{
				ID:    userID,
				Name:  "Charlie",
				Email: "charlie@example.com",
				Age:   40,
			}
			err := session.Save(ctx, user)
			if err != nil {
				t.Fatalf("Save failed: %v", err)
			}

			// Verify with new session
			session2 := createAssignedUserTestSession(setup)
			var found *AssignedUser
			err = session2.Get(ctx, &found, orm1.NewKey(userID))
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}

			if found == nil {
				t.Fatal("user not found")
			}
			if found.ID != userID {
				t.Errorf("expected ID %s, got %s", userID, found.ID)
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

func TestAssignedSaveUpdate(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupAssignedUserDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert test data
			userID := uuid.New().String()
			setup.DB.Exec("INSERT INTO assigned_users (id, name, email, age) VALUES ($1, 'Alice', 'alice@example.com', 20)", userID)

			// Get and update
			session := createAssignedUserTestSession(setup)
			var user *AssignedUser
			session.Get(ctx, &user, orm1.NewKey(userID))

			user.Name = "Alice2"
			user.Age = 21
			err := session.Save(ctx, user)
			if err != nil {
				t.Fatalf("Save failed: %v", err)
			}

			// Verify with new session
			session2 := createAssignedUserTestSession(setup)
			var found *AssignedUser
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

func TestAssignedBatchSave(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupAssignedUserDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert initial data
			id1 := uuid.New().String()
			id2 := uuid.New().String()
			setup.DB.Exec("INSERT INTO assigned_users (id, name, email, age) VALUES ($1, 'Alice', 'alice@example.com', 20)", id1)
			setup.DB.Exec("INSERT INTO assigned_users (id, name, email, age) VALUES ($1, 'Bob', 'bob@example.com', 30)", id2)

			// Get and modify existing users
			session := createAssignedUserTestSession(setup)
			var user1, user2 *AssignedUser
			session.Get(ctx, &user1, orm1.NewKey(id1))
			session.Get(ctx, &user2, orm1.NewKey(id2))

			user1.Name = "Alice2"
			user1.Age = 21
			user2.Name = "Bob2"
			user2.Age = 31

			// Create new users with assigned IDs
			id3 := uuid.New().String()
			id4 := uuid.New().String()
			user3 := &AssignedUser{ID: id3, Name: "Charlie", Email: "charlie@example.com", Age: 40}
			user4 := &AssignedUser{ID: id4, Name: "David", Email: "david@example.com", Age: 50}

			// Batch save (mix of updates and inserts)
			err := session.BatchSave(ctx, user1, user2, user3, user4)
			if err != nil {
				t.Fatalf("BatchSave failed: %v", err)
			}

			// Verify with new session
			session2 := createAssignedUserTestSession(setup)
			var found1, found2, found3, found4 *AssignedUser
			session2.Get(ctx, &found1, orm1.NewKey(id1))
			session2.Get(ctx, &found2, orm1.NewKey(id2))
			session2.Get(ctx, &found3, orm1.NewKey(id3))
			session2.Get(ctx, &found4, orm1.NewKey(id4))

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

func TestAssignedDelete(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupAssignedUserDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert test data
			userID := uuid.New().String()
			setup.DB.Exec("INSERT INTO assigned_users (id, name, email, age) VALUES ($1, 'Alice', 'alice@example.com', 20)", userID)

			// Get and delete
			session := createAssignedUserTestSession(setup)
			var user *AssignedUser
			session.Get(ctx, &user, orm1.NewKey(userID))

			err := session.Delete(ctx, user)
			if err != nil {
				t.Fatalf("Delete failed: %v", err)
			}

			// Verify with new session
			session2 := createAssignedUserTestSession(setup)
			var found *AssignedUser
			session2.Get(ctx, &found, orm1.NewKey(userID))

			if found != nil {
				t.Error("user should be deleted")
			}
		})
	}
}

func TestAssignedBatchDelete(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupAssignedUserDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert test data
			id1 := uuid.New().String()
			id2 := uuid.New().String()
			setup.DB.Exec("INSERT INTO assigned_users (id, name, email, age) VALUES ($1, 'Alice', 'alice@example.com', 20)", id1)
			setup.DB.Exec("INSERT INTO assigned_users (id, name, email, age) VALUES ($1, 'Bob', 'bob@example.com', 30)", id2)

			// Get and batch delete
			session := createAssignedUserTestSession(setup)
			var user1, user2 *AssignedUser
			session.Get(ctx, &user1, orm1.NewKey(id1))
			session.Get(ctx, &user2, orm1.NewKey(id2))

			err := session.BatchDelete(ctx, user1, user2)
			if err != nil {
				t.Fatalf("BatchDelete failed: %v", err)
			}

			// Verify with new session
			session2 := createAssignedUserTestSession(setup)
			var found1, found2 *AssignedUser
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
