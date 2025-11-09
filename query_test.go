package orm1_test

import (
	"context"
	"testing"

	"github.com/hanpama/orm1"
)

// QueryUser represents a user entity for query tests
type QueryUser struct {
	ID    int64 `orm1:"auto"`
	Name  string
	Email string
	Age   int
}

// QueryPost represents a blog post entity for query tests
type QueryPost struct {
	ID       int64 `orm1:"auto"`
	AuthorID int64
	Title    string
	Views    int
}

func setupQueryTestDB(t *testing.T, backend Backend) *DBSetup {
	setup := SetupDB(t, backend)

	// Drop existing tables for PostgreSQL
	setup.DropTables(backend, "query_posts", "query_users")

	setup.ExecSchema(t, backend, `
		CREATE TABLE query_users (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL,
			email TEXT NOT NULL,
			age INTEGER NOT NULL
		);
		CREATE TABLE query_posts (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			author_id INTEGER NOT NULL,
			title TEXT NOT NULL,
			views INTEGER NOT NULL,
			FOREIGN KEY (author_id) REFERENCES query_users(id)
		);
	`, `
		CREATE TABLE query_users (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT NOT NULL,
			age INTEGER NOT NULL
		);
		CREATE TABLE query_posts (
			id SERIAL PRIMARY KEY,
			author_id BIGINT NOT NULL,
			title TEXT NOT NULL,
			views INTEGER NOT NULL,
			FOREIGN KEY (author_id) REFERENCES query_users(id)
		);
	`)

	return setup
}

func createQueryTestSession(setup *DBSetup) *orm1.Session {
	factory := setup.NewSessionFactory()
	factory.RegisterEntity(&QueryUser{}, orm1.WithTable("query_users"))
	factory.RegisterEntity(&QueryPost{}, orm1.WithTable("query_posts"))

	return factory.CreateSession()
}

func TestQueryFetchAll(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupQueryTestDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert test data
			var id1, id2, id3 int64
			setup.DB.QueryRow("INSERT INTO query_users (name, email, age) VALUES ('Alice', 'alice@example.com', 30) RETURNING id").Scan(&id1)
			setup.DB.QueryRow("INSERT INTO query_users (name, email, age) VALUES ('Bob', 'bob@example.com', 25) RETURNING id").Scan(&id2)
			setup.DB.QueryRow("INSERT INTO query_users (name, email, age) VALUES ('Charlie', 'charlie@example.com', 35) RETURNING id").Scan(&id3)

			session := createQueryTestSession(setup)

			// Test FetchAll (no limit)
			query := orm1.NewEntityQuery[QueryUser](session, "u")
			users, err := query.FetchAll(ctx)
			if err != nil {
				t.Fatalf("FetchAll failed: %v", err)
			}

			if len(users) != 3 {
				t.Errorf("Expected 3 users, got %d", len(users))
			}
		})
	}
}

func TestQueryFetchOne(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupQueryTestDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert test data
			var id1 int64
			setup.DB.QueryRow("INSERT INTO query_users (name, email, age) VALUES ('Alice', 'alice@example.com', 30) RETURNING id").Scan(&id1)
			setup.DB.Exec("INSERT INTO query_users (name, email, age) VALUES ('Bob', 'bob@example.com', 25)")

			session := createQueryTestSession(setup)

			// Test FetchOne with Where
			query := orm1.NewEntityQuery[QueryUser](session, "u")
			query.Where("u.name = ?", "Alice")
			user, err := query.FetchOne(ctx)
			if err != nil {
				t.Fatalf("FetchOne failed: %v", err)
			}

			if user == nil {
				t.Fatal("Expected user, got nil")
			}
			if user.Name != "Alice" {
				t.Errorf("Expected name 'Alice', got '%s'", user.Name)
			}
			if user.Age != 30 {
				t.Errorf("Expected age 30, got %d", user.Age)
			}
		})
	}
}

func TestQueryFetchOneEmpty(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupQueryTestDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			session := createQueryTestSession(setup)

			// Test FetchOne with no results
			query := orm1.NewEntityQuery[QueryUser](session, "u")
			query.Where("u.name = ?", "NonExistent")
			user, err := query.FetchOne(ctx)
			if err != nil {
				t.Fatalf("FetchOne failed: %v", err)
			}

			if user != nil {
				t.Error("Expected nil user, got non-nil")
			}
		})
	}
}

func TestQueryFetchMany(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupQueryTestDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert test data
			setup.DB.Exec("INSERT INTO query_users (name, email, age) VALUES ('Alice', 'alice@example.com', 30)")
			setup.DB.Exec("INSERT INTO query_users (name, email, age) VALUES ('Bob', 'bob@example.com', 25)")
			setup.DB.Exec("INSERT INTO query_users (name, email, age) VALUES ('Charlie', 'charlie@example.com', 35)")

			session := createQueryTestSession(setup)

			// Test FetchMany with limit
			query := orm1.NewEntityQuery[QueryUser](session, "u")
			query.OrderBy(query.Asc("u.age"))
			users, err := query.FetchMany(ctx, 2)
			if err != nil {
				t.Fatalf("FetchMany failed: %v", err)
			}

			if len(users) != 2 {
				t.Errorf("Expected 2 users, got %d", len(users))
			}
			if users[0].Name != "Bob" {
				t.Errorf("Expected first user 'Bob', got '%s'", users[0].Name)
			}
			if users[1].Name != "Alice" {
				t.Errorf("Expected second user 'Alice', got '%s'", users[1].Name)
			}
		})
	}
}

func TestQueryWhere(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupQueryTestDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert test data
			setup.DB.Exec("INSERT INTO query_users (name, email, age) VALUES ('Alice', 'alice@example.com', 30)")
			setup.DB.Exec("INSERT INTO query_users (name, email, age) VALUES ('Bob', 'bob@example.com', 25)")
			setup.DB.Exec("INSERT INTO query_users (name, email, age) VALUES ('Charlie', 'charlie@example.com', 35)")

			session := createQueryTestSession(setup)

			// Test multiple Where conditions (AND)
			query := orm1.NewEntityQuery[QueryUser](session, "u")
			query.Where("u.age > ?", 25).Where("u.age < ?", 35)
			users, err := query.FetchAll(ctx)
			if err != nil {
				t.Fatalf("FetchAll failed: %v", err)
			}

			if len(users) != 1 {
				t.Errorf("Expected 1 user, got %d", len(users))
			}
			if len(users) > 0 && users[0].Name != "Alice" {
				t.Errorf("Expected user 'Alice', got '%s'", users[0].Name)
			}
		})
	}
}

func TestQueryOrderBy(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupQueryTestDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert test data
			setup.DB.Exec("INSERT INTO query_users (name, email, age) VALUES ('Alice', 'alice@example.com', 30)")
			setup.DB.Exec("INSERT INTO query_users (name, email, age) VALUES ('Bob', 'bob@example.com', 25)")
			setup.DB.Exec("INSERT INTO query_users (name, email, age) VALUES ('Charlie', 'charlie@example.com', 35)")

			session := createQueryTestSession(setup)

			// Test OrderBy ASC
			query := orm1.NewEntityQuery[QueryUser](session, "u")
			query.OrderBy(query.Asc("u.age"))
			users, err := query.FetchAll(ctx)
			if err != nil {
				t.Fatalf("FetchAll failed: %v", err)
			}

			if len(users) != 3 {
				t.Fatalf("Expected 3 users, got %d", len(users))
			}
			if users[0].Name != "Bob" {
				t.Errorf("Expected first user 'Bob', got '%s'", users[0].Name)
			}
			if users[2].Name != "Charlie" {
				t.Errorf("Expected third user 'Charlie', got '%s'", users[2].Name)
			}

			// Test OrderBy DESC
			query2 := orm1.NewEntityQuery[QueryUser](session, "u")
			query2.OrderBy(query2.Desc("u.age"))
			users2, err := query2.FetchAll(ctx)
			if err != nil {
				t.Fatalf("FetchAll failed: %v", err)
			}

			if users2[0].Name != "Charlie" {
				t.Errorf("Expected first user 'Charlie', got '%s'", users2[0].Name)
			}
			if users2[2].Name != "Bob" {
				t.Errorf("Expected third user 'Bob', got '%s'", users2[2].Name)
			}
		})
	}
}

func TestQueryLimitOffset(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupQueryTestDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert test data (5 users)
			setup.DB.Exec("INSERT INTO query_users (name, email, age) VALUES ('Alice', 'alice@example.com', 30)")
			setup.DB.Exec("INSERT INTO query_users (name, email, age) VALUES ('Bob', 'bob@example.com', 25)")
			setup.DB.Exec("INSERT INTO query_users (name, email, age) VALUES ('Charlie', 'charlie@example.com', 35)")
			setup.DB.Exec("INSERT INTO query_users (name, email, age) VALUES ('David', 'david@example.com', 28)")
			setup.DB.Exec("INSERT INTO query_users (name, email, age) VALUES ('Eve', 'eve@example.com', 32)")

			session := createQueryTestSession(setup)

			// Test Limit only
			query1 := orm1.NewEntityQuery[QueryUser](session, "u")
			query1.OrderBy(query1.Asc("u.age"))
			users1, err := query1.FetchMany(ctx, 2)
			if err != nil {
				t.Fatalf("FetchMany with limit failed: %v", err)
			}
			if len(users1) != 2 {
				t.Errorf("Expected 2 users with limit, got %d", len(users1))
			}
			if len(users1) > 0 && users1[0].Name != "Bob" {
				t.Errorf("Expected first user 'Bob', got '%s'", users1[0].Name)
			}

			// Test Offset only (with large limit)
			query2 := orm1.NewEntityQuery[QueryUser](session, "u")
			query2.OrderBy(query2.Asc("u.age")).Offset(1)
			users2, err := query2.FetchMany(ctx, 100)
			if err != nil {
				t.Fatalf("FetchMany with offset failed: %v", err)
			}
			if len(users2) != 4 {
				t.Errorf("Expected 4 users with offset, got %d", len(users2))
			}
			if len(users2) > 0 && users2[0].Name != "David" {
				t.Errorf("Expected first user 'David' (after offset), got '%s'", users2[0].Name)
			}

			// Test Limit and Offset together
			query3 := orm1.NewEntityQuery[QueryUser](session, "u")
			query3.OrderBy(query3.Asc("u.age")).Offset(2)
			users3, err := query3.FetchMany(ctx, 2)
			if err != nil {
				t.Fatalf("FetchMany with limit and offset failed: %v", err)
			}
			if len(users3) != 2 {
				t.Errorf("Expected 2 users with limit and offset, got %d", len(users3))
			}
			// Ordered by age: Bob(25), David(28), Alice(30), Eve(32), Charlie(35)
			// Offset 2 -> Alice(30), Eve(32), Charlie(35)
			// Limit 2 -> Alice(30), Eve(32)
			if len(users3) > 0 && users3[0].Name != "Alice" {
				t.Errorf("Expected first user 'Alice', got '%s'", users3[0].Name)
			}
			if len(users3) > 1 && users3[1].Name != "Eve" {
				t.Errorf("Expected second user 'Eve', got '%s'", users3[1].Name)
			}
		})
	}
}

func TestQueryJoin(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupQueryTestDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert test data
			var userID int64
			setup.DB.QueryRow("INSERT INTO query_users (name, email, age) VALUES ('Alice', 'alice@example.com', 30) RETURNING id").Scan(&userID)
			setup.DB.Exec("INSERT INTO query_posts (author_id, title, views) VALUES ($1, 'Post 1', 100)", userID)
			setup.DB.Exec("INSERT INTO query_posts (author_id, title, views) VALUES ($1, 'Post 2', 200)", userID)

			// Insert another user with no posts
			setup.DB.Exec("INSERT INTO query_users (name, email, age) VALUES ('Bob', 'bob@example.com', 25)")

			session := createQueryTestSession(setup)

			// Test INNER JOIN
			query := orm1.NewEntityQuery[QueryUser](session, "u")
			query.Join("query_posts", "p", "u.id = p.author_id")
			query.GroupByPrimaryKey()
			users, err := query.FetchAll(ctx)
			if err != nil {
				t.Fatalf("FetchAll failed: %v", err)
			}

			// Only Alice should be returned (has posts)
			if len(users) != 1 {
				t.Errorf("Expected 1 user, got %d", len(users))
			}
			if len(users) > 0 && users[0].Name != "Alice" {
				t.Errorf("Expected user 'Alice', got '%s'", users[0].Name)
			}
		})
	}
}

func TestQueryLeftJoin(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupQueryTestDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert test data
			var userID int64
			setup.DB.QueryRow("INSERT INTO query_users (name, email, age) VALUES ('Alice', 'alice@example.com', 30) RETURNING id").Scan(&userID)
			setup.DB.Exec("INSERT INTO query_posts (author_id, title, views) VALUES ($1, 'Post 1', 100)", userID)

			// Insert another user with no posts
			setup.DB.Exec("INSERT INTO query_users (name, email, age) VALUES ('Bob', 'bob@example.com', 25)")

			session := createQueryTestSession(setup)

			// Test LEFT JOIN
			query := orm1.NewEntityQuery[QueryUser](session, "u")
			query.LeftJoin("query_posts", "p", "u.id = p.author_id")
			query.GroupByPrimaryKey()
			query.OrderBy(query.Asc("u.id"))
			users, err := query.FetchAll(ctx)
			if err != nil {
				t.Fatalf("FetchAll failed: %v", err)
			}

			// Both users should be returned
			if len(users) != 2 {
				t.Errorf("Expected 2 users, got %d", len(users))
			}
		})
	}
}

func TestQueryCount(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupQueryTestDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert test data
			setup.DB.Exec("INSERT INTO query_users (name, email, age) VALUES ('Alice', 'alice@example.com', 30)")
			setup.DB.Exec("INSERT INTO query_users (name, email, age) VALUES ('Bob', 'bob@example.com', 25)")
			setup.DB.Exec("INSERT INTO query_users (name, email, age) VALUES ('Charlie', 'charlie@example.com', 35)")

			session := createQueryTestSession(setup)

			// Test Count
			query := orm1.NewEntityQuery[QueryUser](session, "u")
			count, err := query.Count(ctx)
			if err != nil {
				t.Fatalf("Count failed: %v", err)
			}

			if count != 3 {
				t.Errorf("Expected count 3, got %d", count)
			}

			// Test Count with Where
			query2 := orm1.NewEntityQuery[QueryUser](session, "u")
			query2.Where("u.age > ?", 25)
			count2, err := query2.Count(ctx)
			if err != nil {
				t.Fatalf("Count failed: %v", err)
			}

			if count2 != 2 {
				t.Errorf("Expected count 2, got %d", count2)
			}
		})
	}
}

func TestQueryHaving(t *testing.T) {
	for _, backend := range AllBackends() {
		t.Run(string(backend), func(t *testing.T) {
			setup := setupQueryTestDB(t, backend)
			defer setup.Cleanup()

			ctx := context.Background()

			// Insert test data
			var user1ID, user2ID int64
			setup.DB.QueryRow("INSERT INTO query_users (name, email, age) VALUES ('Alice', 'alice@example.com', 30) RETURNING id").Scan(&user1ID)
			setup.DB.QueryRow("INSERT INTO query_users (name, email, age) VALUES ('Bob', 'bob@example.com', 25) RETURNING id").Scan(&user2ID)

			// Alice has 2 posts, Bob has 1 post
			setup.DB.Exec("INSERT INTO query_posts (author_id, title, views) VALUES ($1, 'Post 1', 100)", user1ID)
			setup.DB.Exec("INSERT INTO query_posts (author_id, title, views) VALUES ($1, 'Post 2', 200)", user1ID)
			setup.DB.Exec("INSERT INTO query_posts (author_id, title, views) VALUES ($1, 'Post 3', 50)", user2ID)

			session := createQueryTestSession(setup)

			// Test HAVING clause
			query := orm1.NewEntityQuery[QueryUser](session, "u")
			query.Join("query_posts", "p", "u.id = p.author_id")
			query.GroupByPrimaryKey()
			query.Having("COUNT(p.id) > ?", 1)
			users, err := query.FetchAll(ctx)
			if err != nil {
				t.Fatalf("FetchAll failed: %v", err)
			}

			// Only Alice should be returned (has more than 1 post)
			if len(users) != 1 {
				t.Errorf("Expected 1 user, got %d", len(users))
			}
			if len(users) > 0 && users[0].Name != "Alice" {
				t.Errorf("Expected user 'Alice', got '%s'", users[0].Name)
			}
		})
	}
}
