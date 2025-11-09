package tests

// Sqlx Benchmark Test Coverage:
// - Simple operations (6/6): ✅ All supported
// - Aggregate operations (6/6): ✅ All supported with manual transactions
//
// Sqlx is a lightweight SQL extension providing struct scanning and named queries.
// Sits between raw SQL and full ORMs in terms of abstraction.

import (
	"context"
	"testing"

	"github.com/hanpama/orm1/benchmark/shared"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// sqlx entities
type SqlxUser struct {
	ID    int64  `db:"id"`
	Name  string `db:"name"`
	Email string `db:"email"`
	Age   int    `db:"age"`
}

type SqlxOrderItem struct {
	ID       int64   `db:"id"`
	OrderID  int64   `db:"order_id"`
	Product  string  `db:"product"`
	Quantity int     `db:"quantity"`
	Price    float64 `db:"price"`
}

type SqlxOrderNote struct {
	ID      int64  `db:"id"`
	OrderID int64  `db:"order_id"`
	Content string `db:"content"`
}

type SqlxOrder struct {
	ID       int64   `db:"id"`
	Customer string  `db:"customer"`
	Total    float64 `db:"total"`
}

func setupSqlxPostgresDB(b *testing.B) (*sqlx.DB, func()) {
	db, err := sqlx.Connect("postgres", "host=localhost port=25432 user=testuser password=testpass dbname=testdb sslmode=disable")
	if err != nil {
		b.Skipf("PostgreSQL not available: %v", err)
	}

	// Setup schema using shared helper
	if err := shared.SetupPostgresSchema(db.DB); err != nil {
		b.Fatal(err)
	}

	cleanup := func() {
		shared.CleanupPostgresTables(db.DB)
		db.Close()
	}

	return db, cleanup
}

// -----------------------------------------------------------------------------
// sqlx PostgreSQL - Simple CRUD
// -----------------------------------------------------------------------------

func BenchmarkSqlx_Postgres_Simple_Insert(b *testing.B) {
	db, cleanup := setupSqlxPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := db.ExecContext(ctx,
			"INSERT INTO users (name, email, age) VALUES ($1, $2, $3)",
			"Alice", "alice@example.com", 30)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSqlx_Postgres_Simple_Select(b *testing.B) {
	db, cleanup := setupSqlxPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate
	for i := 0; i < 100; i++ {
		db.ExecContext(ctx,
			"INSERT INTO users (name, email, age) VALUES ($1, $2, $3)",
			"User", "user@example.com", 20+(i%50))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		user := SqlxUser{}
		id := int64(1 + (i % 100))
		err := db.GetContext(ctx, &user,
			"SELECT id, name, email, age FROM users WHERE id = $1", id)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSqlx_Postgres_Simple_Update(b *testing.B) {
	db, cleanup := setupSqlxPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate
	for i := 0; i < 100; i++ {
		db.ExecContext(ctx,
			"INSERT INTO users (name, email, age) VALUES ($1, $2, $3)",
			"User", "user@example.com", 20)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		user := SqlxUser{}
		id := int64(1 + (i % 100))
		err := db.GetContext(ctx, &user,
			"SELECT id, name, email, age FROM users WHERE id = $1", id)
		if err != nil {
			b.Fatal(err)
		}

		_, err = db.ExecContext(ctx,
			"UPDATE users SET age = $1 WHERE id = $2",
			30, user.ID)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSqlx_Postgres_Simple_BatchSelect(b *testing.B) {
	db, cleanup := setupSqlxPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate 200 rows
	for i := 0; i < 200; i++ {
		db.ExecContext(ctx,
			"INSERT INTO users (name, email, age) VALUES ($1, $2, $3)",
			"User", "user@example.com", 20+(i%50))
	}

	b.ResetTimer()
	b.ReportAllocs()

	// Requested IDs
	ids := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
		21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
		41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60,
		61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80,
		81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100}

	for i := 0; i < b.N; i++ {
		var users []SqlxUser
		// sqlx doesn't have a built-in IN clause helper, so we use ANY for PostgreSQL
		query := "SELECT id, name, email, age FROM users WHERE id = ANY($1)"

		err := db.SelectContext(ctx, &users, query, pq.Array(ids))
		if err != nil {
			b.Fatal(err)
		}
		if len(users) != 100 {
			b.Fatalf("Expected 100 users, got %d", len(users))
		}

		// Sort results to match requested ID order
		idMap := make(map[int64]*SqlxUser, len(users))
		for i := range users {
			idMap[users[i].ID] = &users[i]
		}
		sortedUsers := make([]SqlxUser, 0, len(ids))
		for _, id := range ids {
			if u, ok := idMap[id]; ok {
				sortedUsers = append(sortedUsers, *u)
			}
		}
		users = sortedUsers
	}
}

func BenchmarkSqlx_Postgres_Simple_ReadSlice(b *testing.B) {
	db, cleanup := setupSqlxPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate 100 rows
	for i := 0; i < 100; i++ {
		db.ExecContext(ctx,
			"INSERT INTO users (name, email, age) VALUES ($1, $2, $3)",
			"User", "user@example.com", 20+(i%50))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var users []SqlxUser
		err := db.SelectContext(ctx, &users,
			"SELECT id, name, email, age FROM users WHERE id > $1 LIMIT 100", 0)
		if err != nil {
			b.Fatal(err)
		}
		if len(users) != 100 {
			b.Fatalf("Expected 100 users, got %d", len(users))
		}
	}
}

func BenchmarkSqlx_Postgres_Simple_Delete(b *testing.B) {
	db, cleanup := setupSqlxPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Pre-populate for each iteration
		result, err := db.ExecContext(ctx,
			"INSERT INTO users (name, email, age) VALUES ($1, $2, $3) RETURNING id",
			"DeleteMe", "delete@example.com", 99)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		// Delete using last insert ID (PostgreSQL doesn't return LastInsertId, so we use RETURNING)
		_, err = db.ExecContext(ctx, "DELETE FROM users WHERE name = $1", "DeleteMe")
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
}

// -----------------------------------------------------------------------------
// sqlx PostgreSQL - Aggregate CRUD
// -----------------------------------------------------------------------------

func BenchmarkSqlx_Postgres_Aggregate_Insert(b *testing.B) {
	db, cleanup := setupSqlxPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Begin transaction
		tx, err := db.BeginTxx(ctx, nil)
		if err != nil {
			b.Fatal(err)
		}

		// Insert order
		var orderID int64
		err = tx.QueryRowContext(ctx,
			"INSERT INTO orders (customer, total) VALUES ($1, $2) RETURNING id",
			"John Doe", 299.97).Scan(&orderID)
		if err != nil {
			tx.Rollback()
			b.Fatal(err)
		}

		// Insert items
		_, err = tx.ExecContext(ctx,
			"INSERT INTO order_items (order_id, product, quantity, price) VALUES ($1, $2, $3, $4), ($5, $6, $7, $8), ($9, $10, $11, $12)",
			orderID, "Product A", 1, 99.99,
			orderID, "Product B", 1, 99.99,
			orderID, "Product C", 1, 99.99)
		if err != nil {
			tx.Rollback()
			b.Fatal(err)
		}

		// Insert notes
		_, err = tx.ExecContext(ctx,
			"INSERT INTO order_notes (order_id, content) VALUES ($1, $2), ($3, $4)",
			orderID, "Please deliver before 5pm",
			orderID, "Gift wrap requested")
		if err != nil {
			tx.Rollback()
			b.Fatal(err)
		}

		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSqlx_Postgres_Aggregate_Select(b *testing.B) {
	db, cleanup := setupSqlxPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate
	for i := 0; i < 10; i++ {
		tx, _ := db.BeginTxx(ctx, nil)
		var orderID int64
		tx.QueryRowContext(ctx,
			"INSERT INTO orders (customer, total) VALUES ($1, $2) RETURNING id",
			"Customer", 299.97).Scan(&orderID)
		tx.ExecContext(ctx,
			"INSERT INTO order_items (order_id, product, quantity, price) VALUES ($1, $2, $3, $4), ($5, $6, $7, $8), ($9, $10, $11, $12)",
			orderID, "Product A", 1, 99.99,
			orderID, "Product B", 1, 99.99,
			orderID, "Product C", 1, 99.99)
		tx.ExecContext(ctx,
			"INSERT INTO order_notes (order_id, content) VALUES ($1, $2), ($3, $4)",
			orderID, "Please deliver before 5pm",
			orderID, "Gift wrap requested")
		tx.Commit()
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		order := SqlxOrder{}
		id := int64(1 + (i % 10))
		err := db.GetContext(ctx, &order,
			"SELECT id, customer, total FROM orders WHERE id = $1", id)
		if err != nil {
			b.Fatal(err)
		}

		// Load items
		var items []SqlxOrderItem
		err = db.SelectContext(ctx, &items,
			"SELECT id, order_id, product, quantity, price FROM order_items WHERE order_id = $1", order.ID)
		if err != nil {
			b.Fatal(err)
		}

		// Load notes
		var notes []SqlxOrderNote
		err = db.SelectContext(ctx, &notes,
			"SELECT id, order_id, content FROM order_notes WHERE order_id = $1", order.ID)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSqlx_Postgres_Aggregate_BatchSelect(b *testing.B) {
	db, cleanup := setupSqlxPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate 100 orders with 3 items and 2 notes each
	for i := 0; i < 100; i++ {
		tx, _ := db.BeginTxx(ctx, nil)
		var orderID int64
		tx.QueryRowContext(ctx,
			"INSERT INTO orders (customer, total) VALUES ($1, $2) RETURNING id",
			"Customer", 299.97).Scan(&orderID)
		tx.ExecContext(ctx,
			"INSERT INTO order_items (order_id, product, quantity, price) VALUES ($1, $2, $3, $4), ($5, $6, $7, $8), ($9, $10, $11, $12)",
			orderID, "Product A", 1, 99.99,
			orderID, "Product B", 1, 99.99,
			orderID, "Product C", 1, 99.99)
		tx.ExecContext(ctx,
			"INSERT INTO order_notes (order_id, content) VALUES ($1, $2), ($3, $4)",
			orderID, "Please deliver before 5pm",
			orderID, "Gift wrap requested")
		tx.Commit()
	}

	b.ResetTimer()
	b.ReportAllocs()

	// Requested IDs
	ids := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	for i := 0; i < b.N; i++ {
		var orders []SqlxOrder
		err := db.SelectContext(ctx, &orders,
			"SELECT id, customer, total FROM orders WHERE id = ANY($1)", pq.Array(ids))
		if err != nil {
			b.Fatal(err)
		}
		if len(orders) != 10 {
			b.Fatalf("Expected 10 orders, got %d", len(orders))
		}

		// Sort results to match requested ID order
		idMap := make(map[int64]*SqlxOrder, len(orders))
		for i := range orders {
			idMap[orders[i].ID] = &orders[i]
		}
		sortedOrders := make([]SqlxOrder, 0, len(ids))
		for _, id := range ids {
			if o, ok := idMap[id]; ok {
				sortedOrders = append(sortedOrders, *o)
			}
		}
		orders = sortedOrders

		// Load all items for these orders
		var items []SqlxOrderItem
		err = db.SelectContext(ctx, &items,
			"SELECT id, order_id, product, quantity, price FROM order_items WHERE order_id = ANY($1)", pq.Array(ids))
		if err != nil {
			b.Fatal(err)
		}

		// Load all notes for these orders
		var notes []SqlxOrderNote
		err = db.SelectContext(ctx, &notes,
			"SELECT id, order_id, content FROM order_notes WHERE order_id = ANY($1)", pq.Array(ids))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSqlx_Postgres_Aggregate_Update(b *testing.B) {
	db, cleanup := setupSqlxPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate
	for i := 0; i < 10; i++ {
		tx, _ := db.BeginTxx(ctx, nil)
		var orderID int64
		tx.QueryRowContext(ctx,
			"INSERT INTO orders (customer, total) VALUES ($1, $2) RETURNING id",
			"Customer", 299.97).Scan(&orderID)
		tx.ExecContext(ctx,
			"INSERT INTO order_items (order_id, product, quantity, price) VALUES ($1, $2, $3, $4), ($5, $6, $7, $8), ($9, $10, $11, $12)",
			orderID, "Product A", 1, 99.99,
			orderID, "Product B", 1, 99.99,
			orderID, "Product C", 1, 99.99)
		tx.ExecContext(ctx,
			"INSERT INTO order_notes (order_id, content) VALUES ($1, $2), ($3, $4)",
			orderID, "Please deliver before 5pm",
			orderID, "Gift wrap requested")
		tx.Commit()
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tx, err := db.BeginTxx(ctx, nil)
		if err != nil {
			b.Fatal(err)
		}

		order := SqlxOrder{}
		id := int64(1 + (i % 10))
		err = tx.GetContext(ctx, &order,
			"SELECT id, customer, total FROM orders WHERE id = $1", id)
		if err != nil {
			tx.Rollback()
			b.Fatal(err)
		}

		// Load items to get their IDs
		var items []SqlxOrderItem
		err = tx.SelectContext(ctx, &items,
			"SELECT id, order_id, product, quantity, price FROM order_items WHERE order_id = $1", order.ID)
		if err != nil {
			tx.Rollback()
			b.Fatal(err)
		}

		// Update order
		_, err = tx.ExecContext(ctx,
			"UPDATE orders SET total = $1 WHERE id = $2",
			399.96, order.ID)
		if err != nil {
			tx.Rollback()
			b.Fatal(err)
		}

		// Bulk update using CTE with different prices for each item
		if len(items) > 0 {
			_, err = tx.ExecContext(ctx, `
				UPDATE order_items AS t
				SET price = c.price
				FROM (VALUES ($1::bigint, $2::float8), ($3::bigint, $4::float8), ($5::bigint, $6::float8)) AS c(id, price)
				WHERE t.id = c.id
			`, items[0].ID, 149.99, items[1].ID, 124.99, items[2].ID, 124.98)
			if err != nil {
				tx.Rollback()
				b.Fatal(err)
			}
		}

		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSqlx_Postgres_Aggregate_ReadSlice(b *testing.B) {
	db, cleanup := setupSqlxPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate 100 orders with 3 items and 2 notes each
	for i := 0; i < 100; i++ {
		tx, _ := db.BeginTxx(ctx, nil)
		var orderID int64
		tx.QueryRowContext(ctx,
			"INSERT INTO orders (customer, total) VALUES ($1, $2) RETURNING id",
			"Customer", 299.97+float64(i)).Scan(&orderID)
		tx.ExecContext(ctx,
			"INSERT INTO order_items (order_id, product, quantity, price) VALUES ($1, $2, $3, $4), ($5, $6, $7, $8), ($9, $10, $11, $12)",
			orderID, "Product A", 1, 99.99,
			orderID, "Product B", 1, 99.99,
			orderID, "Product C", 1, 99.99)
		tx.ExecContext(ctx,
			"INSERT INTO order_notes (order_id, content) VALUES ($1, $2), ($3, $4)",
			orderID, "Please deliver before 5pm",
			orderID, "Gift wrap requested")
		tx.Commit()
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var orders []SqlxOrder
		err := db.SelectContext(ctx, &orders,
			"SELECT id, customer, total FROM orders WHERE id > $1 LIMIT 100", 0)
		if err != nil {
			b.Fatal(err)
		}
		if len(orders) != 100 {
			b.Fatalf("Expected 100 orders, got %d", len(orders))
		}

		// Extract order IDs
		orderIDs := make([]int64, len(orders))
		for i, order := range orders {
			orderIDs[i] = order.ID
		}

		// Load all items
		var items []SqlxOrderItem
		err = db.SelectContext(ctx, &items,
			"SELECT id, order_id, product, quantity, price FROM order_items WHERE order_id = ANY($1)", pq.Array(orderIDs))
		if err != nil {
			b.Fatal(err)
		}
		if len(items) != 300 {
			b.Fatalf("Expected 300 items, got %d", len(items))
		}

		// Load all notes
		var notes []SqlxOrderNote
		err = db.SelectContext(ctx, &notes,
			"SELECT id, order_id, content FROM order_notes WHERE order_id = ANY($1)", pq.Array(orderIDs))
		if err != nil {
			b.Fatal(err)
		}
		if len(notes) != 200 {
			b.Fatalf("Expected 200 notes, got %d", len(notes))
		}
	}
}

func BenchmarkSqlx_Postgres_Aggregate_Delete(b *testing.B) {
	db, cleanup := setupSqlxPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Pre-populate for each iteration
		tx, _ := db.BeginTxx(ctx, nil)
		var orderID int64
		tx.QueryRowContext(ctx,
			"INSERT INTO orders (customer, total) VALUES ($1, $2) RETURNING id",
			"DeleteMe", 299.97).Scan(&orderID)
		tx.ExecContext(ctx,
			"INSERT INTO order_items (order_id, product, quantity, price) VALUES ($1, $2, $3, $4), ($5, $6, $7, $8), ($9, $10, $11, $12)",
			orderID, "Product A", 1, 99.99,
			orderID, "Product B", 1, 99.99,
			orderID, "Product C", 1, 99.99)
		tx.ExecContext(ctx,
			"INSERT INTO order_notes (order_id, content) VALUES ($1, $2), ($3, $4)",
			orderID, "Please deliver before 5pm",
			orderID, "Gift wrap requested")
		tx.Commit()
		b.StartTimer()

		// Delete with transaction
		tx2, err := db.BeginTxx(ctx, nil)
		if err != nil {
			b.Fatal(err)
		}

		// Delete items first (FK constraint)
		_, err = tx2.ExecContext(ctx, "DELETE FROM order_items WHERE order_id = $1", orderID)
		if err != nil {
			tx2.Rollback()
			b.Fatal(err)
		}

		// Delete notes (FK constraint)
		_, err = tx2.ExecContext(ctx, "DELETE FROM order_notes WHERE order_id = $1", orderID)
		if err != nil {
			tx2.Rollback()
			b.Fatal(err)
		}

		// Delete order
		_, err = tx2.ExecContext(ctx, "DELETE FROM orders WHERE id = $1", orderID)
		if err != nil {
			tx2.Rollback()
			b.Fatal(err)
		}

		if err := tx2.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}
