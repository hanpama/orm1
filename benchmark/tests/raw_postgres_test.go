package tests

// Raw SQL Benchmark Test Coverage:
// - Simple operations (6/6): ✅ All supported
// - Aggregate operations (6/6): ✅ All supported with manual transactions
//
// Raw SQL provides the performance baseline for all benchmarks.
// Manual transaction management using db.Begin(), tx.Commit(), tx.Rollback().

import (
	"context"
	"testing"

	"github.com/hanpama/orm1/benchmark/shared"
)

func setupRawPostgresDB(b *testing.B) func() {
	db, err := shared.GetPostgresDB()
	if err != nil {
		b.Skipf("PostgreSQL not available: %v", err)
	}

	if err := shared.SetupPostgresSchema(db); err != nil {
		b.Fatal(err)
	}

	// Store in benchmark state
	b.Cleanup(func() {
		shared.CleanupPostgresTables(db)
		db.Close()
	})

	return func() {
		shared.CleanupPostgresTables(db)
		db.Close()
	}
}

// -----------------------------------------------------------------------------
// Raw SQL PostgreSQL - Simple CRUD
// -----------------------------------------------------------------------------

func BenchmarkRaw_Postgres_Simple_Insert(b *testing.B) {
	cleanup := setupRawPostgresDB(b)
	defer cleanup()

	db, _ := shared.GetPostgresDB()
	defer db.Close()
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

func BenchmarkRaw_Postgres_Simple_Select(b *testing.B) {
	cleanup := setupRawPostgresDB(b)
	defer cleanup()

	db, _ := shared.GetPostgresDB()
	defer db.Close()
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
		id := int64(1 + (i % 100))
		var name, email string
		var age int
		err := db.QueryRowContext(ctx,
			"SELECT name, email, age FROM users WHERE id = $1", id).
			Scan(&name, &email, &age)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRaw_Postgres_Simple_Update(b *testing.B) {
	cleanup := setupRawPostgresDB(b)
	defer cleanup()

	db, _ := shared.GetPostgresDB()
	defer db.Close()
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
		id := int64(1 + (i % 100))

		// SELECT to match ORM pattern
		var name, email string
		var age int
		err := db.QueryRowContext(ctx,
			"SELECT name, email, age FROM users WHERE id = $1", id).
			Scan(&name, &email, &age)
		if err != nil {
			b.Fatal(err)
		}

		// UPDATE
		_, err = db.ExecContext(ctx,
			"UPDATE users SET age = $1 WHERE id = $2", 30, id)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRaw_Postgres_Simple_BatchSelect(b *testing.B) {
	cleanup := setupRawPostgresDB(b)
	defer cleanup()

	db, _ := shared.GetPostgresDB()
	defer db.Close()
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
	requestedIDs := []int64{
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
		11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
		21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
		31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
		41, 42, 43, 44, 45, 46, 47, 48, 49, 50,
		51, 52, 53, 54, 55, 56, 57, 58, 59, 60,
		61, 62, 63, 64, 65, 66, 67, 68, 69, 70,
		71, 72, 73, 74, 75, 76, 77, 78, 79, 80,
		81, 82, 83, 84, 85, 86, 87, 88, 89, 90,
		91, 92, 93, 94, 95, 96, 97, 98, 99, 100,
	}

	for i := 0; i < b.N; i++ {
		// Select 100 rows using WHERE IN
		rows, err := db.QueryContext(ctx,
			`SELECT id, name, email, age FROM users WHERE id IN (
				$1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
				$11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
				$21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
				$31, $32, $33, $34, $35, $36, $37, $38, $39, $40,
				$41, $42, $43, $44, $45, $46, $47, $48, $49, $50,
				$51, $52, $53, $54, $55, $56, $57, $58, $59, $60,
				$61, $62, $63, $64, $65, $66, $67, $68, $69, $70,
				$71, $72, $73, $74, $75, $76, $77, $78, $79, $80,
				$81, $82, $83, $84, $85, $86, $87, $88, $89, $90,
				$91, $92, $93, $94, $95, $96, $97, $98, $99, $100)`,
			1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
			11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
			21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
			31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
			41, 42, 43, 44, 45, 46, 47, 48, 49, 50,
			51, 52, 53, 54, 55, 56, 57, 58, 59, 60,
			61, 62, 63, 64, 65, 66, 67, 68, 69, 70,
			71, 72, 73, 74, 75, 76, 77, 78, 79, 80,
			81, 82, 83, 84, 85, 86, 87, 88, 89, 90,
			91, 92, 93, 94, 95, 96, 97, 98, 99, 100)
		if err != nil {
			b.Fatal(err)
		}

		// Collect results into map
		type user struct {
			id    int64
			name  string
			email string
			age   int
		}
		userMap := make(map[int64]user)
		for rows.Next() {
			var u user
			rows.Scan(&u.id, &u.name, &u.email, &u.age)
			userMap[u.id] = u
		}
		rows.Close()

		// Sort results to match requested ID order
		sortedUsers := make([]user, 0, len(requestedIDs))
		for _, id := range requestedIDs {
			if u, ok := userMap[id]; ok {
				sortedUsers = append(sortedUsers, u)
			}
		}

		if len(sortedUsers) != 100 {
			b.Fatalf("Expected 100 rows, got %d", len(sortedUsers))
		}
	}
}

func BenchmarkRaw_Postgres_Simple_Delete(b *testing.B) {
	cleanup := setupRawPostgresDB(b)
	defer cleanup()

	db, _ := shared.GetPostgresDB()
	defer db.Close()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Pre-populate for each iteration
		var id int64
		err := db.QueryRowContext(ctx,
			"INSERT INTO users (name, email, age) VALUES ($1, $2, $3) RETURNING id",
			"DeleteMe", "delete@example.com", 99).Scan(&id)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		_, err = db.ExecContext(ctx, "DELETE FROM users WHERE id = $1", id)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRaw_Postgres_Simple_ReadSlice(b *testing.B) {
	cleanup := setupRawPostgresDB(b)
	defer cleanup()

	db, _ := shared.GetPostgresDB()
	defer db.Close()
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
		rows, err := db.QueryContext(ctx,
			"SELECT name, email, age FROM users WHERE id > $1 LIMIT 100", 0)
		if err != nil {
			b.Fatal(err)
		}

		count := 0
		for rows.Next() {
			var name, email string
			var age int
			rows.Scan(&name, &email, &age)
			count++
		}
		rows.Close()

		if count != 100 {
			b.Fatalf("Expected 100 rows, got %d", count)
		}
	}
}

// -----------------------------------------------------------------------------
// Raw SQL PostgreSQL - Aggregate CRUD
// -----------------------------------------------------------------------------

func BenchmarkRaw_Postgres_Aggregate_Insert(b *testing.B) {
	cleanup := setupRawPostgresDB(b)
	defer cleanup()

	db, _ := shared.GetPostgresDB()
	defer db.Close()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tx, _ := db.BeginTx(ctx, nil)

		var orderID int64
		err := tx.QueryRowContext(ctx,
			"INSERT INTO orders (customer, total) VALUES ($1, $2) RETURNING id",
			"John Doe", 299.97).Scan(&orderID)
		if err != nil {
			tx.Rollback()
			b.Fatal(err)
		}

		// Batch insert for items (optimized)
		_, err = tx.ExecContext(ctx,
			`INSERT INTO order_items (order_id, product, quantity, price)
			VALUES ($1, $2, $3, $4), ($5, $6, $7, $8), ($9, $10, $11, $12)`,
			orderID, "Product", 1, 99.99,
			orderID, "Product", 1, 99.99,
			orderID, "Product", 1, 99.99)
		if err != nil {
			tx.Rollback()
			b.Fatal(err)
		}

		// Batch insert for notes
		_, err = tx.ExecContext(ctx,
			`INSERT INTO order_notes (order_id, content)
			VALUES ($1, $2), ($3, $4)`,
			orderID, "Please deliver before 5pm",
			orderID, "Gift wrap requested")
		if err != nil {
			tx.Rollback()
			b.Fatal(err)
		}

		tx.Commit()
	}
}

func BenchmarkRaw_Postgres_Aggregate_Select(b *testing.B) {
	cleanup := setupRawPostgresDB(b)
	defer cleanup()

	db, _ := shared.GetPostgresDB()
	defer db.Close()
	ctx := context.Background()

	// Pre-populate
	tx, _ := db.BeginTx(ctx, nil)
	for i := 0; i < 10; i++ {
		var orderID int64
		tx.QueryRowContext(ctx,
			"INSERT INTO orders (customer, total) VALUES ($1, $2) RETURNING id",
			"Customer", 299.97).Scan(&orderID)

		for j := 0; j < 3; j++ {
			tx.ExecContext(ctx,
				"INSERT INTO order_items (order_id, product, quantity, price) VALUES ($1, $2, $3, $4)",
				orderID, "Product", 1, 99.99)
		}

		for j := 0; j < 2; j++ {
			tx.ExecContext(ctx,
				"INSERT INTO order_notes (order_id, content) VALUES ($1, $2)",
				orderID, "Note content")
		}
	}
	tx.Commit()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		orderID := int64(1 + (i % 10))

		// Load parent
		var id int64
		var customer string
		var total float64
		err := db.QueryRowContext(ctx,
			"SELECT id, customer, total FROM orders WHERE id = $1", orderID).
			Scan(&id, &customer, &total)
		if err != nil {
			b.Fatal(err)
		}

		// Load children - items
		rows, err := db.QueryContext(ctx,
			"SELECT id, order_id, product, quantity, price FROM order_items WHERE order_id = $1",
			orderID)
		if err != nil {
			b.Fatal(err)
		}

		for rows.Next() {
			var itemID, orderID int64
			var product string
			var quantity int
			var price float64
			rows.Scan(&itemID, &orderID, &product, &quantity, &price)
		}
		rows.Close()

		// Load children - notes
		noteRows, err := db.QueryContext(ctx,
			"SELECT id, order_id, content FROM order_notes WHERE order_id = $1",
			orderID)
		if err != nil {
			b.Fatal(err)
		}

		for noteRows.Next() {
			var noteID, orderID int64
			var content string
			noteRows.Scan(&noteID, &orderID, &content)
		}
		noteRows.Close()
	}
}

func BenchmarkRaw_Postgres_Aggregate_BatchSelect(b *testing.B) {
	cleanup := setupRawPostgresDB(b)
	defer cleanup()

	db, _ := shared.GetPostgresDB()
	defer db.Close()
	ctx := context.Background()

	// Pre-populate 100 orders with 3 items and 2 notes each
	tx, _ := db.BeginTx(ctx, nil)
	for i := 0; i < 100; i++ {
		var orderID int64
		tx.QueryRowContext(ctx,
			"INSERT INTO orders (customer, total) VALUES ($1, $2) RETURNING id",
			"Customer", 299.97).Scan(&orderID)

		for j := 0; j < 3; j++ {
			tx.ExecContext(ctx,
				"INSERT INTO order_items (order_id, product, quantity, price) VALUES ($1, $2, $3, $4)",
				orderID, "Product", 1, 99.99)
		}

		for j := 0; j < 2; j++ {
			tx.ExecContext(ctx,
				"INSERT INTO order_notes (order_id, content) VALUES ($1, $2)",
				orderID, "Note content")
		}
	}
	tx.Commit()

	b.ResetTimer()
	b.ReportAllocs()

	// Requested IDs
	requestedIDs := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	for i := 0; i < b.N; i++ {
		// Load 10 orders
		rows, err := db.QueryContext(ctx,
			`SELECT id, customer, total FROM orders WHERE id IN ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
			1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		if err != nil {
			b.Fatal(err)
		}

		// Collect orders into map
		type order struct {
			id       int64
			customer string
			total    float64
		}
		orderMap := make(map[int64]order)
		for rows.Next() {
			var o order
			rows.Scan(&o.id, &o.customer, &o.total)
			orderMap[o.id] = o
		}
		rows.Close()

		// Sort results to match requested ID order
		sortedOrders := make([]order, 0, len(requestedIDs))
		for _, id := range requestedIDs {
			if o, ok := orderMap[id]; ok {
				sortedOrders = append(sortedOrders, o)
			}
		}

		if len(sortedOrders) != 10 {
			b.Fatalf("Expected 10 orders, got %d", len(sortedOrders))
		}

		// Load all items for these 10 orders
		itemRows, err := db.QueryContext(ctx,
			`SELECT id, order_id, product, quantity, price FROM order_items WHERE order_id IN ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
			1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		if err != nil {
			b.Fatal(err)
		}

		itemCount := 0
		for itemRows.Next() {
			var itemID, orderID int64
			var product string
			var quantity int
			var price float64
			itemRows.Scan(&itemID, &orderID, &product, &quantity, &price)
			itemCount++
		}
		itemRows.Close()

		if itemCount != 30 {
			b.Fatalf("Expected 30 items, got %d", itemCount)
		}

		// Load all notes for these 10 orders
		noteRows, err := db.QueryContext(ctx,
			`SELECT id, order_id, content FROM order_notes WHERE order_id IN ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
			1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		if err != nil {
			b.Fatal(err)
		}

		noteCount := 0
		for noteRows.Next() {
			var noteID, orderID int64
			var content string
			noteRows.Scan(&noteID, &orderID, &content)
			noteCount++
		}
		noteRows.Close()

		if noteCount != 20 {
			b.Fatalf("Expected 20 notes, got %d", noteCount)
		}
	}
}

func BenchmarkRaw_Postgres_Aggregate_Update(b *testing.B) {
	cleanup := setupRawPostgresDB(b)
	defer cleanup()

	db, _ := shared.GetPostgresDB()
	defer db.Close()
	ctx := context.Background()

	// Pre-populate
	tx, _ := db.BeginTx(ctx, nil)
	for i := 0; i < 10; i++ {
		var orderID int64
		tx.QueryRowContext(ctx,
			"INSERT INTO orders (customer, total) VALUES ($1, $2) RETURNING id",
			"Customer", 299.97).Scan(&orderID)

		for j := 0; j < 3; j++ {
			tx.ExecContext(ctx,
				"INSERT INTO order_items (order_id, product, quantity, price) VALUES ($1, $2, $3, $4)",
				orderID, "Product", 1, 99.99)
		}

		for j := 0; j < 2; j++ {
			tx.ExecContext(ctx,
				"INSERT INTO order_notes (order_id, content) VALUES ($1, $2)",
				orderID, "Note content")
		}
	}
	tx.Commit()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		orderID := int64(1 + (i % 10))

		// SELECT to match ORM pattern
		// Load parent
		var id int64
		var customer string
		var total float64
		err := db.QueryRowContext(ctx,
			"SELECT id, customer, total FROM orders WHERE id = $1", orderID).
			Scan(&id, &customer, &total)
		if err != nil {
			b.Fatal(err)
		}

		// Load children - items (store IDs for proper update by PK)
		type ItemData struct {
			ID       int64
			OrderID  int64
			Product  string
			Quantity int
			Price    float64
		}
		var items []ItemData

		rows, err := db.QueryContext(ctx,
			"SELECT id, order_id, product, quantity, price FROM order_items WHERE order_id = $1",
			orderID)
		if err != nil {
			b.Fatal(err)
		}

		for rows.Next() {
			var item ItemData
			rows.Scan(&item.ID, &item.OrderID, &item.Product, &item.Quantity, &item.Price)
			items = append(items, item)
		}
		rows.Close()

		// Load children - notes
		noteRows, err := db.QueryContext(ctx,
			"SELECT id, order_id, content FROM order_notes WHERE order_id = $1",
			orderID)
		if err != nil {
			b.Fatal(err)
		}

		for noteRows.Next() {
			var noteID, orderID int64
			var content string
			noteRows.Scan(&noteID, &orderID, &content)
		}
		noteRows.Close()

		// UPDATE
		tx, _ := db.BeginTx(ctx, nil)

		// Update parent
		_, err = tx.ExecContext(ctx,
			"UPDATE orders SET total = $1 WHERE id = $2", 399.96, orderID)
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

func BenchmarkRaw_Postgres_Aggregate_Delete(b *testing.B) {
	cleanup := setupRawPostgresDB(b)
	defer cleanup()

	db, _ := shared.GetPostgresDB()
	defer db.Close()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Pre-populate for each iteration
		tx, _ := db.BeginTx(ctx, nil)
		var orderID int64
		tx.QueryRowContext(ctx,
			"INSERT INTO orders (customer, total) VALUES ($1, $2) RETURNING id",
			"DeleteMe", 299.97).Scan(&orderID)

		for j := 0; j < 3; j++ {
			tx.ExecContext(ctx,
				"INSERT INTO order_items (order_id, product, quantity, price) VALUES ($1, $2, $3, $4)",
				orderID, "Product", 1, 99.99)
		}

		for j := 0; j < 2; j++ {
			tx.ExecContext(ctx,
				"INSERT INTO order_notes (order_id, content) VALUES ($1, $2)",
				orderID, "Note content")
		}
		tx.Commit()
		b.StartTimer()

		tx, _ = db.BeginTx(ctx, nil)
		// Delete children first (or rely on CASCADE)
		_, err := tx.ExecContext(ctx, "DELETE FROM order_notes WHERE order_id = $1", orderID)
		if err != nil {
			tx.Rollback()
			b.Fatal(err)
		}

		_, err = tx.ExecContext(ctx, "DELETE FROM order_items WHERE order_id = $1", orderID)
		if err != nil {
			tx.Rollback()
			b.Fatal(err)
		}

		_, err = tx.ExecContext(ctx, "DELETE FROM orders WHERE id = $1", orderID)
		if err != nil {
			tx.Rollback()
			b.Fatal(err)
		}

		tx.Commit()
	}
}

func BenchmarkRaw_Postgres_Aggregate_ReadSlice(b *testing.B) {
	cleanup := setupRawPostgresDB(b)
	defer cleanup()

	db, _ := shared.GetPostgresDB()
	defer db.Close()
	ctx := context.Background()

	// Pre-populate 100 orders with 3 items and 2 notes each
	tx, _ := db.BeginTx(ctx, nil)
	for i := 0; i < 100; i++ {
		var orderID int64
		tx.QueryRowContext(ctx,
			"INSERT INTO orders (customer, total) VALUES ($1, $2) RETURNING id",
			"Customer", 299.97+float64(i)).Scan(&orderID)

		for j := 0; j < 3; j++ {
			tx.ExecContext(ctx,
				"INSERT INTO order_items (order_id, product, quantity, price) VALUES ($1, $2, $3, $4)",
				orderID, "Product", 1, 99.99)
		}

		for j := 0; j < 2; j++ {
			tx.ExecContext(ctx,
				"INSERT INTO order_notes (order_id, content) VALUES ($1, $2)",
				orderID, "Note content")
		}
	}
	tx.Commit()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Load 100 orders
		rows, err := db.QueryContext(ctx,
			"SELECT id, customer, total FROM orders WHERE id > $1 LIMIT 100", 0)
		if err != nil {
			b.Fatal(err)
		}

		orders := make([]*shared.Order, 0, 100)
		orderIDs := make([]int64, 0, 100)

		for rows.Next() {
			o := &shared.Order{}
			rows.Scan(&o.ID, &o.Customer, &o.Total)
			orders = append(orders, o)
			orderIDs = append(orderIDs, o.ID)
		}
		rows.Close()

		if len(orders) != 100 {
			b.Fatalf("Expected 100 orders, got %d", len(orders))
		}

		// Load ALL items for ALL orders in a single query
		itemRows, err := db.QueryContext(ctx,
			"SELECT id, order_id, product, quantity, price FROM order_items WHERE order_id > $1 ORDER BY order_id", 0)
		if err != nil {
			b.Fatal(err)
		}

		// Group items by order_id
		itemsByOrder := make(map[int64][]*shared.OrderItem)
		for itemRows.Next() {
			item := &shared.OrderItem{}
			itemRows.Scan(&item.ID, &item.OrderID, &item.Product, &item.Quantity, &item.Price)
			itemsByOrder[item.OrderID] = append(itemsByOrder[item.OrderID], item)
		}
		itemRows.Close()

		// Load ALL notes for ALL orders in a single query
		noteRows, err := db.QueryContext(ctx,
			"SELECT id, order_id, content FROM order_notes WHERE order_id > $1 ORDER BY order_id", 0)
		if err != nil {
			b.Fatal(err)
		}

		// Group notes by order_id
		notesByOrder := make(map[int64][]*shared.OrderNote)
		for noteRows.Next() {
			note := &shared.OrderNote{}
			noteRows.Scan(&note.ID, &note.OrderID, &note.Content)
			notesByOrder[note.OrderID] = append(notesByOrder[note.OrderID], note)
		}
		noteRows.Close()

		// Attach items and notes to orders
		for _, order := range orders {
			order.Items = itemsByOrder[order.ID]
			order.Notes = notesByOrder[order.ID]
			if len(order.Items) != 3 {
				b.Fatalf("Expected 3 items for order %d, got %d", order.ID, len(order.Items))
			}
			if len(order.Notes) != 2 {
				b.Fatalf("Expected 2 notes for order %d, got %d", order.ID, len(order.Notes))
			}
		}
	}
}
