package tests

// ORM1 Benchmark Test Coverage:
// - Simple operations (6/6): ✅ All supported
// - Aggregate operations (6/6): ✅ All supported with automatic transactions
//
// ORM1 is the primary test subject for these benchmarks.

import (
	"context"
	"testing"

	"github.com/hanpama/orm1/benchmark/shared"

	"github.com/hanpama/orm1"
)

func setupORM1PostgresDB(b *testing.B) (*orm1.SessionFactory, func()) {
	db, err := shared.GetPostgresDB()
	if err != nil {
		b.Skipf("PostgreSQL not available: %v", err)
	}

	if err := shared.SetupPostgresSchema(db); err != nil {
		b.Fatal(err)
	}

	registry := orm1.NewRegistry()
	registry.Register(&shared.User{}, orm1.WithTable("users"))
	registry.Register(&shared.Order{}, orm1.WithTable("orders"))
	registry.Register(&shared.OrderItem{}, orm1.WithTable("order_items"))
	registry.Register(&shared.OrderNote{}, orm1.WithTable("order_notes"))

	driver := orm1.NewPostgreSQLDriver(db)
	factory := orm1.NewSessionFactory(registry, driver)

	cleanup := func() {
		shared.CleanupPostgresTables(db)
		db.Close()
	}

	return factory, cleanup
}

// -----------------------------------------------------------------------------
// ORM1 PostgreSQL - Simple CRUD
// -----------------------------------------------------------------------------

func BenchmarkORM1_Postgres_Simple_Insert(b *testing.B) {
	factory, cleanup := setupORM1PostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		session := factory.CreateSession()
		user := &shared.User{
			Name:  "Alice",
			Email: "alice@example.com",
			Age:   30,
		}
		if err := session.Save(ctx, user); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkORM1_Postgres_Simple_Select(b *testing.B) {
	factory, cleanup := setupORM1PostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate
	for i := 0; i < 100; i++ {
		session := factory.CreateSession()
		user := &shared.User{
			Name:  "User",
			Email: "user@example.com",
			Age:   20 + (i % 50),
		}
		session.Save(ctx, user)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		session := factory.CreateSession()
		var user *shared.User
		id := int64(1 + (i % 100))
		if err := session.Get(ctx, &user, orm1.NewKey(id)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkORM1_Postgres_Simple_Update(b *testing.B) {
	factory, cleanup := setupORM1PostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate
	for i := 0; i < 100; i++ {
		session := factory.CreateSession()
		user := &shared.User{
			Name:  "User",
			Email: "user@example.com",
			Age:   20,
		}
		session.Save(ctx, user)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		session := factory.CreateSession()
		var user *shared.User
		id := int64(1 + (i % 100))
		if err := session.Get(ctx, &user, orm1.NewKey(id)); err != nil {
			b.Fatal(err)
		}

		if user != nil {
			user.Age = 30
			if err := session.Save(ctx, user); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkORM1_Postgres_Simple_BatchSelect(b *testing.B) {
	factory, cleanup := setupORM1PostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate 200 rows
	for i := 0; i < 200; i++ {
		session := factory.CreateSession()
		user := &shared.User{
			Name:  "User",
			Email: "user@example.com",
			Age:   20 + (i % 50),
		}
		session.Save(ctx, user)
	}

	// Build 100 keys
	keys := make([]orm1.Key, 100)
	for i := 0; i < 100; i++ {
		keys[i] = orm1.NewKey(int64(i + 1))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		session := factory.CreateSession()
		var users []*shared.User
		if err := session.BatchGet(ctx, &users, keys); err != nil {
			b.Fatal(err)
		}
		if len(users) != 100 {
			b.Fatalf("Expected 100 users, got %d", len(users))
		}
	}
}

func BenchmarkORM1_Postgres_Simple_ReadSlice(b *testing.B) {
	factory, cleanup := setupORM1PostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate 100 rows
	for i := 0; i < 100; i++ {
		session := factory.CreateSession()
		user := &shared.User{
			Name:  "User",
			Email: "user@example.com",
			Age:   20 + (i % 50),
		}
		session.Save(ctx, user)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		session := factory.CreateSession()
		query := orm1.NewEntityQuery[shared.User](session, "u")
		users, err := query.Where("u.id > ?", 0).FetchMany(ctx, 100)
		if err != nil {
			b.Fatal(err)
		}
		if len(users) != 100 {
			b.Fatalf("Expected 100 users, got %d", len(users))
		}
	}
}

func BenchmarkORM1_Postgres_Simple_Delete(b *testing.B) {
	factory, cleanup := setupORM1PostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Pre-populate for each iteration
		session := factory.CreateSession()
		user := &shared.User{
			Name:  "DeleteMe",
			Email: "delete@example.com",
			Age:   99,
		}
		session.Save(ctx, user)
		b.StartTimer()

		if err := session.Delete(ctx, user); err != nil {
			b.Fatal(err)
		}
	}
}

// -----------------------------------------------------------------------------
// ORM1 PostgreSQL - Aggregate CRUD
// -----------------------------------------------------------------------------

func BenchmarkORM1_Postgres_Aggregate_Insert(b *testing.B) {
	factory, cleanup := setupORM1PostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		session := factory.CreateSession()

		// Use transaction for atomic insert
		tx, err := session.Begin(ctx, nil); if err != nil {
			b.Fatal(err)
		}

		order := &shared.Order{
			Customer: "John Doe",
			Total:    299.97,
			Items: []*shared.OrderItem{
				{Product: "Product A", Quantity: 1, Price: 99.99},
				{Product: "Product B", Quantity: 1, Price: 99.99},
				{Product: "Product C", Quantity: 1, Price: 99.99},
			},
			Notes: []*shared.OrderNote{
				{Content: "Please deliver before 5pm"},
				{Content: "Gift wrap requested"},
			},
		}

		if err := session.Save(ctx, order); err != nil {
			tx.Rollback(ctx)
			b.Fatal(err)
		}

		if err := tx.Commit(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkORM1_Postgres_Aggregate_Select(b *testing.B) {
	factory, cleanup := setupORM1PostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate
	for i := 0; i < 10; i++ {
		session := factory.CreateSession()
		order := &shared.Order{
			Customer: "Customer",
			Total:    299.97,
			Items: []*shared.OrderItem{
				{Product: "Product A", Quantity: 1, Price: 99.99},
				{Product: "Product B", Quantity: 1, Price: 99.99},
				{Product: "Product C", Quantity: 1, Price: 99.99},
			},
			Notes: []*shared.OrderNote{
				{Content: "Please deliver before 5pm"},
				{Content: "Gift wrap requested"},
			},
		}
		session.Save(ctx, order)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		session := factory.CreateSession()
		var order *shared.Order
		id := int64(1 + (i % 10))
		if err := session.Get(ctx, &order, orm1.NewKey(id)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkORM1_Postgres_Aggregate_BatchSelect(b *testing.B) {
	factory, cleanup := setupORM1PostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate 100 orders with 3 items each
	for i := 0; i < 100; i++ {
		session := factory.CreateSession()
		order := &shared.Order{
			Customer: "Customer",
			Total:    299.97,
			Items: []*shared.OrderItem{
				{Product: "Product A", Quantity: 1, Price: 99.99},
				{Product: "Product B", Quantity: 1, Price: 99.99},
				{Product: "Product C", Quantity: 1, Price: 99.99},
			},
			Notes: []*shared.OrderNote{
				{Content: "Please deliver before 5pm"},
				{Content: "Gift wrap requested"},
			},
		}
		session.Save(ctx, order)
	}

	// Build 10 keys
	keys := make([]orm1.Key, 10)
	for i := 0; i < 10; i++ {
		keys[i] = orm1.NewKey(int64(i + 1))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		session := factory.CreateSession()
		var orders []*shared.Order
		if err := session.BatchGet(ctx, &orders, keys); err != nil {
			b.Fatal(err)
		}
		if len(orders) != 10 {
			b.Fatalf("Expected 10 orders, got %d", len(orders))
		}
	}
}

func BenchmarkORM1_Postgres_Aggregate_Update(b *testing.B) {
	factory, cleanup := setupORM1PostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate
	for i := 0; i < 10; i++ {
		session := factory.CreateSession()
		order := &shared.Order{
			Customer: "Customer",
			Total:    299.97,
			Items: []*shared.OrderItem{
				{Product: "Product A", Quantity: 1, Price: 99.99},
				{Product: "Product B", Quantity: 1, Price: 99.99},
				{Product: "Product C", Quantity: 1, Price: 99.99},
			},
			Notes: []*shared.OrderNote{
				{Content: "Please deliver before 5pm"},
				{Content: "Gift wrap requested"},
			},
		}
		session.Save(ctx, order)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		session := factory.CreateSession()
		var order *shared.Order
		id := int64(1 + (i % 10))
		if err := session.Get(ctx, &order, orm1.NewKey(id)); err != nil {
			b.Fatal(err)
		}

		if order != nil {
			order.Total = 399.96
			order.Items[0].Price = 149.99
			order.Items[1].Price = 124.99
			order.Items[2].Price = 124.98

			// Use transaction for atomic update
			tx, err := session.Begin(ctx, nil); if err != nil {
				b.Fatal(err)
			}

			if err := session.Save(ctx, order); err != nil {
				tx.Rollback(ctx)
				b.Fatal(err)
			}

			if err := tx.Commit(ctx); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkORM1_Postgres_Aggregate_ReadSlice(b *testing.B) {
	factory, cleanup := setupORM1PostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate 100 orders with 3 items each
	for i := 0; i < 100; i++ {
		session := factory.CreateSession()
		order := &shared.Order{
			Customer: "Customer",
			Total:    299.97 + float64(i),
			Items: []*shared.OrderItem{
				{Product: "Product A", Quantity: 1, Price: 99.99},
				{Product: "Product B", Quantity: 1, Price: 99.99},
				{Product: "Product C", Quantity: 1, Price: 99.99},
			},
		}
		session.Save(ctx, order)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		session := factory.CreateSession()
		query := orm1.NewEntityQuery[shared.Order](session, "o")
		orders, err := query.Where("o.id > ?", 0).FetchMany(ctx, 100)
		if err != nil {
			b.Fatal(err)
		}
		if len(orders) != 100 {
			b.Fatalf("Expected 100 orders, got %d", len(orders))
		}
		// Verify children are loaded
		for _, order := range orders {
			if len(order.Items) != 3 {
				b.Fatalf("Expected 3 items, got %d", len(order.Items))
			}
		}
	}
}

func BenchmarkORM1_Postgres_Aggregate_Delete(b *testing.B) {
	factory, cleanup := setupORM1PostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Pre-populate for each iteration
		session := factory.CreateSession()
		order := &shared.Order{
			Customer: "DeleteMe",
			Total:    299.97,
			Items: []*shared.OrderItem{
				{Product: "Product A", Quantity: 1, Price: 99.99},
				{Product: "Product B", Quantity: 1, Price: 99.99},
				{Product: "Product C", Quantity: 1, Price: 99.99},
			},
		}
		session.Save(ctx, order)
		b.StartTimer()

		// Use transaction for atomic delete
		tx, err := session.Begin(ctx, nil); if err != nil {
			b.Fatal(err)
		}

		if err := session.Delete(ctx, order); err != nil {
			tx.Rollback(ctx)
			b.Fatal(err)
		}

		if err := tx.Commit(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

// -----------------------------------------------------------------------------
// ORM1 PostgreSQL - Pagination
// -----------------------------------------------------------------------------

func BenchmarkORM1_Postgres_Pagination_Forward_FirstPage(b *testing.B) {
	factory, cleanup := setupORM1PostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate 100 users
	db, _ := shared.GetPostgresDB()
	defer db.Close()
	for i := 0; i < 100; i++ {
		db.ExecContext(ctx,
			"INSERT INTO users (name, email, age) VALUES ($1, $2, $3)",
			"User", "user@example.com", 20+(i%50))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		session := factory.CreateSession()
		query := orm1.NewEntityQuery[shared.User](session, "u")
		query.OrderBy(query.AscNullsLast("u.id"))

		first := 10
		page, err := query.Paginate(ctx, nil, &first, nil, nil)
		if err != nil {
			b.Fatal(err)
		}
		if len(page.Cursors) != 10 {
			b.Fatalf("Expected 10 cursors, got %d", len(page.Cursors))
		}
	}
}

func BenchmarkORM1_Postgres_Pagination_Forward_SecondPage(b *testing.B) {
	factory, cleanup := setupORM1PostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate 100 users
	db, _ := shared.GetPostgresDB()
	defer db.Close()
	for i := 0; i < 100; i++ {
		db.ExecContext(ctx,
			"INSERT INTO users (name, email, age) VALUES ($1, $2, $3)",
			"User", "user@example.com", 20+(i%50))
	}

	// Get first page to establish cursor
	session := factory.CreateSession()
	query := orm1.NewEntityQuery[shared.User](session, "u")
	query.OrderBy(query.AscNullsLast("u.id"))
	first := 10
	firstPage, _ := query.Paginate(ctx, nil, &first, nil, nil)
	afterCursor := firstPage.Cursors[9] // Last cursor of first page

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		session := factory.CreateSession()
		query := orm1.NewEntityQuery[shared.User](session, "u")
		query.OrderBy(query.AscNullsLast("u.id"))

		page, err := query.Paginate(ctx, afterCursor, &first, nil, nil)
		if err != nil {
			b.Fatal(err)
		}
		if len(page.Cursors) != 10 {
			b.Fatalf("Expected 10 cursors, got %d", len(page.Cursors))
		}
	}
}

func BenchmarkORM1_Postgres_Pagination_Backward_FirstPage(b *testing.B) {
	factory, cleanup := setupORM1PostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate 100 users
	db, _ := shared.GetPostgresDB()
	defer db.Close()
	for i := 0; i < 100; i++ {
		db.ExecContext(ctx,
			"INSERT INTO users (name, email, age) VALUES ($1, $2, $3)",
			"User", "user@example.com", 20+(i%50))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		session := factory.CreateSession()
		query := orm1.NewEntityQuery[shared.User](session, "u")
		query.OrderBy(query.AscNullsLast("u.id"))

		last := 10
		page, err := query.Paginate(ctx, nil, nil, nil, &last)
		if err != nil {
			b.Fatal(err)
		}
		if len(page.Cursors) != 10 {
			b.Fatalf("Expected 10 cursors, got %d", len(page.Cursors))
		}
	}
}

func BenchmarkORM1_Postgres_Pagination_Backward_SecondPage(b *testing.B) {
	factory, cleanup := setupORM1PostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate 100 users
	db, _ := shared.GetPostgresDB()
	defer db.Close()
	for i := 0; i < 100; i++ {
		db.ExecContext(ctx,
			"INSERT INTO users (name, email, age) VALUES ($1, $2, $3)",
			"User", "user@example.com", 20+(i%50))
	}

	// Get last page (backward first page) to establish cursor
	session := factory.CreateSession()
	query := orm1.NewEntityQuery[shared.User](session, "u")
	query.OrderBy(query.AscNullsLast("u.id"))
	last := 10
	lastPage, _ := query.Paginate(ctx, nil, nil, nil, &last)
	beforeCursor := lastPage.Cursors[0] // First cursor of last page

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		session := factory.CreateSession()
		query := orm1.NewEntityQuery[shared.User](session, "u")
		query.OrderBy(query.AscNullsLast("u.id"))

		page, err := query.Paginate(ctx, nil, nil, beforeCursor, &last)
		if err != nil {
			b.Fatal(err)
		}
		if len(page.Cursors) != 10 {
			b.Fatalf("Expected 10 cursors, got %d", len(page.Cursors))
		}
	}
}

// -----------------------------------------------------------------------------
// ORM1 PostgreSQL - RawQuery
// -----------------------------------------------------------------------------

func BenchmarkORM1_Postgres_RawQuery_ScanOne(b *testing.B) {
	factory, cleanup := setupORM1PostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate 100 users
	db, _ := shared.GetPostgresDB()
	defer db.Close()
	for i := 0; i < 100; i++ {
		db.ExecContext(ctx,
			"INSERT INTO users (name, email, age) VALUES ($1, $2, $3)",
			"User", "user@example.com", 20+(i%50))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		session := factory.CreateSession()

		type UserResult struct {
			ID    int64
			Name  string
			Email string
			Age   int
		}

		var result UserResult
		query := orm1.NewRawQuery(session, "SELECT id, name, email, age FROM users WHERE id = ?", int64(1+(i%100)))
		if err := query.ScanOne(ctx, &result); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkORM1_Postgres_RawQuery_ScanAll_10Rows(b *testing.B) {
	factory, cleanup := setupORM1PostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate 100 users
	db, _ := shared.GetPostgresDB()
	defer db.Close()
	for i := 0; i < 100; i++ {
		db.ExecContext(ctx,
			"INSERT INTO users (name, email, age) VALUES ($1, $2, $3)",
			"User", "user@example.com", 20+(i%50))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		session := factory.CreateSession()

		type UserResult struct {
			ID    int64
			Name  string
			Email string
			Age   int
		}

		var results []*UserResult
		query := orm1.NewRawQuery(session, "SELECT id, name, email, age FROM users LIMIT 10")
		if err := query.ScanAll(ctx, &results); err != nil {
			b.Fatal(err)
		}
		if len(results) != 10 {
			b.Fatalf("Expected 10 results, got %d", len(results))
		}
	}
}

func BenchmarkORM1_Postgres_RawQuery_ScanAll_100Rows(b *testing.B) {
	factory, cleanup := setupORM1PostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate 100 users
	db, _ := shared.GetPostgresDB()
	defer db.Close()
	for i := 0; i < 100; i++ {
		db.ExecContext(ctx,
			"INSERT INTO users (name, email, age) VALUES ($1, $2, $3)",
			"User", "user@example.com", 20+(i%50))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		session := factory.CreateSession()

		type UserResult struct {
			ID    int64
			Name  string
			Email string
			Age   int
		}

		var results []*UserResult
		query := orm1.NewRawQuery(session, "SELECT id, name, email, age FROM users")
		if err := query.ScanAll(ctx, &results); err != nil {
			b.Fatal(err)
		}
		if len(results) != 100 {
			b.Fatalf("Expected 100 results, got %d", len(results))
		}
	}
}
