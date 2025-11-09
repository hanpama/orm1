package tests

// Bun Benchmark Test Coverage:
// - Simple operations (6/6): ✅ All supported
// - Aggregate operations (3/6): ✅ Select/BatchSelect/ReadSlice supported
//
// Not Implemented (Bun's SQL-first philosophy):
// - Aggregate_Insert: No automatic transaction wrapping for nested entities
// - Aggregate_Update: No cascade update support
// - Aggregate_Delete: No cascade delete support
//
// Bun requires explicit SQL or manual transaction management for aggregate writes.

import (
	"context"
	"database/sql"
	"testing"

	"github.com/hanpama/orm1/benchmark/shared"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"
)

// Bun entities
type BunUser struct {
	bun.BaseModel `bun:"table:users"`

	ID    int64  `bun:"id,pk,autoincrement"`
	Name  string `bun:"name,notnull"`
	Email string `bun:"email,notnull"`
	Age   int    `bun:"age,notnull"`
}

type BunOrderItem struct {
	bun.BaseModel `bun:"table:order_items"`

	ID       int64   `bun:"id,pk,autoincrement"`
	OrderID  int64   `bun:"order_id,notnull"`
	Product  string  `bun:"product,notnull"`
	Quantity int     `bun:"quantity,notnull"`
	Price    float64 `bun:"price,notnull"`
}

type BunOrderNote struct {
	bun.BaseModel `bun:"table:order_notes"`

	ID      int64  `bun:"id,pk,autoincrement"`
	OrderID int64  `bun:"order_id,notnull"`
	Content string `bun:"content,notnull"`
}

type BunOrder struct {
	bun.BaseModel `bun:"table:orders"`

	ID       int64            `bun:"id,pk,autoincrement"`
	Customer string           `bun:"customer,notnull"`
	Total    float64          `bun:"total,notnull"`
	Items    []*BunOrderItem  `bun:"rel:has-many,join:id=order_id"`
	Notes    []*BunOrderNote  `bun:"rel:has-many,join:id=order_id"`
}

func setupBunPostgresDB(b *testing.B) (*bun.DB, func()) {
	sqldb := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN("postgres://testuser:testpass@localhost:25432/testdb?sslmode=disable")))

	db := bun.NewDB(sqldb, pgdialect.New())

	if err := sqldb.Ping(); err != nil {
		b.Skipf("PostgreSQL not available: %v", err)
	}

	// Setup schema using shared helper
	if err := shared.SetupPostgresSchema(sqldb); err != nil {
		b.Fatal(err)
	}

	cleanup := func() {
		shared.CleanupPostgresTables(sqldb)
		db.Close()
	}

	return db, cleanup
}

// -----------------------------------------------------------------------------
// Bun PostgreSQL - Simple CRUD
// -----------------------------------------------------------------------------

func BenchmarkBun_Postgres_Simple_Insert(b *testing.B) {
	db, cleanup := setupBunPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		user := &BunUser{
			Name:  "Alice",
			Email: "alice@example.com",
			Age:   30,
		}
		if _, err := db.NewInsert().Model(user).Exec(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBun_Postgres_Simple_Select(b *testing.B) {
	db, cleanup := setupBunPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate
	for i := 0; i < 100; i++ {
		user := &BunUser{
			Name:  "User",
			Email: "user@example.com",
			Age:   20 + (i % 50),
		}
		db.NewInsert().Model(user).Exec(ctx)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		user := new(BunUser)
		id := int64(1 + (i % 100))
		if err := db.NewSelect().Model(user).Where("id = ?", id).Scan(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBun_Postgres_Simple_Update(b *testing.B) {
	db, cleanup := setupBunPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate
	for i := 0; i < 100; i++ {
		user := &BunUser{
			Name:  "User",
			Email: "user@example.com",
			Age:   20,
		}
		db.NewInsert().Model(user).Exec(ctx)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		user := new(BunUser)
		id := int64(1 + (i % 100))
		if err := db.NewSelect().Model(user).Where("id = ?", id).Scan(ctx); err != nil {
			b.Fatal(err)
		}

		user.Age = 30
		if _, err := db.NewUpdate().Model(user).WherePK().Exec(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBun_Postgres_Simple_BatchSelect(b *testing.B) {
	db, cleanup := setupBunPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate 200 rows
	for i := 0; i < 200; i++ {
		user := &BunUser{
			Name:  "User",
			Email: "user@example.com",
			Age:   20 + (i % 50),
		}
		db.NewInsert().Model(user).Exec(ctx)
	}

	// Build 100 IDs
	ids := make([]int64, 100)
	for i := 0; i < 100; i++ {
		ids[i] = int64(i + 1)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var users []BunUser
		if err := db.NewSelect().Model(&users).Where("id IN (?)", bun.In(ids)).Scan(ctx); err != nil {
			b.Fatal(err)
		}
		if len(users) != 100 {
			b.Fatalf("Expected 100 users, got %d", len(users))
		}

		// Sort results to match requested ID order
		idMap := make(map[int64]*BunUser, len(users))
		for i := range users {
			idMap[users[i].ID] = &users[i]
		}
		sortedUsers := make([]BunUser, 0, len(ids))
		for _, id := range ids {
			if u, ok := idMap[id]; ok {
				sortedUsers = append(sortedUsers, *u)
			}
		}
		users = sortedUsers
	}
}

func BenchmarkBun_Postgres_Simple_ReadSlice(b *testing.B) {
	db, cleanup := setupBunPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate 100 rows
	for i := 0; i < 100; i++ {
		user := &BunUser{
			Name:  "User",
			Email: "user@example.com",
			Age:   20 + (i % 50),
		}
		db.NewInsert().Model(user).Exec(ctx)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var users []BunUser
		if err := db.NewSelect().Model(&users).Where("id > ?", 0).Limit(100).Scan(ctx); err != nil {
			b.Fatal(err)
		}
		if len(users) != 100 {
			b.Fatalf("Expected 100 users, got %d", len(users))
		}
	}
}

func BenchmarkBun_Postgres_Simple_Delete(b *testing.B) {
	db, cleanup := setupBunPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Pre-populate for each iteration
		user := &BunUser{
			Name:  "DeleteMe",
			Email: "delete@example.com",
			Age:   99,
		}
		db.NewInsert().Model(user).Exec(ctx)
		b.StartTimer()

		if _, err := db.NewDelete().Model(user).WherePK().Exec(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

// -----------------------------------------------------------------------------
// Bun PostgreSQL - Aggregate CRUD
// -----------------------------------------------------------------------------

func BenchmarkBun_Postgres_Aggregate_Select(b *testing.B) {
	db, cleanup := setupBunPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate
	for i := 0; i < 10; i++ {
		order := &BunOrder{
			Customer: "Customer",
			Total:    299.97,
		}
		db.NewInsert().Model(order).Exec(ctx)

		items := []*BunOrderItem{
			{OrderID: order.ID, Product: "Product A", Quantity: 1, Price: 99.99},
			{OrderID: order.ID, Product: "Product B", Quantity: 1, Price: 99.99},
			{OrderID: order.ID, Product: "Product C", Quantity: 1, Price: 99.99},
		}
		db.NewInsert().Model(&items).Exec(ctx)

		notes := []*BunOrderNote{
			{OrderID: order.ID, Content: "Please deliver before 5pm"},
			{OrderID: order.ID, Content: "Gift wrap requested"},
		}
		db.NewInsert().Model(&notes).Exec(ctx)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		order := new(BunOrder)
		id := int64(1 + (i % 10))
		if err := db.NewSelect().Model(order).Where("id = ?", id).Relation("Items").Relation("Notes").Scan(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBun_Postgres_Aggregate_BatchSelect(b *testing.B) {
	db, cleanup := setupBunPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate 100 orders with 3 items and 2 notes each
	for i := 0; i < 100; i++ {
		order := &BunOrder{
			Customer: "Customer",
			Total:    299.97,
		}
		db.NewInsert().Model(order).Exec(ctx)

		items := []*BunOrderItem{
			{OrderID: order.ID, Product: "Product A", Quantity: 1, Price: 99.99},
			{OrderID: order.ID, Product: "Product B", Quantity: 1, Price: 99.99},
			{OrderID: order.ID, Product: "Product C", Quantity: 1, Price: 99.99},
		}
		db.NewInsert().Model(&items).Exec(ctx)

		notes := []*BunOrderNote{
			{OrderID: order.ID, Content: "Please deliver before 5pm"},
			{OrderID: order.ID, Content: "Gift wrap requested"},
		}
		db.NewInsert().Model(&notes).Exec(ctx)
	}

	// Build 10 IDs
	ids := make([]int64, 10)
	for i := 0; i < 10; i++ {
		ids[i] = int64(i + 1)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var orders []BunOrder
		if err := db.NewSelect().Model(&orders).Where("id IN (?)", bun.In(ids)).Relation("Items").Relation("Notes").Scan(ctx); err != nil {
			b.Fatal(err)
		}
		if len(orders) != 10 {
			b.Fatalf("Expected 10 orders, got %d", len(orders))
		}

		// Sort results to match requested ID order
		idMap := make(map[int64]*BunOrder, len(orders))
		for i := range orders {
			idMap[orders[i].ID] = &orders[i]
		}
		sortedOrders := make([]BunOrder, 0, len(ids))
		for _, id := range ids {
			if o, ok := idMap[id]; ok {
				sortedOrders = append(sortedOrders, *o)
			}
		}
		orders = sortedOrders
	}
}

func BenchmarkBun_Postgres_Aggregate_ReadSlice(b *testing.B) {
	db, cleanup := setupBunPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate 100 orders with 3 items and 2 notes each
	for i := 0; i < 100; i++ {
		order := &BunOrder{
			Customer: "Customer",
			Total:    299.97 + float64(i),
		}
		db.NewInsert().Model(order).Exec(ctx)

		items := []*BunOrderItem{
			{OrderID: order.ID, Product: "Product A", Quantity: 1, Price: 99.99},
			{OrderID: order.ID, Product: "Product B", Quantity: 1, Price: 99.99},
			{OrderID: order.ID, Product: "Product C", Quantity: 1, Price: 99.99},
		}
		db.NewInsert().Model(&items).Exec(ctx)

		notes := []*BunOrderNote{
			{OrderID: order.ID, Content: "Please deliver before 5pm"},
			{OrderID: order.ID, Content: "Gift wrap requested"},
		}
		db.NewInsert().Model(&notes).Exec(ctx)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var orders []BunOrder
		if err := db.NewSelect().Model(&orders).Where("id > ?", 0).Limit(100).Relation("Items").Relation("Notes").Scan(ctx); err != nil {
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
			if len(order.Notes) != 2 {
				b.Fatalf("Expected 2 notes, got %d", len(order.Notes))
			}
		}
	}
}
