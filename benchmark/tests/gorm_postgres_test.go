package tests

// GORM Benchmark Test Coverage:
// - Simple operations (6/6): ✅ All supported
// - Aggregate operations (6/6): ✅ All supported with automatic transactions
//
// GORM provides the most complete ORM experience:
// - Automatic transaction wrapping for Create() with nested associations
// - FullSaveAssociations flag for cascade updates
// - Select().Delete() for cascade deletes
// This is the gold standard implementation for ORM benchmarks.

import (
	"context"
	"testing"

	"github.com/hanpama/orm1/benchmark/shared"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// GORM entities - must use gorm.Model or define ID explicitly
type GormUser struct {
	ID    uint   `gorm:"primaryKey;autoIncrement"`
	Name  string `gorm:"not null"`
	Email string `gorm:"not null"`
	Age   int    `gorm:"not null"`
}

func (GormUser) TableName() string {
	return "users"
}

type GormOrder struct {
	ID       uint             `gorm:"primaryKey;autoIncrement"`
	Customer string           `gorm:"not null"`
	Total    float64          `gorm:"not null"`
	Items    []GormOrderItem  `gorm:"foreignKey:OrderID"`
	Notes    []GormOrderNote  `gorm:"foreignKey:OrderID"`
}

func (GormOrder) TableName() string {
	return "orders"
}

type GormOrderItem struct {
	ID       uint    `gorm:"primaryKey;autoIncrement"`
	OrderID  uint    `gorm:"not null"`
	Product  string  `gorm:"not null"`
	Quantity int     `gorm:"not null"`
	Price    float64 `gorm:"not null"`
}

func (GormOrderItem) TableName() string {
	return "order_items"
}

type GormOrderNote struct {
	ID      uint   `gorm:"primaryKey;autoIncrement"`
	OrderID uint   `gorm:"not null"`
	Content string `gorm:"not null"`
}

func (GormOrderNote) TableName() string {
	return "order_notes"
}

func setupGORMPostgresDB(b *testing.B) (*gorm.DB, func()) {
	db, err := shared.GetPostgresDB()
	if err != nil {
		b.Skipf("PostgreSQL not available: %v", err)
	}

	if err := shared.SetupPostgresSchema(db); err != nil {
		b.Fatal(err)
	}

	gormDB, err := gorm.Open(postgres.New(postgres.Config{
		Conn: db,
	}), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		b.Fatal(err)
	}

	cleanup := func() {
		shared.CleanupPostgresTables(db)
		db.Close()
	}

	return gormDB, cleanup
}

// -----------------------------------------------------------------------------
// GORM PostgreSQL - Simple CRUD
// -----------------------------------------------------------------------------

func BenchmarkGORM_Postgres_Simple_Insert(b *testing.B) {
	gormDB, cleanup := setupGORMPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		user := &GormUser{
			Name:  "Alice",
			Email: "alice@example.com",
			Age:   30,
		}
		if err := gormDB.WithContext(ctx).Create(user).Error; err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGORM_Postgres_Simple_Select(b *testing.B) {
	gormDB, cleanup := setupGORMPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate
	for i := 0; i < 100; i++ {
		user := &GormUser{
			Name:  "User",
			Email: "user@example.com",
			Age:   20 + (i % 50),
		}
		gormDB.Create(user)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var user GormUser
		id := uint(1 + (i % 100))
		if err := gormDB.WithContext(ctx).First(&user, id).Error; err != nil && err != gorm.ErrRecordNotFound {
			b.Fatal(err)
		}
	}
}

func BenchmarkGORM_Postgres_Simple_Update(b *testing.B) {
	gormDB, cleanup := setupGORMPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate
	for i := 0; i < 100; i++ {
		user := &GormUser{
			Name:  "User",
			Email: "user@example.com",
			Age:   20,
		}
		gormDB.Create(user)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var user GormUser
		id := uint(1 + (i % 100))
		if err := gormDB.WithContext(ctx).First(&user, id).Error; err != nil {
			b.Fatal(err)
		}

		user.Age = 30
		if err := gormDB.WithContext(ctx).Save(&user).Error; err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGORM_Postgres_Simple_BatchSelect(b *testing.B) {
	gormDB, cleanup := setupGORMPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate 200 rows
	for i := 0; i < 200; i++ {
		user := &GormUser{
			Name:  "User",
			Email: "user@example.com",
			Age:   20 + (i % 50),
		}
		gormDB.Create(user)
	}

	// Build 100 IDs
	ids := make([]uint, 100)
	for i := 0; i < 100; i++ {
		ids[i] = uint(i + 1)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var users []GormUser
		if err := gormDB.WithContext(ctx).Find(&users, ids).Error; err != nil {
			b.Fatal(err)
		}
		if len(users) != 100 {
			b.Fatalf("Expected 100 users, got %d", len(users))
		}

		// Sort results to match requested ID order
		idMap := make(map[uint]*GormUser, len(users))
		for i := range users {
			idMap[users[i].ID] = &users[i]
		}
		sortedUsers := make([]GormUser, 0, len(ids))
		for _, id := range ids {
			if u, ok := idMap[id]; ok {
				sortedUsers = append(sortedUsers, *u)
			}
		}
		users = sortedUsers
	}
}

func BenchmarkGORM_Postgres_Simple_ReadSlice(b *testing.B) {
	gormDB, cleanup := setupGORMPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate 100 rows
	for i := 0; i < 100; i++ {
		user := &GormUser{
			Name:  "User",
			Email: "user@example.com",
			Age:   20 + (i % 50),
		}
		gormDB.WithContext(ctx).Create(user)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var users []GormUser
		if err := gormDB.WithContext(ctx).Where("id > ?", 0).Limit(100).Find(&users).Error; err != nil {
			b.Fatal(err)
		}
		if len(users) != 100 {
			b.Fatalf("Expected 100 users, got %d", len(users))
		}
	}
}

func BenchmarkGORM_Postgres_Simple_Delete(b *testing.B) {
	gormDB, cleanup := setupGORMPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Pre-populate for each iteration
		user := &GormUser{
			Name:  "DeleteMe",
			Email: "delete@example.com",
			Age:   99,
		}
		gormDB.Create(user)
		b.StartTimer()

		if err := gormDB.WithContext(ctx).Delete(user).Error; err != nil {
			b.Fatal(err)
		}
	}
}

// -----------------------------------------------------------------------------
// GORM PostgreSQL - Aggregate CRUD
// -----------------------------------------------------------------------------

func BenchmarkGORM_Postgres_Aggregate_Insert(b *testing.B) {
	gormDB, cleanup := setupGORMPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		order := &GormOrder{
			Customer: "John Doe",
			Total:    299.97,
			Items: []GormOrderItem{
				{Product: "Product A", Quantity: 1, Price: 99.99},
				{Product: "Product B", Quantity: 1, Price: 99.99},
				{Product: "Product C", Quantity: 1, Price: 99.99},
			},
			Notes: []GormOrderNote{
				{Content: "Please deliver before 5pm"},
				{Content: "Gift wrap requested"},
			},
		}

		if err := gormDB.WithContext(ctx).Create(order).Error; err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGORM_Postgres_Aggregate_Select(b *testing.B) {
	gormDB, cleanup := setupGORMPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate
	for i := 0; i < 10; i++ {
		order := &GormOrder{
			Customer: "Customer",
			Total:    299.97,
			Items: []GormOrderItem{
				{Product: "Product A", Quantity: 1, Price: 99.99},
				{Product: "Product B", Quantity: 1, Price: 99.99},
				{Product: "Product C", Quantity: 1, Price: 99.99},
			},
			Notes: []GormOrderNote{
				{Content: "Please deliver before 5pm"},
				{Content: "Gift wrap requested"},
			},
		}
		gormDB.Create(order)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var order GormOrder
		id := uint(1 + (i % 10))
		if err := gormDB.WithContext(ctx).Preload("Items").Preload("Notes").First(&order, id).Error; err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGORM_Postgres_Aggregate_BatchSelect(b *testing.B) {
	gormDB, cleanup := setupGORMPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate 100 orders with 3 items and 2 notes each
	for i := 0; i < 100; i++ {
		order := &GormOrder{
			Customer: "Customer",
			Total:    299.97,
			Items: []GormOrderItem{
				{Product: "Product A", Quantity: 1, Price: 99.99},
				{Product: "Product B", Quantity: 1, Price: 99.99},
				{Product: "Product C", Quantity: 1, Price: 99.99},
			},
			Notes: []GormOrderNote{
				{Content: "Please deliver before 5pm"},
				{Content: "Gift wrap requested"},
			},
		}
		gormDB.Create(order)
	}

	// Build 10 IDs
	ids := []uint{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var orders []GormOrder
		if err := gormDB.WithContext(ctx).Preload("Items").Preload("Notes").Find(&orders, ids).Error; err != nil {
			b.Fatal(err)
		}
		if len(orders) != 10 {
			b.Fatalf("Expected 10 orders, got %d", len(orders))
		}

		// Sort results to match requested ID order
		idMap := make(map[uint]*GormOrder, len(orders))
		for i := range orders {
			idMap[orders[i].ID] = &orders[i]
		}
		sortedOrders := make([]GormOrder, 0, len(ids))
		for _, id := range ids {
			if o, ok := idMap[id]; ok {
				sortedOrders = append(sortedOrders, *o)
			}
		}
		orders = sortedOrders
	}
}

func BenchmarkGORM_Postgres_Aggregate_Update(b *testing.B) {
	gormDB, cleanup := setupGORMPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate
	for i := 0; i < 10; i++ {
		order := &GormOrder{
			Customer: "Customer",
			Total:    299.97,
			Items: []GormOrderItem{
				{Product: "Product A", Quantity: 1, Price: 99.99},
				{Product: "Product B", Quantity: 1, Price: 99.99},
				{Product: "Product C", Quantity: 1, Price: 99.99},
			},
			Notes: []GormOrderNote{
				{Content: "Please deliver before 5pm"},
				{Content: "Gift wrap requested"},
			},
		}
		gormDB.Create(order)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var order GormOrder
		id := uint(1 + (i % 10))
		if err := gormDB.WithContext(ctx).Preload("Items").Preload("Notes").First(&order, id).Error; err != nil {
			b.Fatal(err)
		}

		order.Total = 399.96
		order.Items[0].Price = 149.99
		order.Items[1].Price = 124.99
		order.Items[2].Price = 124.98

		// Save with FullSaveAssociations to update all associations in one call
		if err := gormDB.Session(&gorm.Session{FullSaveAssociations: true}).WithContext(ctx).Save(&order).Error; err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGORM_Postgres_Aggregate_Delete(b *testing.B) {
	gormDB, cleanup := setupGORMPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Pre-populate for each iteration
		order := &GormOrder{
			Customer: "DeleteMe",
			Total:    299.97,
			Items: []GormOrderItem{
				{Product: "Product A", Quantity: 1, Price: 99.99},
				{Product: "Product B", Quantity: 1, Price: 99.99},
				{Product: "Product C", Quantity: 1, Price: 99.99},
			},
			Notes: []GormOrderNote{
				{Content: "Please deliver before 5pm"},
				{Content: "Gift wrap requested"},
			},
		}
		gormDB.Create(order)
		b.StartTimer()

		if err := gormDB.WithContext(ctx).Select("Items", "Notes").Delete(order).Error; err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGORM_Postgres_Aggregate_ReadSlice(b *testing.B) {
	gormDB, cleanup := setupGORMPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate 100 orders with 3 items and 2 notes each
	for i := 0; i < 100; i++ {
		order := &GormOrder{
			Customer: "Customer",
			Total:    299.97 + float64(i),
			Items: []GormOrderItem{
				{Product: "Product A", Quantity: 1, Price: 99.99},
				{Product: "Product B", Quantity: 1, Price: 99.99},
				{Product: "Product C", Quantity: 1, Price: 99.99},
			},
			Notes: []GormOrderNote{
				{Content: "Please deliver before 5pm"},
				{Content: "Gift wrap requested"},
			},
		}
		gormDB.Create(order)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var orders []GormOrder
		if err := gormDB.WithContext(ctx).Preload("Items").Preload("Notes").Where("id > ?", 0).Limit(100).Find(&orders).Error; err != nil {
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
