package tests

// Ent Benchmark Test Coverage:
// - Simple operations (6/6): ✅ All supported
// - Aggregate operations (6/6): ✅ All supported with transaction wrappers
//
// Note: Aggregate Insert/Update require explicit tx.Client() usage for atomicity.
// This ensures fair comparison with ORM1/GORM which provide automatic transactions.

import (
	"context"
	"testing"

	"entgo.io/ent/dialect/sql"
	"github.com/hanpama/orm1/benchmark/ent"
	"github.com/hanpama/orm1/benchmark/ent/order"
	"github.com/hanpama/orm1/benchmark/ent/orderitem"
	"github.com/hanpama/orm1/benchmark/ent/ordernote"
	"github.com/hanpama/orm1/benchmark/ent/user"
	"github.com/hanpama/orm1/benchmark/shared"
	_ "github.com/lib/pq"
)

func setupEntPostgresDB(b *testing.B) (*ent.Client, func()) {
	db, err := shared.GetPostgresDB()
	if err != nil {
		b.Skipf("PostgreSQL not available: %v", err)
	}

	// Setup schema using shared helper
	if err := shared.SetupPostgresSchema(db); err != nil {
		b.Fatal(err)
	}

	// Create Ent client with existing DB connection
	drv := sql.OpenDB("postgres", db)
	client := ent.NewClient(ent.Driver(drv))

	cleanup := func() {
		shared.CleanupPostgresTables(db)
		client.Close()
		db.Close()
	}

	return client, cleanup
}

// -----------------------------------------------------------------------------
// Ent PostgreSQL - Simple CRUD
// -----------------------------------------------------------------------------

func BenchmarkEnt_Postgres_Simple_Insert(b *testing.B) {
	client, cleanup := setupEntPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := client.User.Create().
			SetName("Alice").
			SetEmail("alice@example.com").
			SetAge(30).
			Save(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEnt_Postgres_Simple_Select(b *testing.B) {
	client, cleanup := setupEntPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate
	for i := 0; i < 100; i++ {
		client.User.Create().
			SetName("User").
			SetEmail("user@example.com").
			SetAge(20 + (i % 50)).
			Save(ctx)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		id := int64(1 + (i % 100))
		_, err := client.User.Get(ctx, id)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEnt_Postgres_Simple_Update(b *testing.B) {
	client, cleanup := setupEntPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate
	for i := 0; i < 100; i++ {
		client.User.Create().
			SetName("User").
			SetEmail("user@example.com").
			SetAge(20).
			Save(ctx)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		id := int64(1 + (i % 100))
		user, err := client.User.Get(ctx, id)
		if err != nil {
			b.Fatal(err)
		}

		_, err = client.User.UpdateOne(user).
			SetAge(30).
			Save(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEnt_Postgres_Simple_BatchSelect(b *testing.B) {
	client, cleanup := setupEntPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate 200 rows
	for i := 0; i < 200; i++ {
		client.User.Create().
			SetName("User").
			SetEmail("user@example.com").
			SetAge(20 + (i % 50)).
			Save(ctx)
	}

	// Build 100 IDs
	ids := make([]int64, 100)
	for i := 0; i < 100; i++ {
		ids[i] = int64(i + 1)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		users, err := client.User.Query().
			Where(user.IDIn(ids...)).
			All(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if len(users) != 100 {
			b.Fatalf("Expected 100 users, got %d", len(users))
		}

		// Sort results to match requested ID order
		idMap := make(map[int64]*ent.User, len(users))
		for _, u := range users {
			idMap[u.ID] = u
		}
		sortedUsers := make([]*ent.User, 0, len(ids))
		for _, id := range ids {
			if u, ok := idMap[id]; ok {
				sortedUsers = append(sortedUsers, u)
			}
		}
		users = sortedUsers
	}
}

func BenchmarkEnt_Postgres_Simple_ReadSlice(b *testing.B) {
	client, cleanup := setupEntPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate 100 rows
	for i := 0; i < 100; i++ {
		client.User.Create().
			SetName("User").
			SetEmail("user@example.com").
			SetAge(20 + (i % 50)).
			Save(ctx)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		users, err := client.User.Query().
			Where(user.IDGT(0)).
			Limit(100).
			All(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if len(users) != 100 {
			b.Fatalf("Expected 100 users, got %d", len(users))
		}
	}
}

func BenchmarkEnt_Postgres_Simple_Delete(b *testing.B) {
	client, cleanup := setupEntPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Pre-populate for each iteration
		user, err := client.User.Create().
			SetName("DeleteMe").
			SetEmail("delete@example.com").
			SetAge(99).
			Save(ctx)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		err = client.User.DeleteOne(user).Exec(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// -----------------------------------------------------------------------------
// Ent PostgreSQL - Aggregate CRUD
// -----------------------------------------------------------------------------

func BenchmarkEnt_Postgres_Aggregate_Insert(b *testing.B) {
	client, cleanup := setupEntPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Wrap in transaction for atomicity
		tx, err := client.Tx(ctx)
		if err != nil {
			b.Fatal(err)
		}

		// Create order
		order, err := tx.Order.Create().
			SetCustomer("John Doe").
			SetTotal(299.97).
			Save(ctx)
		if err != nil {
			tx.Rollback()
			b.Fatal(err)
		}

		// Create items
		itemBulk := make([]*ent.OrderItemCreate, 3)
		itemBulk[0] = tx.OrderItem.Create().
			SetOrder(order).
			SetProduct("Product A").
			SetQuantity(1).
			SetPrice(99.99)
		itemBulk[1] = tx.OrderItem.Create().
			SetOrder(order).
			SetProduct("Product B").
			SetQuantity(1).
			SetPrice(99.99)
		itemBulk[2] = tx.OrderItem.Create().
			SetOrder(order).
			SetProduct("Product C").
			SetQuantity(1).
			SetPrice(99.99)

		_, err = tx.OrderItem.CreateBulk(itemBulk...).Save(ctx)
		if err != nil {
			tx.Rollback()
			b.Fatal(err)
		}

		// Create notes
		noteBulk := make([]*ent.OrderNoteCreate, 2)
		noteBulk[0] = tx.OrderNote.Create().
			SetOrder(order).
			SetContent("Please deliver before 5pm")
		noteBulk[1] = tx.OrderNote.Create().
			SetOrder(order).
			SetContent("Gift wrap requested")

		_, err = tx.OrderNote.CreateBulk(noteBulk...).Save(ctx)
		if err != nil {
			tx.Rollback()
			b.Fatal(err)
		}

		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEnt_Postgres_Aggregate_Select(b *testing.B) {
	client, cleanup := setupEntPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate
	for i := 0; i < 10; i++ {
		order, _ := client.Order.Create().
			SetCustomer("Customer").
			SetTotal(299.97).
			Save(ctx)

		itemBulk := make([]*ent.OrderItemCreate, 3)
		itemBulk[0] = client.OrderItem.Create().SetOrder(order).SetProduct("Product A").SetQuantity(1).SetPrice(99.99)
		itemBulk[1] = client.OrderItem.Create().SetOrder(order).SetProduct("Product B").SetQuantity(1).SetPrice(99.99)
		itemBulk[2] = client.OrderItem.Create().SetOrder(order).SetProduct("Product C").SetQuantity(1).SetPrice(99.99)
		client.OrderItem.CreateBulk(itemBulk...).Save(ctx)

		noteBulk := make([]*ent.OrderNoteCreate, 2)
		noteBulk[0] = client.OrderNote.Create().SetOrder(order).SetContent("Please deliver before 5pm")
		noteBulk[1] = client.OrderNote.Create().SetOrder(order).SetContent("Gift wrap requested")
		client.OrderNote.CreateBulk(noteBulk...).Save(ctx)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		id := int64(1 + (i % 10))
		_, err := client.Order.Query().
			Where(order.ID(id)).
			WithItems().
			WithNotes().
			Only(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEnt_Postgres_Aggregate_BatchSelect(b *testing.B) {
	client, cleanup := setupEntPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate 100 orders with 3 items and 2 notes each
	for i := 0; i < 100; i++ {
		order, _ := client.Order.Create().
			SetCustomer("Customer").
			SetTotal(299.97).
			Save(ctx)

		itemBulk := make([]*ent.OrderItemCreate, 3)
		itemBulk[0] = client.OrderItem.Create().SetOrder(order).SetProduct("Product A").SetQuantity(1).SetPrice(99.99)
		itemBulk[1] = client.OrderItem.Create().SetOrder(order).SetProduct("Product B").SetQuantity(1).SetPrice(99.99)
		itemBulk[2] = client.OrderItem.Create().SetOrder(order).SetProduct("Product C").SetQuantity(1).SetPrice(99.99)
		client.OrderItem.CreateBulk(itemBulk...).Save(ctx)

		noteBulk := make([]*ent.OrderNoteCreate, 2)
		noteBulk[0] = client.OrderNote.Create().SetOrder(order).SetContent("Please deliver before 5pm")
		noteBulk[1] = client.OrderNote.Create().SetOrder(order).SetContent("Gift wrap requested")
		client.OrderNote.CreateBulk(noteBulk...).Save(ctx)
	}

	// Build 10 IDs
	ids := make([]int64, 10)
	for i := 0; i < 10; i++ {
		ids[i] = int64(i + 1)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		orders, err := client.Order.Query().
			Where(order.IDIn(ids...)).
			WithItems().
			WithNotes().
			All(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if len(orders) != 10 {
			b.Fatalf("Expected 10 orders, got %d", len(orders))
		}

		// Sort results to match requested ID order
		idMap := make(map[int64]*ent.Order, len(orders))
		for _, o := range orders {
			idMap[o.ID] = o
		}
		sortedOrders := make([]*ent.Order, 0, len(ids))
		for _, id := range ids {
			if o, ok := idMap[id]; ok {
				sortedOrders = append(sortedOrders, o)
			}
		}
		orders = sortedOrders
	}
}

func BenchmarkEnt_Postgres_Aggregate_Update(b *testing.B) {
	client, cleanup := setupEntPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate
	for i := 0; i < 10; i++ {
		order, _ := client.Order.Create().
			SetCustomer("Customer").
			SetTotal(299.97).
			Save(ctx)

		itemBulk := make([]*ent.OrderItemCreate, 3)
		itemBulk[0] = client.OrderItem.Create().SetOrder(order).SetProduct("Product A").SetQuantity(1).SetPrice(99.99)
		itemBulk[1] = client.OrderItem.Create().SetOrder(order).SetProduct("Product B").SetQuantity(1).SetPrice(99.99)
		itemBulk[2] = client.OrderItem.Create().SetOrder(order).SetProduct("Product C").SetQuantity(1).SetPrice(99.99)
		client.OrderItem.CreateBulk(itemBulk...).Save(ctx)

		noteBulk := make([]*ent.OrderNoteCreate, 2)
		noteBulk[0] = client.OrderNote.Create().SetOrder(order).SetContent("Please deliver before 5pm")
		noteBulk[1] = client.OrderNote.Create().SetOrder(order).SetContent("Gift wrap requested")
		client.OrderNote.CreateBulk(noteBulk...).Save(ctx)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		id := int64(1 + (i % 10))

		// Wrap in transaction for atomicity
		tx, err := client.Tx(ctx)
		if err != nil {
			b.Fatal(err)
		}

		orderResult, err := tx.Order.Query().
			Where(order.ID(id)).
			WithItems().
			WithNotes().
			Only(ctx)
		if err != nil {
			tx.Rollback()
			b.Fatal(err)
		}

		// Update order
		_, err = tx.Order.UpdateOne(orderResult).
			SetTotal(399.96).
			Save(ctx)
		if err != nil {
			tx.Rollback()
			b.Fatal(err)
		}

		// Bulk upsert items with updated prices using sql/upsert feature
		err = tx.OrderItem.CreateBulk(
			tx.OrderItem.Create().
				SetID(orderResult.Edges.Items[0].ID).
				SetOrder(orderResult).
				SetProduct(orderResult.Edges.Items[0].Product).
				SetQuantity(orderResult.Edges.Items[0].Quantity).
				SetPrice(149.99),
			tx.OrderItem.Create().
				SetID(orderResult.Edges.Items[1].ID).
				SetOrder(orderResult).
				SetProduct(orderResult.Edges.Items[1].Product).
				SetQuantity(orderResult.Edges.Items[1].Quantity).
				SetPrice(124.99),
			tx.OrderItem.Create().
				SetID(orderResult.Edges.Items[2].ID).
				SetOrder(orderResult).
				SetProduct(orderResult.Edges.Items[2].Product).
				SetQuantity(orderResult.Edges.Items[2].Quantity).
				SetPrice(124.98),
		).OnConflictColumns(orderitem.FieldID).UpdateNewValues().Exec(ctx)
		if err != nil {
			tx.Rollback()
			b.Fatal(err)
		}

		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEnt_Postgres_Aggregate_ReadSlice(b *testing.B) {
	client, cleanup := setupEntPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	// Pre-populate 100 orders with 3 items and 2 notes each
	for i := 0; i < 100; i++ {
		order, _ := client.Order.Create().
			SetCustomer("Customer").
			SetTotal(299.97 + float64(i)).
			Save(ctx)

		itemBulk := make([]*ent.OrderItemCreate, 3)
		itemBulk[0] = client.OrderItem.Create().SetOrder(order).SetProduct("Product A").SetQuantity(1).SetPrice(99.99)
		itemBulk[1] = client.OrderItem.Create().SetOrder(order).SetProduct("Product B").SetQuantity(1).SetPrice(99.99)
		itemBulk[2] = client.OrderItem.Create().SetOrder(order).SetProduct("Product C").SetQuantity(1).SetPrice(99.99)
		client.OrderItem.CreateBulk(itemBulk...).Save(ctx)

		noteBulk := make([]*ent.OrderNoteCreate, 2)
		noteBulk[0] = client.OrderNote.Create().SetOrder(order).SetContent("Please deliver before 5pm")
		noteBulk[1] = client.OrderNote.Create().SetOrder(order).SetContent("Gift wrap requested")
		client.OrderNote.CreateBulk(noteBulk...).Save(ctx)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		orders, err := client.Order.Query().
			Where(order.IDGT(0)).
			WithItems().
			WithNotes().
			Limit(100).
			All(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if len(orders) != 100 {
			b.Fatalf("Expected 100 orders, got %d", len(orders))
		}
		// Verify children are loaded
		for _, orderResult := range orders {
			if len(orderResult.Edges.Items) != 3 {
				b.Fatalf("Expected 3 items, got %d", len(orderResult.Edges.Items))
			}
			if len(orderResult.Edges.Notes) != 2 {
				b.Fatalf("Expected 2 notes, got %d", len(orderResult.Edges.Notes))
			}
		}
	}
}

func BenchmarkEnt_Postgres_Aggregate_Delete(b *testing.B) {
	client, cleanup := setupEntPostgresDB(b)
	defer cleanup()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Pre-populate for each iteration
		orderEntity, _ := client.Order.Create().
			SetCustomer("DeleteMe").
			SetTotal(299.97).
			Save(ctx)

		itemBulk := make([]*ent.OrderItemCreate, 3)
		itemBulk[0] = client.OrderItem.Create().SetOrder(orderEntity).SetProduct("Product A").SetQuantity(1).SetPrice(99.99)
		itemBulk[1] = client.OrderItem.Create().SetOrder(orderEntity).SetProduct("Product B").SetQuantity(1).SetPrice(99.99)
		itemBulk[2] = client.OrderItem.Create().SetOrder(orderEntity).SetProduct("Product C").SetQuantity(1).SetPrice(99.99)
		client.OrderItem.CreateBulk(itemBulk...).Save(ctx)

		noteBulk := make([]*ent.OrderNoteCreate, 2)
		noteBulk[0] = client.OrderNote.Create().SetOrder(orderEntity).SetContent("Please deliver before 5pm")
		noteBulk[1] = client.OrderNote.Create().SetOrder(orderEntity).SetContent("Gift wrap requested")
		client.OrderNote.CreateBulk(noteBulk...).Save(ctx)
		b.StartTimer()

		// Delete items first (FK constraint)
		_, err := client.OrderItem.Delete().
			Where(orderitem.HasOrderWith(order.ID(orderEntity.ID))).
			Exec(ctx)
		if err != nil {
			b.Fatal(err)
		}

		// Delete notes (FK constraint)
		_, err = client.OrderNote.Delete().
			Where(ordernote.HasOrderWith(order.ID(orderEntity.ID))).
			Exec(ctx)
		if err != nil {
			b.Fatal(err)
		}

		// Delete order
		err = client.Order.DeleteOne(orderEntity).Exec(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}
}
