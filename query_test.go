//go:build e2e
// +build e2e

package orm1_test

import (
	"context"
	"testing"

	"github.com/hanpama/orm1"
)

func TestQueryFetchAll(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver orm1.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			registry := orm1.NewRegistry()
			registry.Register(&SimpleAuto{}, orm1.WithTable("simple_auto"))
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			// Insert test data
			entity1 := &SimpleAuto{Name: "Alice", Nullable: strPtr("nullable1")}
			entity2 := &SimpleAuto{Name: "Bob", Nullable: strPtr("nullable2")}
			entity3 := &SimpleAuto{Name: "Charlie", Nullable: nil}

			if err := session.Save(ctx, entity1); err != nil {
				t.Fatalf("Save failed: %v", err)
			}
			if err := session.Save(ctx, entity2); err != nil {
				t.Fatalf("Save failed: %v", err)
			}
			if err := session.Save(ctx, entity3); err != nil {
				t.Fatalf("Save failed: %v", err)
			}

			// Test FetchAll (no limit)
			query := orm1.NewEntityQuery[SimpleAuto](session, "sa")
			entities, err := query.FetchAll(ctx)
			if err != nil {
				t.Fatalf("FetchAll failed: %v", err)
			}

			if len(entities) != 3 {
				t.Errorf("Expected 3 entities, got %d", len(entities))
			}

			// Cleanup
			session.Delete(ctx, entity1)
			session.Delete(ctx, entity2)
			session.Delete(ctx, entity3)
		})
	}
}

func TestQueryFetchOne(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver orm1.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			registry := orm1.NewRegistry()
			registry.Register(&SimpleAuto{}, orm1.WithTable("simple_auto"))
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			// Insert test data
			entity1 := &SimpleAuto{Name: "Alice", Nullable: strPtr("nullable1")}
			entity2 := &SimpleAuto{Name: "Bob", Nullable: strPtr("nullable2")}

			if err := session.Save(ctx, entity1); err != nil {
				t.Fatalf("Save failed: %v", err)
			}
			if err := session.Save(ctx, entity2); err != nil {
				t.Fatalf("Save failed: %v", err)
			}

			// Test FetchOne with Where
			query := orm1.NewEntityQuery[SimpleAuto](session, "sa")
			query.Where("sa.name = ?", "Alice")
			entity, err := query.FetchOne(ctx)
			if err != nil {
				t.Fatalf("FetchOne failed: %v", err)
			}

			if entity == nil {
				t.Fatal("Expected entity, got nil")
			}
			if entity.Name != "Alice" {
				t.Errorf("Expected name 'Alice', got '%s'", entity.Name)
			}
			if entity.Nullable == nil || *entity.Nullable != "nullable1" {
				t.Errorf("Expected nullable 'nullable1', got %v", entity.Nullable)
			}

			// Cleanup
			session.Delete(ctx, entity1)
			session.Delete(ctx, entity2)
		})
	}
}

func TestQueryFetchOneEmpty(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver orm1.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			registry := orm1.NewRegistry()
			registry.Register(&SimpleAuto{}, orm1.WithTable("simple_auto"))
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			// Test FetchOne with no results
			query := orm1.NewEntityQuery[SimpleAuto](session, "sa")
			query.Where("sa.name = ?", "NonExistent")
			entity, err := query.FetchOne(ctx)
			if err != nil {
				t.Fatalf("FetchOne failed: %v", err)
			}

			if entity != nil {
				t.Error("Expected nil entity, got non-nil")
			}
		})
	}
}

func TestQueryFetchMany(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver orm1.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			registry := orm1.NewRegistry()
			registry.Register(&SimpleAuto{}, orm1.WithTable("simple_auto"))
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			// Insert test data with different IDs (auto-generated, ordered by insertion)
			entity1 := &SimpleAuto{Name: "Alice", Nullable: nil}
			entity2 := &SimpleAuto{Name: "Bob", Nullable: nil}
			entity3 := &SimpleAuto{Name: "Charlie", Nullable: nil}

			if err := session.Save(ctx, entity1); err != nil {
				t.Fatalf("Save failed: %v", err)
			}
			if err := session.Save(ctx, entity2); err != nil {
				t.Fatalf("Save failed: %v", err)
			}
			if err := session.Save(ctx, entity3); err != nil {
				t.Fatalf("Save failed: %v", err)
			}

			// Test FetchMany with limit
			query := orm1.NewEntityQuery[SimpleAuto](session, "sa")
			query.OrderBy(query.Asc("sa.id"))
			entities, err := query.FetchMany(ctx, 2)
			if err != nil {
				t.Fatalf("FetchMany failed: %v", err)
			}

			if len(entities) != 2 {
				t.Errorf("Expected 2 entities, got %d", len(entities))
			}
			if entities[0].Name != "Alice" {
				t.Errorf("Expected first entity 'Alice', got '%s'", entities[0].Name)
			}
			if entities[1].Name != "Bob" {
				t.Errorf("Expected second entity 'Bob', got '%s'", entities[1].Name)
			}

			// Cleanup
			session.Delete(ctx, entity1)
			session.Delete(ctx, entity2)
			session.Delete(ctx, entity3)
		})
	}
}

func TestQueryWhere(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver orm1.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			registry := orm1.NewRegistry()
			registry.Register(&SimpleAuto{}, orm1.WithTable("simple_auto"))
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			// Insert test data
			entity1 := &SimpleAuto{Name: "Alice", Nullable: nil}
			entity2 := &SimpleAuto{Name: "Bob", Nullable: nil}
			entity3 := &SimpleAuto{Name: "Charlie", Nullable: nil}

			if err := session.Save(ctx, entity1); err != nil {
				t.Fatalf("Save failed: %v", err)
			}
			if err := session.Save(ctx, entity2); err != nil {
				t.Fatalf("Save failed: %v", err)
			}
			if err := session.Save(ctx, entity3); err != nil {
				t.Fatalf("Save failed: %v", err)
			}

			// Test multiple Where conditions (AND)
			// entity1.ID < entity2.ID < entity3.ID (auto-increment order)
			query := orm1.NewEntityQuery[SimpleAuto](session, "sa")
			query.Where("sa.id > ?", entity1.ID).Where("sa.id < ?", entity3.ID)
			entities, err := query.FetchAll(ctx)
			if err != nil {
				t.Fatalf("FetchAll failed: %v", err)
			}

			if len(entities) != 1 {
				t.Errorf("Expected 1 entity, got %d", len(entities))
			}
			if len(entities) > 0 && entities[0].Name != "Bob" {
				t.Errorf("Expected entity 'Bob', got '%s'", entities[0].Name)
			}

			// Cleanup
			session.Delete(ctx, entity1)
			session.Delete(ctx, entity2)
			session.Delete(ctx, entity3)
		})
	}
}

func TestQueryOrderBy(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver orm1.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			registry := orm1.NewRegistry()
			registry.Register(&SimpleAuto{}, orm1.WithTable("simple_auto"))
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			// Insert test data (names in non-alphabetical order)
			entity1 := &SimpleAuto{Name: "Charlie", Nullable: nil}
			entity2 := &SimpleAuto{Name: "Alice", Nullable: nil}
			entity3 := &SimpleAuto{Name: "Bob", Nullable: nil}

			if err := session.Save(ctx, entity1); err != nil {
				t.Fatalf("Save failed: %v", err)
			}
			if err := session.Save(ctx, entity2); err != nil {
				t.Fatalf("Save failed: %v", err)
			}
			if err := session.Save(ctx, entity3); err != nil {
				t.Fatalf("Save failed: %v", err)
			}

			// Test OrderBy ASC
			query := orm1.NewEntityQuery[SimpleAuto](session, "sa")
			query.OrderBy(query.Asc("sa.name"))
			entities, err := query.FetchAll(ctx)
			if err != nil {
				t.Fatalf("FetchAll failed: %v", err)
			}

			if len(entities) != 3 {
				t.Fatalf("Expected 3 entities, got %d", len(entities))
			}
			if entities[0].Name != "Alice" {
				t.Errorf("Expected first entity 'Alice', got '%s'", entities[0].Name)
			}
			if entities[2].Name != "Charlie" {
				t.Errorf("Expected third entity 'Charlie', got '%s'", entities[2].Name)
			}

			// Test OrderBy DESC
			query2 := orm1.NewEntityQuery[SimpleAuto](session, "sa")
			query2.OrderBy(query2.Desc("sa.name"))
			entities2, err := query2.FetchAll(ctx)
			if err != nil {
				t.Fatalf("FetchAll failed: %v", err)
			}

			if entities2[0].Name != "Charlie" {
				t.Errorf("Expected first entity 'Charlie', got '%s'", entities2[0].Name)
			}
			if entities2[2].Name != "Alice" {
				t.Errorf("Expected third entity 'Alice', got '%s'", entities2[2].Name)
			}

			// Cleanup
			session.Delete(ctx, entity1)
			session.Delete(ctx, entity2)
			session.Delete(ctx, entity3)
		})
	}
}

func TestQueryLimitOffset(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver orm1.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			registry := orm1.NewRegistry()
			registry.Register(&SimpleAuto{}, orm1.WithTable("simple_auto"))
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			// Insert test data (5 entities)
			entity1 := &SimpleAuto{Name: "Alice", Nullable: nil}
			entity2 := &SimpleAuto{Name: "Bob", Nullable: nil}
			entity3 := &SimpleAuto{Name: "Charlie", Nullable: nil}
			entity4 := &SimpleAuto{Name: "David", Nullable: nil}
			entity5 := &SimpleAuto{Name: "Eve", Nullable: nil}

			if err := session.Save(ctx, entity1); err != nil {
				t.Fatalf("Save failed: %v", err)
			}
			if err := session.Save(ctx, entity2); err != nil {
				t.Fatalf("Save failed: %v", err)
			}
			if err := session.Save(ctx, entity3); err != nil {
				t.Fatalf("Save failed: %v", err)
			}
			if err := session.Save(ctx, entity4); err != nil {
				t.Fatalf("Save failed: %v", err)
			}
			if err := session.Save(ctx, entity5); err != nil {
				t.Fatalf("Save failed: %v", err)
			}

			// Test Limit only
			query1 := orm1.NewEntityQuery[SimpleAuto](session, "sa")
			query1.OrderBy(query1.Asc("sa.name"))
			entities1, err := query1.FetchMany(ctx, 2)
			if err != nil {
				t.Fatalf("FetchMany with limit failed: %v", err)
			}
			if len(entities1) != 2 {
				t.Errorf("Expected 2 entities with limit, got %d", len(entities1))
			}
			if len(entities1) > 0 && entities1[0].Name != "Alice" {
				t.Errorf("Expected first entity 'Alice', got '%s'", entities1[0].Name)
			}

			// Test Offset only (with large limit)
			query2 := orm1.NewEntityQuery[SimpleAuto](session, "sa")
			query2.OrderBy(query2.Asc("sa.name")).Offset(1)
			entities2, err := query2.FetchMany(ctx, 100)
			if err != nil {
				t.Fatalf("FetchMany with offset failed: %v", err)
			}
			if len(entities2) != 4 {
				t.Errorf("Expected 4 entities with offset, got %d", len(entities2))
			}
			if len(entities2) > 0 && entities2[0].Name != "Bob" {
				t.Errorf("Expected first entity 'Bob' (after offset), got '%s'", entities2[0].Name)
			}

			// Test Limit and Offset together
			query3 := orm1.NewEntityQuery[SimpleAuto](session, "sa")
			query3.OrderBy(query3.Asc("sa.name")).Offset(2)
			entities3, err := query3.FetchMany(ctx, 2)
			if err != nil {
				t.Fatalf("FetchMany with limit and offset failed: %v", err)
			}
			if len(entities3) != 2 {
				t.Errorf("Expected 2 entities with limit and offset, got %d", len(entities3))
			}
			// Ordered by name: Alice, Bob, Charlie, David, Eve
			// Offset 2 -> Charlie, David, Eve
			// Limit 2 -> Charlie, David
			if len(entities3) > 0 && entities3[0].Name != "Charlie" {
				t.Errorf("Expected first entity 'Charlie', got '%s'", entities3[0].Name)
			}
			if len(entities3) > 1 && entities3[1].Name != "David" {
				t.Errorf("Expected second entity 'David', got '%s'", entities3[1].Name)
			}

			// Cleanup
			session.Delete(ctx, entity1)
			session.Delete(ctx, entity2)
			session.Delete(ctx, entity3)
			session.Delete(ctx, entity4)
			session.Delete(ctx, entity5)
		})
	}
}

func TestQueryJoin(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver orm1.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			registry := orm1.NewRegistry()
			registry.Register(&Purchase{}, orm1.WithTable("purchase"))
			registry.Register(&PurchaseLineItem{}, orm1.WithTable("purchase_line_item"))
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			// Insert test data
			purchase1 := &Purchase{
				ID:         "purchase1",
				CustomerID: strPtr("customer1"),
				Price:      100.0,
			}
			purchase2 := &Purchase{
				ID:         "purchase2",
				CustomerID: strPtr("customer2"),
				Price:      200.0,
			}

			lineItem1 := &PurchaseLineItem{
				ID:         "lineitem1",
				PurchaseID: "purchase1",
				ItemIndex:  0,
				Product:    strPtr("Product A"),
				Quantity:   2,
			}
			lineItem2 := &PurchaseLineItem{
				ID:         "lineitem2",
				PurchaseID: "purchase1",
				ItemIndex:  1,
				Product:    strPtr("Product B"),
				Quantity:   3,
			}

			if err := session.Save(ctx, purchase1); err != nil {
				t.Fatalf("Save purchase1 failed: %v", err)
			}
			if err := session.Save(ctx, purchase2); err != nil {
				t.Fatalf("Save purchase2 failed: %v", err)
			}
			if err := session.Save(ctx, lineItem1); err != nil {
				t.Fatalf("Save lineItem1 failed: %v", err)
			}
			if err := session.Save(ctx, lineItem2); err != nil {
				t.Fatalf("Save lineItem2 failed: %v", err)
			}

			// Test INNER JOIN
			query := orm1.NewEntityQuery[Purchase](session, "p")
			query.Join("purchase_line_item", "pli", "p.id = pli.purchase_id")
			query.GroupByPrimaryKey()
			purchases, err := query.FetchAll(ctx)
			if err != nil {
				t.Fatalf("FetchAll failed: %v", err)
			}

			// Only purchase1 should be returned (has line items)
			if len(purchases) != 1 {
				t.Errorf("Expected 1 purchase, got %d", len(purchases))
			}
			if len(purchases) > 0 && purchases[0].ID != "purchase1" {
				t.Errorf("Expected purchase 'purchase1', got '%s'", purchases[0].ID)
			}

			// Cleanup
			session.Delete(ctx, lineItem1)
			session.Delete(ctx, lineItem2)
			session.Delete(ctx, purchase1)
			session.Delete(ctx, purchase2)
		})
	}
}

func TestQueryLeftJoin(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver orm1.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			registry := orm1.NewRegistry()
			registry.Register(&Purchase{}, orm1.WithTable("purchase"))
			registry.Register(&PurchaseLineItem{}, orm1.WithTable("purchase_line_item"))
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			// Insert test data
			purchase1 := &Purchase{
				ID:         "purchase_lj1",
				CustomerID: strPtr("customer1"),
				Price:      100.0,
			}
			purchase2 := &Purchase{
				ID:         "purchase_lj2",
				CustomerID: strPtr("customer2"),
				Price:      200.0,
			}

			lineItem1 := &PurchaseLineItem{
				ID:         "lineitem_lj1",
				PurchaseID: "purchase_lj1",
				ItemIndex:  0,
				Product:    strPtr("Product A"),
				Quantity:   2,
			}

			if err := session.Save(ctx, purchase1); err != nil {
				t.Fatalf("Save purchase1 failed: %v", err)
			}
			if err := session.Save(ctx, purchase2); err != nil {
				t.Fatalf("Save purchase2 failed: %v", err)
			}
			if err := session.Save(ctx, lineItem1); err != nil {
				t.Fatalf("Save lineItem1 failed: %v", err)
			}

			// Test LEFT JOIN
			query := orm1.NewEntityQuery[Purchase](session, "p")
			query.LeftJoin("purchase_line_item", "pli", "p.id = pli.purchase_id")
			query.GroupByPrimaryKey()
			query.OrderBy(query.Asc("p.id"))
			purchases, err := query.FetchAll(ctx)
			if err != nil {
				t.Fatalf("FetchAll failed: %v", err)
			}

			// Both purchases should be returned
			if len(purchases) != 2 {
				t.Errorf("Expected 2 purchases, got %d", len(purchases))
			}

			// Cleanup
			session.Delete(ctx, lineItem1)
			session.Delete(ctx, purchase1)
			session.Delete(ctx, purchase2)
		})
	}
}

func TestQueryCount(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver orm1.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			registry := orm1.NewRegistry()
			registry.Register(&SimpleAuto{}, orm1.WithTable("simple_auto"))
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			// Insert test data
			entity1 := &SimpleAuto{Name: "Alice", Nullable: nil}
			entity2 := &SimpleAuto{Name: "Bob", Nullable: nil}
			entity3 := &SimpleAuto{Name: "Charlie", Nullable: nil}

			if err := session.Save(ctx, entity1); err != nil {
				t.Fatalf("Save failed: %v", err)
			}
			if err := session.Save(ctx, entity2); err != nil {
				t.Fatalf("Save failed: %v", err)
			}
			if err := session.Save(ctx, entity3); err != nil {
				t.Fatalf("Save failed: %v", err)
			}

			// Test Count
			query := orm1.NewEntityQuery[SimpleAuto](session, "sa")
			count, err := query.Count(ctx)
			if err != nil {
				t.Fatalf("Count failed: %v", err)
			}

			if count != 3 {
				t.Errorf("Expected count 3, got %d", count)
			}

			// Test Count with Where
			query2 := orm1.NewEntityQuery[SimpleAuto](session, "sa")
			query2.Where("sa.id > ?", entity1.ID)
			count2, err := query2.Count(ctx)
			if err != nil {
				t.Fatalf("Count failed: %v", err)
			}

			if count2 != 2 {
				t.Errorf("Expected count 2, got %d", count2)
			}

			// Cleanup
			session.Delete(ctx, entity1)
			session.Delete(ctx, entity2)
			session.Delete(ctx, entity3)
		})
	}
}

func TestQueryHaving(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver orm1.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			registry := orm1.NewRegistry()
			registry.Register(&Purchase{}, orm1.WithTable("purchase"))
			registry.Register(&PurchaseLineItem{}, orm1.WithTable("purchase_line_item"))
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			// Insert test data
			purchase1 := &Purchase{
				ID:         "purchase_h1",
				CustomerID: strPtr("customer1"),
				Price:      100.0,
			}
			purchase2 := &Purchase{
				ID:         "purchase_h2",
				CustomerID: strPtr("customer2"),
				Price:      200.0,
			}

			// purchase1 has 2 line items, purchase2 has 1 line item
			lineItem1 := &PurchaseLineItem{
				ID:         "lineitem_h1",
				PurchaseID: "purchase_h1",
				ItemIndex:  0,
				Product:    strPtr("Product A"),
				Quantity:   2,
			}
			lineItem2 := &PurchaseLineItem{
				ID:         "lineitem_h2",
				PurchaseID: "purchase_h1",
				ItemIndex:  1,
				Product:    strPtr("Product B"),
				Quantity:   3,
			}
			lineItem3 := &PurchaseLineItem{
				ID:         "lineitem_h3",
				PurchaseID: "purchase_h2",
				ItemIndex:  0,
				Product:    strPtr("Product C"),
				Quantity:   1,
			}

			if err := session.Save(ctx, purchase1); err != nil {
				t.Fatalf("Save purchase1 failed: %v", err)
			}
			if err := session.Save(ctx, purchase2); err != nil {
				t.Fatalf("Save purchase2 failed: %v", err)
			}
			if err := session.Save(ctx, lineItem1); err != nil {
				t.Fatalf("Save lineItem1 failed: %v", err)
			}
			if err := session.Save(ctx, lineItem2); err != nil {
				t.Fatalf("Save lineItem2 failed: %v", err)
			}
			if err := session.Save(ctx, lineItem3); err != nil {
				t.Fatalf("Save lineItem3 failed: %v", err)
			}

			// Test HAVING clause
			query := orm1.NewEntityQuery[Purchase](session, "p")
			query.Join("purchase_line_item", "pli", "p.id = pli.purchase_id")
			query.GroupByPrimaryKey()
			query.Having("COUNT(pli.id) > ?", 1)
			purchases, err := query.FetchAll(ctx)
			if err != nil {
				t.Fatalf("FetchAll failed: %v", err)
			}

			// Only purchase1 should be returned (has more than 1 line item)
			if len(purchases) != 1 {
				t.Errorf("Expected 1 purchase, got %d", len(purchases))
			}
			if len(purchases) > 0 && purchases[0].ID != "purchase_h1" {
				t.Errorf("Expected purchase 'purchase_h1', got '%s'", purchases[0].ID)
			}

			// Cleanup
			session.Delete(ctx, lineItem1)
			session.Delete(ctx, lineItem2)
			session.Delete(ctx, lineItem3)
			session.Delete(ctx, purchase1)
			session.Delete(ctx, purchase2)
		})
	}
}
