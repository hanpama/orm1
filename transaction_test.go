//go:build e2e
// +build e2e

package orm1_test

import (
	"context"
	"strings"
	"testing"

	"github.com/hanpama/orm1"
)

// TestTransaction_StateRestoration_Rollback verifies that Rollback restores
// the session state (persisted map and children map) to the snapshot taken at Begin.
func TestTransaction_StateRestoration_Rollback(t *testing.T) {
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
			registry.Register(&Purchase{})
			registry.Register(&PurchaseLineItem{})
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			defer func() {
				orm1.NewRawQuery(session, "DELETE FROM purchase_line_item").Exec(ctx)
				orm1.NewRawQuery(session, "DELETE FROM purchase").Exec(ctx)
			}()

			// Setup: purchase1 with line item persisted before transaction
			purchase1 := &Purchase{
				ID:    "p1",
				Price: 100.0,
				LineItems: []*PurchaseLineItem{
					{ID: "li1", PurchaseID: "p1", Quantity: 1},
				},
			}
			if err := session.Save(ctx, purchase1); err != nil {
				t.Fatalf("Setup Save failed: %v", err)
			}

			// Begin transaction
			tx, err := session.Begin(ctx, nil)
			if err != nil {
				t.Fatalf("Begin failed: %v", err)
			}

			// Within transaction: purchase2 with line item becomes persisted
			purchase2 := &Purchase{
				ID:    "p2",
				Price: 200.0,
				LineItems: []*PurchaseLineItem{
					{ID: "li2", PurchaseID: "p2", Quantity: 2},
				},
			}
			if err := session.Save(ctx, purchase2); err != nil {
				tx.Rollback(ctx)
				t.Fatalf("Save failed: %v", err)
			}

			// Rollback
			if err := tx.Rollback(ctx); err != nil {
				t.Fatalf("Rollback failed: %v", err)
			}

			// Verify: purchase1 still persisted (UPDATE), purchase2 not persisted (INSERT)
			purchase1.Price = 150.0
			if err := session.Save(ctx, purchase1); err != nil {
				t.Fatalf("Save purchase1 after rollback failed: %v", err)
			}

			if err := session.Save(ctx, purchase2); err != nil {
				t.Fatalf("Save purchase2 after rollback failed: %v", err)
			}

			// Verify database state
			var got1, got2 *Purchase
			session.Get(ctx, &got1, orm1.NewKey("p1"))
			session.Get(ctx, &got2, orm1.NewKey("p2"))

			if got1 == nil || got1.Price != 150.0 {
				t.Errorf("purchase1 should be updated, got: %v", got1)
			}
			if got2 == nil || got2.Price != 200.0 {
				t.Errorf("purchase2 should exist, got: %v", got2)
			}
		})
	}
}

// TestTransaction_StateRetention_Commit verifies that Commit does NOT restore
// the session state - changes made during transaction are retained.
func TestTransaction_StateRetention_Commit(t *testing.T) {
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
			registry.Register(&BlogPost{}, orm1.WithTable("blog_posts"))
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			defer func() {
				orm1.NewRawQuery(session, "DELETE FROM blog_posts").Exec(ctx)
			}()

			// Setup: entity1 is persisted before transaction
			entity1 := &BlogPost{ID: 1, Title: "Post 1"}
			if err := session.Save(ctx, entity1); err != nil {
				t.Fatalf("Setup Save failed: %v", err)
			}

			// Begin transaction
			tx, err := session.Begin(ctx, nil)
			if err != nil {
				t.Fatalf("Begin failed: %v", err)
			}

			// Within transaction: entity2 becomes persisted
			entity2 := &BlogPost{ID: 2, Title: "Post 2"}
			if err := session.Save(ctx, entity2); err != nil {
				tx.Rollback(ctx)
				t.Fatalf("Save failed: %v", err)
			}

			// Commit
			if err := tx.Commit(ctx); err != nil {
				t.Fatalf("Commit failed: %v", err)
			}

			// Verify: both entity1 and entity2 are persisted
			// Test by calling Save - both should UPDATE
			entity1.Title = "Updated 1"
			entity2.Title = "Updated 2"
			if err := session.Save(ctx, entity1); err != nil {
				t.Fatalf("Save entity1 after commit failed: %v", err)
			}
			if err := session.Save(ctx, entity2); err != nil {
				t.Fatalf("Save entity2 after commit failed: %v", err)
			}

			// Verify database state
			var got1, got2 *BlogPost
			session.Get(ctx, &got1, orm1.NewKey(int64(1)))
			session.Get(ctx, &got2, orm1.NewKey(int64(2)))

			if got1 == nil || got1.Title != "Updated 1" {
				t.Errorf("entity1 should be updated, got: %v", got1)
			}
			if got2 == nil || got2.Title != "Updated 2" {
				t.Errorf("entity2 should be updated, got: %v", got2)
			}
		})
	}
}

// TestTransaction_Nested_InnerRollback verifies that inner transaction rollback
// does not affect outer transaction state (including children map).
func TestTransaction_Nested_InnerRollback(t *testing.T) {
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
			registry.Register(&Purchase{})
			registry.Register(&PurchaseLineItem{})
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			defer func() {
				orm1.NewRawQuery(session, "DELETE FROM purchase_line_item").Exec(ctx)
				orm1.NewRawQuery(session, "DELETE FROM purchase").Exec(ctx)
			}()

			// Begin outer transaction
			txOuter, err := session.Begin(ctx, nil)
			if err != nil {
				t.Fatalf("Outer Begin failed: %v", err)
			}

			// purchase1 with children persisted in outer transaction
			purchase1 := &Purchase{
				ID:    "p1",
				Price: 100.0,
				LineItems: []*PurchaseLineItem{
					{ID: "li1", PurchaseID: "p1", Quantity: 1},
				},
			}
			if err := session.Save(ctx, purchase1); err != nil {
				txOuter.Rollback(ctx)
				t.Fatalf("Outer Save failed: %v", err)
			}

			// Begin inner transaction
			txInner, err := session.Begin(ctx, nil)
			if err != nil {
				txOuter.Rollback(ctx)
				t.Fatalf("Inner Begin failed: %v", err)
			}

			// purchase2 with children persisted in inner transaction
			purchase2 := &Purchase{
				ID:    "p2",
				Price: 200.0,
				LineItems: []*PurchaseLineItem{
					{ID: "li2", PurchaseID: "p2", Quantity: 2},
				},
			}
			if err := session.Save(ctx, purchase2); err != nil {
				txInner.Rollback(ctx)
				txOuter.Rollback(ctx)
				t.Fatalf("Inner Save failed: %v", err)
			}

			// Rollback inner transaction
			if err := txInner.Rollback(ctx); err != nil {
				txOuter.Rollback(ctx)
				t.Fatalf("Inner Rollback failed: %v", err)
			}

			// Commit outer transaction
			if err := txOuter.Commit(ctx); err != nil {
				t.Fatalf("Outer Commit failed: %v", err)
			}

			// Verify: purchase1 persisted (UPDATE), purchase2 not persisted (INSERT)
			purchase1.Price = 150.0
			if err := session.Save(ctx, purchase1); err != nil {
				t.Fatalf("Save purchase1 after commit failed: %v", err)
			}
			if err := session.Save(ctx, purchase2); err != nil {
				t.Fatalf("Save purchase2 after rollback failed: %v", err)
			}

			// Verify database
			var got1, got2 *Purchase
			session.Get(ctx, &got1, orm1.NewKey("p1"))
			session.Get(ctx, &got2, orm1.NewKey("p2"))

			if got1 == nil || got1.Price != 150.0 {
				t.Errorf("purchase1 should be updated, got: %v", got1)
			}
			if got2 == nil || got2.Price != 200.0 {
				t.Errorf("purchase2 should exist (re-inserted), got: %v", got2)
			}
		})
	}
}

// TestTransaction_Nested_OuterRollback verifies that outer transaction rollback
// restores state to before outer Begin, regardless of inner commit (including children map).
func TestTransaction_Nested_OuterRollback(t *testing.T) {
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
			registry.Register(&Purchase{})
			registry.Register(&PurchaseLineItem{})
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			defer func() {
				orm1.NewRawQuery(session, "DELETE FROM purchase_line_item").Exec(ctx)
				orm1.NewRawQuery(session, "DELETE FROM purchase").Exec(ctx)
			}()

			// Begin outer transaction
			txOuter, err := session.Begin(ctx, nil)
			if err != nil {
				t.Fatalf("Outer Begin failed: %v", err)
			}

			// purchase1 with children persisted in outer transaction
			purchase1 := &Purchase{
				ID:    "p1",
				Price: 100.0,
				LineItems: []*PurchaseLineItem{
					{ID: "li1", PurchaseID: "p1", Quantity: 1},
				},
			}
			if err := session.Save(ctx, purchase1); err != nil {
				txOuter.Rollback(ctx)
				t.Fatalf("Outer Save failed: %v", err)
			}

			// Begin inner transaction
			txInner, err := session.Begin(ctx, nil)
			if err != nil {
				txOuter.Rollback(ctx)
				t.Fatalf("Inner Begin failed: %v", err)
			}

			// purchase2 with children persisted in inner transaction
			purchase2 := &Purchase{
				ID:    "p2",
				Price: 200.0,
				LineItems: []*PurchaseLineItem{
					{ID: "li2", PurchaseID: "p2", Quantity: 2},
				},
			}
			if err := session.Save(ctx, purchase2); err != nil {
				txInner.Rollback(ctx)
				txOuter.Rollback(ctx)
				t.Fatalf("Inner Save failed: %v", err)
			}

			// Commit inner transaction
			if err := txInner.Commit(ctx); err != nil {
				txOuter.Rollback(ctx)
				t.Fatalf("Inner Commit failed: %v", err)
			}

			// Rollback outer transaction
			if err := txOuter.Rollback(ctx); err != nil {
				t.Fatalf("Outer Rollback failed: %v", err)
			}

			// Verify: neither purchase is persisted (both should INSERT)
			if err := session.Save(ctx, purchase1); err != nil {
				t.Fatalf("Save purchase1 after rollback failed: %v", err)
			}
			if err := session.Save(ctx, purchase2); err != nil {
				t.Fatalf("Save purchase2 after rollback failed: %v", err)
			}

			// Verify database
			var got1, got2 *Purchase
			session.Get(ctx, &got1, orm1.NewKey("p1"))
			session.Get(ctx, &got2, orm1.NewKey("p2"))

			if got1 == nil || got1.Price != 100.0 {
				t.Errorf("purchase1 should exist (inserted), got: %v", got1)
			}
			if got2 == nil || got2.Price != 200.0 {
				t.Errorf("purchase2 should exist (inserted), got: %v", got2)
			}
		})
	}
}

// TestTransaction_Idempotency_DoubleCommit verifies that calling Commit twice
// is safe and does not cause errors (defer safety).
func TestTransaction_Idempotency_DoubleCommit(t *testing.T) {
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
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			tx, err := session.Begin(ctx, nil)
			if err != nil {
				t.Fatalf("Begin failed: %v", err)
			}

			// First Commit
			if err := tx.Commit(ctx); err != nil {
				t.Fatalf("First Commit failed: %v", err)
			}

			// Second Commit (should be no-op)
			if err := tx.Commit(ctx); err != nil {
				t.Errorf("Second Commit should be no-op, got error: %v", err)
			}
		})
	}
}

// TestTransaction_Idempotency_CommitThenRollback verifies that calling Rollback
// after Commit is safe (defer safety).
func TestTransaction_Idempotency_CommitThenRollback(t *testing.T) {
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
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			tx, err := session.Begin(ctx, nil)
			if err != nil {
				t.Fatalf("Begin failed: %v", err)
			}

			// Commit
			if err := tx.Commit(ctx); err != nil {
				t.Fatalf("Commit failed: %v", err)
			}

			// Rollback after Commit (should be no-op)
			if err := tx.Rollback(ctx); err != nil {
				t.Errorf("Rollback after Commit should be no-op, got error: %v", err)
			}
		})
	}
}

// TestTransaction_Idempotency_DoubleRollback verifies that calling Rollback twice
// is safe and does not cause errors (defer safety).
func TestTransaction_Idempotency_DoubleRollback(t *testing.T) {
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
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			tx, err := session.Begin(ctx, nil)
			if err != nil {
				t.Fatalf("Begin failed: %v", err)
			}

			// First Rollback
			if err := tx.Rollback(ctx); err != nil {
				t.Fatalf("First Rollback failed: %v", err)
			}

			// Second Rollback (should be no-op)
			if err := tx.Rollback(ctx); err != nil {
				t.Errorf("Second Rollback should be no-op, got error: %v", err)
			}
		})
	}
}

// TestTransaction_Validation_NestedOptions verifies that nested transactions
// cannot change isolation level or read-only option.
func TestTransaction_Validation_NestedOptions(t *testing.T) {
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
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			// Begin outer transaction
			txOuter, err := session.Begin(ctx, nil)
			if err != nil {
				t.Fatalf("Outer Begin failed: %v", err)
			}
			defer txOuter.Rollback(ctx)

			// Attempt to change isolation level in nested transaction
			_, err = session.Begin(ctx, orm1.SerializableTxOptions())
			if err == nil {
				t.Error("Expected error when changing isolation in nested transaction")
			}
			if !strings.Contains(err.Error(), "cannot specify isolation level") {
				t.Errorf("Expected 'cannot specify isolation level' error, got: %v", err)
			}

			// Attempt to set read-only in nested transaction
			_, err = session.Begin(ctx, orm1.ReadOnlyTxOptions())
			if err == nil {
				t.Error("Expected error when setting read-only in nested transaction")
			}
			if !strings.Contains(err.Error(), "cannot specify read-only") {
				t.Errorf("Expected 'cannot specify read-only' error, got: %v", err)
			}
		})
	}
}

// TestTransaction_Validation_DepthMismatch verifies that committing a parent
// transaction while child is still active returns an error.
func TestTransaction_Validation_DepthMismatch(t *testing.T) {
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
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			// Begin outer transaction
			txOuter, err := session.Begin(ctx, nil)
			if err != nil {
				t.Fatalf("Outer Begin failed: %v", err)
			}

			// Begin inner transaction
			txInner, err := session.Begin(ctx, nil)
			if err != nil {
				txOuter.Rollback(ctx)
				t.Fatalf("Inner Begin failed: %v", err)
			}

			// Attempt to commit outer while inner is active
			err = txOuter.Commit(ctx)
			if err == nil {
				t.Error("Expected error when committing parent with active child")
			}
			if !strings.Contains(err.Error(), "nested transaction still active") {
				t.Errorf("Expected 'nested transaction still active' error, got: %v", err)
			}

			// Cleanup
			txInner.Rollback(ctx)
			txOuter.Rollback(ctx)
		})
	}
}

// TestTransaction_ReverseRollback verifies that rolling back a parent transaction
// automatically rolls back all nested transactions (including children map restoration).
func TestTransaction_ReverseRollback(t *testing.T) {
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
			registry.Register(&Purchase{})
			registry.Register(&PurchaseLineItem{})
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			defer func() {
				orm1.NewRawQuery(session, "DELETE FROM purchase_line_item").Exec(ctx)
				orm1.NewRawQuery(session, "DELETE FROM purchase").Exec(ctx)
			}()

			// Begin tx1
			tx1, err := session.Begin(ctx, nil)
			if err != nil {
				t.Fatalf("tx1 Begin failed: %v", err)
			}

			purchase1 := &Purchase{
				ID:    "p1",
				Price: 100.0,
				LineItems: []*PurchaseLineItem{
					{ID: "li1", PurchaseID: "p1", Quantity: 1},
				},
			}
			session.Save(ctx, purchase1)

			// Begin tx2
			tx2, err := session.Begin(ctx, nil)
			if err != nil {
				tx1.Rollback(ctx)
				t.Fatalf("tx2 Begin failed: %v", err)
			}

			purchase2 := &Purchase{
				ID:    "p2",
				Price: 200.0,
				LineItems: []*PurchaseLineItem{
					{ID: "li2", PurchaseID: "p2", Quantity: 2},
				},
			}
			session.Save(ctx, purchase2)

			// Begin tx3
			tx3, err := session.Begin(ctx, nil)
			if err != nil {
				tx2.Rollback(ctx)
				tx1.Rollback(ctx)
				t.Fatalf("tx3 Begin failed: %v", err)
			}

			purchase3 := &Purchase{
				ID:    "p3",
				Price: 300.0,
				LineItems: []*PurchaseLineItem{
					{ID: "li3", PurchaseID: "p3", Quantity: 3},
				},
			}
			session.Save(ctx, purchase3)

			// Rollback tx1 (should rollback tx2 and tx3 as well)
			if err := tx1.Rollback(ctx); err != nil {
				t.Fatalf("tx1 Rollback failed: %v", err)
			}

			// Verify: all purchases should not be persisted (should INSERT)
			session.Save(ctx, purchase1)
			session.Save(ctx, purchase2)
			session.Save(ctx, purchase3)

			// All should be newly inserted
			var got1, got2, got3 *Purchase
			session.Get(ctx, &got1, orm1.NewKey("p1"))
			session.Get(ctx, &got2, orm1.NewKey("p2"))
			session.Get(ctx, &got3, orm1.NewKey("p3"))

			if got1 == nil || got2 == nil || got3 == nil {
				t.Error("All purchases should exist after re-save")
			}

			// Calling Rollback on tx2 and tx3 should be no-op (already rolled back by parent)
			if err := tx2.Rollback(ctx); err != nil {
				t.Errorf("tx2 Rollback should be no-op, got error: %v", err)
			}
			if err := tx3.Rollback(ctx); err != nil {
				t.Errorf("tx3 Rollback should be no-op, got error: %v", err)
			}
		})
	}
}

// TestTransaction_ParentRollbackThenChildCommit verifies that attempting to
// commit a child transaction after parent rollback returns an error.
func TestTransaction_ParentRollbackThenChildCommit(t *testing.T) {
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
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			// Begin outer transaction
			txOuter, err := session.Begin(ctx, nil)
			if err != nil {
				t.Fatalf("Outer Begin failed: %v", err)
			}

			// Begin inner transaction
			txInner, err := session.Begin(ctx, nil)
			if err != nil {
				txOuter.Rollback(ctx)
				t.Fatalf("Inner Begin failed: %v", err)
			}

			// Rollback outer (this also rolls back inner)
			if err := txOuter.Rollback(ctx); err != nil {
				t.Fatalf("Outer Rollback failed: %v", err)
			}

			// Attempt to commit inner after parent rollback
			err = txInner.Commit(ctx)
			if err == nil {
				t.Error("Expected error when committing after parent rollback")
			}
			if !strings.Contains(err.Error(), "already rolled back by parent") {
				t.Errorf("Expected 'already rolled back by parent' error, got: %v", err)
			}
		})
	}
}

// TestTransaction_HelperFunctions verifies that helper functions create
// correct TxOptions and work with Begin.
func TestTransaction_HelperFunctions(t *testing.T) {
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
			factory := orm1.NewSessionFactory(registry, drv.driver)
			session := factory.CreateSession()

			// Test ReadOnlyTxOptions
			tx1, err := session.Begin(ctx, orm1.ReadOnlyTxOptions())
			if err != nil {
				t.Fatalf("Begin with ReadOnlyTxOptions failed: %v", err)
			}
			tx1.Rollback(ctx)

			// Test SerializableTxOptions
			tx2, err := session.Begin(ctx, orm1.SerializableTxOptions())
			if err != nil {
				t.Fatalf("Begin with SerializableTxOptions failed: %v", err)
			}
			tx2.Rollback(ctx)

			// Test RepeatableReadTxOptions
			tx3, err := session.Begin(ctx, orm1.RepeatableReadTxOptions())
			if err != nil {
				t.Fatalf("Begin with RepeatableReadTxOptions failed: %v", err)
			}
			tx3.Rollback(ctx)
		})
	}
}
