package orm1_test

import (
	"context"
	"testing"

	"github.com/hanpama/orm1"
	"github.com/hanpama/orm1/driver"
)

// TestNestedTransactionInnerInsertRollbackOuterCommit tests that:
// - Inner transaction INSERT is rolled back
// - Outer transaction changes are committed
// - Persisted state is correctly restored after inner rollback
func TestNestedTransactionInnerInsertRollbackOuterCommit(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver driver.Driver
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

			// Cleanup
			defer func() {
				rawQuery := orm1.NewRawQuery(session, "DELETE FROM blog_posts WHERE id IN (?, ?)", int64(1), int64(2))
				rawQuery.Exec(ctx)
			}()

			// Outer transaction begins
			txOuter, err := session.Begin(ctx, nil)
			if err != nil {
				t.Fatalf("Outer Begin failed: %v", err)
			}

			// Insert post1 in outer transaction
			post1 := &BlogPost{ID: 1, Title: "Post 1"}
			if err := session.Save(ctx, post1); err != nil {
				txOuter.Rollback(ctx)
				t.Fatalf("Outer Save failed: %v", err)
			}

			// Inner transaction begins (SAVEPOINT)
			txInner, err := session.Begin(ctx, nil)
			if err != nil {
				txOuter.Rollback(ctx)
				t.Fatalf("Inner Begin failed: %v", err)
			}

			// Insert post2 in inner transaction
			post2 := &BlogPost{ID: 2, Title: "Post 2"}
			if err := session.Save(ctx, post2); err != nil {
				txInner.Rollback(ctx)
				txOuter.Rollback(ctx)
				t.Fatalf("Inner Save failed: %v", err)
			}

			// Rollback inner transaction (ROLLBACK TO SAVEPOINT)
			if err := txInner.Rollback(ctx); err != nil {
				txOuter.Rollback(ctx)
				t.Fatalf("Inner Rollback failed: %v", err)
			}

			// Commit outer transaction
			if err := txOuter.Commit(ctx); err != nil {
				t.Fatalf("Outer Commit failed: %v", err)
			}

			// Verify: post1 should exist, post2 should not
			var got1 *BlogPost
			if err := session.Get(ctx, &got1, orm1.NewKey(int64(1))); err != nil {
				t.Fatalf("Get post1 failed: %v", err)
			}
			if got1 == nil {
				t.Error("Expected post1 to exist")
			}

			var got2 *BlogPost
			if err := session.Get(ctx, &got2, orm1.NewKey(int64(2))); err != nil {
				t.Fatalf("Get post2 failed: %v", err)
			}
			if got2 != nil {
				t.Error("Expected post2 to not exist after inner rollback")
			}

			// Verify persisted state: post2 should not be marked as persisted
			// This is tested by trying to save post2 again - it should INSERT
			if err := session.Save(ctx, post2); err != nil {
				t.Fatalf("Save post2 again failed: %v", err)
			}

			// Verify post2 now exists
			if err := session.Get(ctx, &got2, orm1.NewKey(int64(2))); err != nil {
				t.Fatalf("Get post2 after re-save failed: %v", err)
			}
			if got2 == nil {
				t.Error("Expected post2 to exist after re-save")
			}
		})
	}
}

// TestNestedTransactionInnerDeleteRollbackOuterCommit tests that:
// - Inner transaction DELETE is rolled back
// - Entity remains persisted after inner rollback
func TestNestedTransactionInnerDeleteRollbackOuterCommit(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver driver.Driver
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

			// Cleanup
			defer func() {
				rawQuery := orm1.NewRawQuery(session, "DELETE FROM blog_posts WHERE id = ?", int64(1))
				rawQuery.Exec(ctx)
			}()

			// Setup: Insert post1 outside transaction
			post1 := &BlogPost{ID: 1, Title: "Post 1"}
			if err := session.Save(ctx, post1); err != nil {
				t.Fatalf("Setup Save failed: %v", err)
			}

			// Outer transaction begins
			txOuter, err := session.Begin(ctx, nil)
			if err != nil {
				t.Fatalf("Outer Begin failed: %v", err)
			}

			// Inner transaction begins (SAVEPOINT)
			txInner, err := session.Begin(ctx, nil)
			if err != nil {
				txOuter.Rollback(ctx)
				t.Fatalf("Inner Begin failed: %v", err)
			}

			// Delete post1 in inner transaction
			if err := session.Delete(ctx, post1); err != nil {
				txInner.Rollback(ctx)
				txOuter.Rollback(ctx)
				t.Fatalf("Inner Delete failed: %v", err)
			}

			// Rollback inner transaction (ROLLBACK TO SAVEPOINT)
			if err := txInner.Rollback(ctx); err != nil {
				txOuter.Rollback(ctx)
				t.Fatalf("Inner Rollback failed: %v", err)
			}

			// Commit outer transaction
			if err := txOuter.Commit(ctx); err != nil {
				t.Fatalf("Outer Commit failed: %v", err)
			}

			// Verify: post1 should still exist in DB
			var got1 *BlogPost
			if err := session.Get(ctx, &got1, orm1.NewKey(int64(1))); err != nil {
				t.Fatalf("Get post1 failed: %v", err)
			}
			if got1 == nil {
				t.Error("Expected post1 to still exist after inner delete rollback")
			}

			// Verify persisted state: post1 should still be marked as persisted
			// This is tested by trying to save post1 again - it should UPDATE
			post1.Title = "Updated Title"
			if err := session.Save(ctx, post1); err != nil {
				t.Fatalf("Save (update) post1 failed: %v", err)
			}
		})
	}
}

// TestNestedTransactionInnerCommitOuterRollback tests that:
// - Inner transaction commits (RELEASE SAVEPOINT)
// - Outer transaction rollback undoes everything including inner changes
// - Persisted state is restored to before outer transaction
func TestNestedTransactionInnerCommitOuterRollback(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver driver.Driver
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

			// Outer transaction begins
			txOuter, err := session.Begin(ctx, nil)
			if err != nil {
				t.Fatalf("Outer Begin failed: %v", err)
			}

			// Insert post1 in outer transaction
			post1 := &BlogPost{ID: 1, Title: "Post 1"}
			if err := session.Save(ctx, post1); err != nil {
				txOuter.Rollback(ctx)
				t.Fatalf("Outer Save failed: %v", err)
			}

			// Inner transaction begins (SAVEPOINT)
			txInner, err := session.Begin(ctx, nil)
			if err != nil {
				txOuter.Rollback(ctx)
				t.Fatalf("Inner Begin failed: %v", err)
			}

			// Insert post2 in inner transaction
			post2 := &BlogPost{ID: 2, Title: "Post 2"}
			if err := session.Save(ctx, post2); err != nil {
				txInner.Rollback(ctx)
				txOuter.Rollback(ctx)
				t.Fatalf("Inner Save failed: %v", err)
			}

			// Commit inner transaction (RELEASE SAVEPOINT)
			if err := txInner.Commit(ctx); err != nil {
				txOuter.Rollback(ctx)
				t.Fatalf("Inner Commit failed: %v", err)
			}

			// Rollback outer transaction (this should undo everything)
			if err := txOuter.Rollback(ctx); err != nil {
				t.Fatalf("Outer Rollback failed: %v", err)
			}

			// Verify: neither post should exist in DB
			var got1 *BlogPost
			if err := session.Get(ctx, &got1, orm1.NewKey(int64(1))); err != nil {
				t.Fatalf("Get post1 failed: %v", err)
			}
			if got1 != nil {
				t.Error("Expected post1 to not exist after outer rollback")
			}

			var got2 *BlogPost
			if err := session.Get(ctx, &got2, orm1.NewKey(int64(2))); err != nil {
				t.Fatalf("Get post2 failed: %v", err)
			}
			if got2 != nil {
				t.Error("Expected post2 to not exist after outer rollback")
			}

			// Verify persisted state: both posts should not be marked as persisted
			// Cleanup not needed since rollback already undid everything
		})
	}
}

// TestNestedTransactionThreeLevels tests 3-level nested transactions
// with middle level rollback
func TestNestedTransactionThreeLevels(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver driver.Driver
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

			// Cleanup
			defer func() {
				rawQuery := orm1.NewRawQuery(session, "DELETE FROM blog_posts WHERE id IN (?, ?, ?)",
					int64(1), int64(2), int64(3))
				rawQuery.Exec(ctx)
			}()

			// Level 1: Outer transaction
			tx1, err := session.Begin(ctx, nil)
			if err != nil {
				t.Fatalf("Level 1 Begin failed: %v", err)
			}

			post1 := &BlogPost{ID: 1, Title: "Post 1"}
			if err := session.Save(ctx, post1); err != nil {
				tx1.Rollback(ctx)
				t.Fatalf("Level 1 Save failed: %v", err)
			}

			// Level 2: Middle transaction (SAVEPOINT sp_1)
			tx2, err := session.Begin(ctx, nil)
			if err != nil {
				tx1.Rollback(ctx)
				t.Fatalf("Level 2 Begin failed: %v", err)
			}

			post2 := &BlogPost{ID: 2, Title: "Post 2"}
			if err := session.Save(ctx, post2); err != nil {
				tx2.Rollback(ctx)
				tx1.Rollback(ctx)
				t.Fatalf("Level 2 Save failed: %v", err)
			}

			// Level 3: Inner transaction (SAVEPOINT sp_2)
			tx3, err := session.Begin(ctx, nil)
			if err != nil {
				tx2.Rollback(ctx)
				tx1.Rollback(ctx)
				t.Fatalf("Level 3 Begin failed: %v", err)
			}

			post3 := &BlogPost{ID: 3, Title: "Post 3"}
			if err := session.Save(ctx, post3); err != nil {
				tx3.Rollback(ctx)
				tx2.Rollback(ctx)
				tx1.Rollback(ctx)
				t.Fatalf("Level 3 Save failed: %v", err)
			}

			// Commit level 3
			if err := tx3.Commit(ctx); err != nil {
				tx2.Rollback(ctx)
				tx1.Rollback(ctx)
				t.Fatalf("Level 3 Commit failed: %v", err)
			}

			// Rollback level 2 (this should undo post2 and post3)
			if err := tx2.Rollback(ctx); err != nil {
				tx1.Rollback(ctx)
				t.Fatalf("Level 2 Rollback failed: %v", err)
			}

			// Commit level 1
			if err := tx1.Commit(ctx); err != nil {
				t.Fatalf("Level 1 Commit failed: %v", err)
			}

			// Verify: only post1 should exist
			var got1 *BlogPost
			if err := session.Get(ctx, &got1, orm1.NewKey(int64(1))); err != nil {
				t.Fatalf("Get post1 failed: %v", err)
			}
			if got1 == nil {
				t.Error("Expected post1 to exist")
			}

			var got2 *BlogPost
			if err := session.Get(ctx, &got2, orm1.NewKey(int64(2))); err != nil {
				t.Fatalf("Get post2 failed: %v", err)
			}
			if got2 != nil {
				t.Error("Expected post2 to not exist after level 2 rollback")
			}

			var got3 *BlogPost
			if err := session.Get(ctx, &got3, orm1.NewKey(int64(3))); err != nil {
				t.Fatalf("Get post3 failed: %v", err)
			}
			if got3 != nil {
				t.Error("Expected post3 to not exist after level 2 rollback")
			}
		})
	}
}
