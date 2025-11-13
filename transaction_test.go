package orm1_test

import (
	"context"
	"testing"

	"github.com/hanpama/orm1"
	"github.com/hanpama/orm1/driver"
)

func TestTransactionInsertCommitGet(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			factory := orm1.NewSessionFactoryWithDriver(drv.driver)
			factory.RegisterEntity(&BlogPost{}, orm1.WithTable("blog_posts"))
			session := factory.CreateSession()

			// Cleanup
			defer func() {
				rawQuery := orm1.NewRawQuery(session, "DELETE FROM blog_posts WHERE id = ?", int64(1))
				rawQuery.Exec(ctx)
			}()

			blogPost := &BlogPost{
				ID:    1,
				Title: "First post",
			}

			// Begin transaction
			tx, err := session.Begin(ctx, nil); if err != nil {
				t.Fatalf("Begin failed: %v", err)
			}

			// Save entity
			if err := session.Save(ctx, blogPost); err != nil {
				tx.Rollback(ctx)
				t.Fatalf("Save failed: %v", err)
			}

			// Commit transaction
			if err := tx.Commit(ctx); err != nil {
				t.Fatalf("Commit failed: %v", err)
			}

			// Verify: Get the entity
			var got *BlogPost
			if err := session.Get(ctx, &got, orm1.NewKey(int64(1))); err != nil {
				t.Fatalf("Get failed: %v", err)
			}

			if got == nil {
				t.Fatal("Expected entity, got nil")
			}
			if got.ID != 1 {
				t.Errorf("Expected ID 1, got %d", got.ID)
			}
			if got.Title != "First post" {
				t.Errorf("Expected title 'First post', got %s", got.Title)
			}
		})
	}
}

func TestTransactionInsertCommitSave(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			factory := orm1.NewSessionFactoryWithDriver(drv.driver)
			factory.RegisterEntity(&BlogPost{}, orm1.WithTable("blog_posts"))
			session := factory.CreateSession()

			// Cleanup
			defer func() {
				rawQuery := orm1.NewRawQuery(session, "DELETE FROM blog_posts WHERE id = ?", int64(1))
				rawQuery.Exec(ctx)
			}()

			blogPost := &BlogPost{
				ID:    1,
				Title: "First post",
			}

			// Begin transaction
			tx, err := session.Begin(ctx, nil); if err != nil {
				t.Fatalf("Begin failed: %v", err)
			}

			// Save entity
			if err := session.Save(ctx, blogPost); err != nil {
				tx.Rollback(ctx)
				t.Fatalf("Save failed: %v", err)
			}

			// Commit transaction
			if err := tx.Commit(ctx); err != nil {
				t.Fatalf("Commit failed: %v", err)
			}

			// Verify: Save again (should UPDATE)
			blogPost.Title = "Updated title"
			if err := session.Save(ctx, blogPost); err != nil {
				t.Fatalf("Second Save failed: %v", err)
			}
		})
	}
}

func TestTransactionInsertCommitDelete(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			factory := orm1.NewSessionFactoryWithDriver(drv.driver)
			factory.RegisterEntity(&BlogPost{}, orm1.WithTable("blog_posts"))
			session := factory.CreateSession()

			blogPost := &BlogPost{
				ID:    1,
				Title: "First post",
			}

			// Begin transaction
			tx, err := session.Begin(ctx, nil); if err != nil {
				t.Fatalf("Begin failed: %v", err)
			}

			// Save entity
			if err := session.Save(ctx, blogPost); err != nil {
				tx.Rollback(ctx)
				t.Fatalf("Save failed: %v", err)
			}

			// Commit transaction
			if err := tx.Commit(ctx); err != nil {
				t.Fatalf("Commit failed: %v", err)
			}

			// Verify: Delete should work
			if err := session.Delete(ctx, blogPost); err != nil {
				t.Fatalf("Delete failed: %v", err)
			}
		})
	}
}

func TestTransactionInsertRollbackGet(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			factory := orm1.NewSessionFactoryWithDriver(drv.driver)
			factory.RegisterEntity(&BlogPost{}, orm1.WithTable("blog_posts"))
			session := factory.CreateSession()

			blogPost := &BlogPost{
				ID:    1,
				Title: "First post",
			}

			// Begin transaction
			tx, err := session.Begin(ctx, nil); if err != nil {
				t.Fatalf("Begin failed: %v", err)
			}

			// Save entity
			if err := session.Save(ctx, blogPost); err != nil {
				tx.Rollback(ctx)
				t.Fatalf("Save failed: %v", err)
			}

			// Rollback transaction
			if err := tx.Rollback(ctx); err != nil {
				t.Fatalf("Rollback failed: %v", err)
			}

			// Verify: Entity should not exist
			var got *BlogPost
			if err := session.Get(ctx, &got, orm1.NewKey(int64(1))); err != nil {
				t.Fatalf("Get failed: %v", err)
			}

			if got != nil {
				t.Errorf("Expected nil after rollback, got %+v", got)
			}
		})
	}
}

func TestTransactionInsertRollbackSave(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			factory := orm1.NewSessionFactoryWithDriver(drv.driver)
			factory.RegisterEntity(&BlogPost{}, orm1.WithTable("blog_posts"))
			session := factory.CreateSession()

			// Cleanup
			defer func() {
				rawQuery := orm1.NewRawQuery(session, "DELETE FROM blog_posts WHERE id = ?", int64(1))
				rawQuery.Exec(ctx)
			}()

			blogPost := &BlogPost{
				ID:    1,
				Title: "First post",
			}

			// Begin transaction
			tx, err := session.Begin(ctx, nil); if err != nil {
				t.Fatalf("Begin failed: %v", err)
			}

			// Save entity
			if err := session.Save(ctx, blogPost); err != nil {
				tx.Rollback(ctx)
				t.Fatalf("Save failed: %v", err)
			}

			// Rollback transaction
			if err := tx.Rollback(ctx); err != nil {
				t.Fatalf("Rollback failed: %v", err)
			}

			// Verify: Save should work (INSERT, not UPDATE)
			// After rollback, the entity should not be marked as persisted
			if err := session.Save(ctx, blogPost); err != nil {
				t.Fatalf("Save after rollback failed: %v", err)
			}
		})
	}
}

func TestTransactionInsertRollbackDelete(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			factory := orm1.NewSessionFactoryWithDriver(drv.driver)
			factory.RegisterEntity(&BlogPost{}, orm1.WithTable("blog_posts"))
			session := factory.CreateSession()

			blogPost := &BlogPost{
				ID:    1,
				Title: "First post",
			}

			// Begin transaction
			tx, err := session.Begin(ctx, nil); if err != nil {
				t.Fatalf("Begin failed: %v", err)
			}

			// Save entity
			if err := session.Save(ctx, blogPost); err != nil {
				tx.Rollback(ctx)
				t.Fatalf("Save failed: %v", err)
			}

			// Rollback transaction
			if err := tx.Rollback(ctx); err != nil {
				t.Fatalf("Rollback failed: %v", err)
			}

			// Verify: Delete should be no-op (entity not persisted after rollback)
			// This should not cause an error
			if err := session.Delete(ctx, blogPost); err != nil {
				t.Fatalf("Delete after rollback failed: %v", err)
			}
		})
	}
}

func TestTransactionDeleteCommitGet(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			factory := orm1.NewSessionFactoryWithDriver(drv.driver)
			factory.RegisterEntity(&BlogPost{}, orm1.WithTable("blog_posts"))
			session := factory.CreateSession()

			blogPost := &BlogPost{
				ID:    1,
				Title: "First post",
			}

			// Insert entity first (outside transaction)
			if err := session.Save(ctx, blogPost); err != nil {
				t.Fatalf("Initial Save failed: %v", err)
			}

			// Begin transaction
			tx, err := session.Begin(ctx, nil); if err != nil {
				t.Fatalf("Begin failed: %v", err)
			}

			// Delete entity
			if err := session.Delete(ctx, blogPost); err != nil {
				tx.Rollback(ctx)
				t.Fatalf("Delete failed: %v", err)
			}

			// Commit transaction
			if err := tx.Commit(ctx); err != nil {
				t.Fatalf("Commit failed: %v", err)
			}

			// Verify: Entity should not exist
			var got *BlogPost
			if err := session.Get(ctx, &got, orm1.NewKey(int64(1))); err != nil {
				t.Fatalf("Get failed: %v", err)
			}

			if got != nil {
				t.Errorf("Expected nil after delete, got %+v", got)
			}
		})
	}
}

func TestTransactionDeleteCommitSave(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			factory := orm1.NewSessionFactoryWithDriver(drv.driver)
			factory.RegisterEntity(&BlogPost{}, orm1.WithTable("blog_posts"))
			session := factory.CreateSession()

			// Cleanup
			defer func() {
				rawQuery := orm1.NewRawQuery(session, "DELETE FROM blog_posts WHERE id = ?", int64(1))
				rawQuery.Exec(ctx)
			}()

			blogPost := &BlogPost{
				ID:    1,
				Title: "First post",
			}

			// Insert entity first (outside transaction)
			if err := session.Save(ctx, blogPost); err != nil {
				t.Fatalf("Initial Save failed: %v", err)
			}

			// Begin transaction
			tx, err := session.Begin(ctx, nil); if err != nil {
				t.Fatalf("Begin failed: %v", err)
			}

			// Delete entity
			if err := session.Delete(ctx, blogPost); err != nil {
				tx.Rollback(ctx)
				t.Fatalf("Delete failed: %v", err)
			}

			// Commit transaction
			if err := tx.Commit(ctx); err != nil {
				t.Fatalf("Commit failed: %v", err)
			}

			// Verify: Save should work (INSERT)
			if err := session.Save(ctx, blogPost); err != nil {
				t.Fatalf("Save after delete failed: %v", err)
			}
		})
	}
}

func TestTransactionDeleteRollbackGet(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			factory := orm1.NewSessionFactoryWithDriver(drv.driver)
			factory.RegisterEntity(&BlogPost{}, orm1.WithTable("blog_posts"))
			session := factory.CreateSession()

			// Cleanup
			defer func() {
				rawQuery := orm1.NewRawQuery(session, "DELETE FROM blog_posts WHERE id = ?", int64(1))
				rawQuery.Exec(ctx)
			}()

			blogPost := &BlogPost{
				ID:    1,
				Title: "First post",
			}

			// Insert entity first (outside transaction)
			if err := session.Save(ctx, blogPost); err != nil {
				t.Fatalf("Initial Save failed: %v", err)
			}

			// Begin transaction
			tx, err := session.Begin(ctx, nil); if err != nil {
				t.Fatalf("Begin failed: %v", err)
			}

			// Delete entity
			if err := session.Delete(ctx, blogPost); err != nil {
				tx.Rollback(ctx)
				t.Fatalf("Delete failed: %v", err)
			}

			// Rollback transaction
			if err := tx.Rollback(ctx); err != nil {
				t.Fatalf("Rollback failed: %v", err)
			}

			// Verify: Entity should still exist after rollback
			var got *BlogPost
			if err := session.Get(ctx, &got, orm1.NewKey(int64(1))); err != nil {
				t.Fatalf("Get failed: %v", err)
			}

			if got == nil {
				t.Fatal("Expected entity to exist after rollback, got nil")
			}
			if got.ID != 1 {
				t.Errorf("Expected ID 1, got %d", got.ID)
			}
		})
	}
}

func TestTransactionDeleteRollbackSave(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			ctx := context.Background()
			factory := orm1.NewSessionFactoryWithDriver(drv.driver)
			factory.RegisterEntity(&BlogPost{}, orm1.WithTable("blog_posts"))
			session := factory.CreateSession()

			// Cleanup
			defer func() {
				rawQuery := orm1.NewRawQuery(session, "DELETE FROM blog_posts WHERE id = ?", int64(1))
				rawQuery.Exec(ctx)
			}()

			blogPost := &BlogPost{
				ID:    1,
				Title: "First post",
			}

			// Insert entity first (outside transaction)
			if err := session.Save(ctx, blogPost); err != nil {
				t.Fatalf("Initial Save failed: %v", err)
			}

			// Begin transaction
			tx, err := session.Begin(ctx, nil); if err != nil {
				t.Fatalf("Begin failed: %v", err)
			}

			// Delete entity
			if err := session.Delete(ctx, blogPost); err != nil {
				tx.Rollback(ctx)
				t.Fatalf("Delete failed: %v", err)
			}

			// Rollback transaction
			if err := tx.Rollback(ctx); err != nil {
				t.Fatalf("Rollback failed: %v", err)
			}

			// Verify: Save should work (UPDATE, entity still persisted after rollback)
			blogPost.Title = "Updated title"
			if err := session.Save(ctx, blogPost); err != nil {
				t.Fatalf("Save after rollback failed: %v", err)
			}
		})
	}
}
