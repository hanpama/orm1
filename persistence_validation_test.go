package orm1_test

import (
	"context"
	"strings"
	"testing"

	"github.com/hanpama/orm1"
	"github.com/hanpama/orm1/driver"
)

// Test Get with invalid dest parameter
func TestGetErrorOnInvalidDest(t *testing.T) {
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
			factory.RegisterEntity(&SimpleAuto{}, orm1.WithTable("simple_auto"))
			session := factory.CreateSession()

			// Test 1: Non-pointer dest
			t.Run("non-pointer-dest", func(t *testing.T) {
				var entity SimpleAuto
				err := session.Get(ctx, entity, orm1.NewKey(int64(1)))
				if err == nil {
					t.Error("Expected error when passing non-pointer dest to Get")
				}
				if !strings.Contains(err.Error(), "must be a pointer") {
					t.Errorf("Expected error about pointer, got: %v", err)
				}
			})

			// Test 2: Pointer to struct (not pointer to pointer)
			t.Run("pointer-to-struct-not-double-pointer", func(t *testing.T) {
				var entity SimpleAuto
				err := session.Get(ctx, &entity, orm1.NewKey(int64(1)))
				if err == nil {
					t.Error("Expected error when passing &entity (not &&entity) to Get")
				}
				if !strings.Contains(err.Error(), "must be a pointer to entity pointer") {
					t.Errorf("Expected error about double pointer, got: %v", err)
				}
			})

			// Test 3: Pointer to pointer to non-struct
			t.Run("pointer-to-pointer-to-non-struct", func(t *testing.T) {
				var intVal int = 42
				var intPtr *int = &intVal
				err := session.Get(ctx, &intPtr, orm1.NewKey(int64(1)))
				if err == nil {
					t.Error("Expected error when passing **int to Get")
				}
				if !strings.Contains(err.Error(), "must be a struct") {
					t.Errorf("Expected error about struct, got: %v", err)
				}
			})

			// Test 4: Unregistered entity type
			t.Run("unregistered-entity", func(t *testing.T) {
				type UnregisteredEntity struct {
					ID int64
				}
				var entity *UnregisteredEntity
				err := session.Get(ctx, &entity, orm1.NewKey(int64(1)))
				if err == nil {
					t.Error("Expected error when getting unregistered entity type")
				}
				if !strings.Contains(err.Error(), "no mapping found") {
					t.Errorf("Expected error about no mapping found, got: %v", err)
				}
			})
		})
	}
}

// Test Save with invalid entity parameter
func TestSaveErrorOnInvalidEntity(t *testing.T) {
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
			factory.RegisterEntity(&SimpleAuto{}, orm1.WithTable("simple_auto"))
			session := factory.CreateSession()

			// Test 1: Non-pointer entity
			t.Run("non-pointer-entity", func(t *testing.T) {
				entity := SimpleAuto{Name: "test"}
				err := session.Save(ctx, entity)
				if err == nil {
					t.Error("Expected error when passing non-pointer entity to Save")
				}
				if !strings.Contains(err.Error(), "must be a pointer") {
					t.Errorf("Expected error about pointer, got: %v", err)
				}
			})

			// Test 2: Pointer to non-struct
			t.Run("pointer-to-non-struct", func(t *testing.T) {
				var intVal int = 42
				err := session.Save(ctx, &intVal)
				if err == nil {
					t.Error("Expected error when passing pointer to non-struct to Save")
				}
				if !strings.Contains(err.Error(), "pointer to struct") {
					t.Errorf("Expected error about pointer to struct, got: %v", err)
				}
			})

			// Test 3: Unregistered entity type
			t.Run("unregistered-entity", func(t *testing.T) {
				type UnregisteredEntity struct {
					ID int64
				}
				entity := &UnregisteredEntity{ID: 1}
				err := session.Save(ctx, entity)
				if err == nil {
					t.Error("Expected error when saving unregistered entity type")
				}
				if !strings.Contains(err.Error(), "no mapping found") {
					t.Errorf("Expected error about no mapping found, got: %v", err)
				}
			})
		})
	}
}

// Test Delete with invalid entity parameter
func TestDeleteErrorOnInvalidEntity(t *testing.T) {
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
			factory.RegisterEntity(&SimpleAuto{}, orm1.WithTable("simple_auto"))
			session := factory.CreateSession()

			// Test 1: Non-pointer entity
			t.Run("non-pointer-entity", func(t *testing.T) {
				entity := SimpleAuto{ID: 1, Name: "test"}
				err := session.Delete(ctx, entity)
				if err == nil {
					t.Error("Expected error when passing non-pointer entity to Delete")
				}
				if !strings.Contains(err.Error(), "must be a pointer") {
					t.Errorf("Expected error about pointer, got: %v", err)
				}
			})

			// Test 2: Pointer to non-struct
			t.Run("pointer-to-non-struct", func(t *testing.T) {
				var intVal int = 42
				err := session.Delete(ctx, &intVal)
				if err == nil {
					t.Error("Expected error when passing pointer to non-struct to Delete")
				}
				if !strings.Contains(err.Error(), "pointer to struct") {
					t.Errorf("Expected error about pointer to struct, got: %v", err)
				}
			})

			// Test 3: Unregistered entity type
			t.Run("unregistered-entity", func(t *testing.T) {
				type UnregisteredEntity struct {
					ID int64
				}
				entity := &UnregisteredEntity{ID: 1}
				err := session.Delete(ctx, entity)
				if err == nil {
					t.Error("Expected error when deleting unregistered entity type")
				}
				if !strings.Contains(err.Error(), "no mapping found") {
					t.Errorf("Expected error about no mapping found, got: %v", err)
				}
			})
		})
	}
}
