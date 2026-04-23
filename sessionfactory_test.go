//go:build e2e
// +build e2e

package orm1_test

import (
	"testing"

	"github.com/hanpama/orm1"
)

// Test Registry.Register with invalid inputs
func TestRegistryRegisterPanicOnInvalidInput(t *testing.T) {
	// Test 1: Non-pointer input
	t.Run("non-pointer", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic when passing non-pointer to Register")
			} else if r != "entityPtr must be a pointer to struct" {
				t.Errorf("Expected panic message 'entityPtr must be a pointer to struct', got '%v'", r)
			}
		}()
		type User struct {
			ID int64
		}
		registry := orm1.NewRegistry()
		registry.Register(User{}) // Should panic
	})

	// Test 2: Pointer to non-struct
	t.Run("pointer-to-non-struct", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic when passing pointer to non-struct to Register")
			} else if r != "entityPtr must point to a struct" {
				t.Errorf("Expected panic message 'entityPtr must point to a struct', got '%v'", r)
			}
		}()
		var intVal int = 42
		registry := orm1.NewRegistry()
		registry.Register(&intVal) // Should panic
	})

	// Test 3: Nil pointer
	t.Run("nil-pointer", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic when passing nil to Register")
			}
			// nil pointer causes reflect panic
			// which is acceptable as it still panics
		}()
		type SimpleAuto struct {
			ID int64
		}
		var nilPtr *SimpleAuto
		registry := orm1.NewRegistry()
		registry.Register(nilPtr) // Should panic
	})
}

// Test CreateSession with explicit registry and driver
func TestCreateSessionWithRegistryAndDriver(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver orm1.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			registry := orm1.NewRegistry()
			factory := orm1.NewSessionFactory(registry, drv.driver)

			session := factory.CreateSession()
			if session == nil {
				t.Error("Expected non-nil session")
			}
		})
	}
}
