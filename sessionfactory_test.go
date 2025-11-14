package orm1_test

import (
	"testing"

	"github.com/hanpama/orm1"
	"github.com/hanpama/orm1/driver"
)

// Test RegisterEntity with invalid inputs
func TestRegisterEntityPanicOnInvalidInput(t *testing.T) {
	for _, drv := range []struct {
		name   string
		driver driver.Driver
	}{
		{"sqlite", sqliteDriver},
		{"postgres", postgresDriver},
	} {
		t.Run(drv.name, func(t *testing.T) {
			factory := orm1.NewSessionFactoryWithDriver(drv.driver)

			// Test 1: Non-pointer input
			t.Run("non-pointer", func(t *testing.T) {
				defer func() {
					if r := recover(); r == nil {
						t.Error("Expected panic when passing non-pointer to RegisterEntity")
					} else if r != "entityPtr must be a pointer to struct" {
						t.Errorf("Expected panic message 'entityPtr must be a pointer to struct', got '%v'", r)
					}
				}()
				type User struct {
					ID int64
				}
				factory.RegisterEntity(User{}) // Should panic
			})

			// Test 2: Pointer to non-struct
			t.Run("pointer-to-non-struct", func(t *testing.T) {
				defer func() {
					if r := recover(); r == nil {
						t.Error("Expected panic when passing pointer to non-struct to RegisterEntity")
					} else if r != "entityPtr must point to a struct" {
						t.Errorf("Expected panic message 'entityPtr must point to a struct', got '%v'", r)
					}
				}()
				var intVal int = 42
				factory.RegisterEntity(&intVal) // Should panic
			})

			// Test 3: Nil pointer
			t.Run("nil-pointer", func(t *testing.T) {
				defer func() {
					if r := recover(); r == nil {
						t.Error("Expected panic when passing nil to RegisterEntity")
					}
					// nil pointer causes reflect panic "reflect: call of reflect.Value.Type on zero Value"
					// which is acceptable as it still panics
				}()
				var nilPtr *SimpleAuto
				factory.RegisterEntity(nilPtr) // Should panic
			})
		})
	}
}

// Test CreateSession without driver
func TestCreateSessionPanicWithoutDriver(t *testing.T) {
	t.Run("no-driver-set", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("Expected panic when creating session without driver")
			} else {
				msg, ok := r.(string)
				if !ok || msg != "Driver not set. Use SetDriver() or NewSessionFactoryWithDriver() before creating sessions." {
					t.Errorf("Expected panic message about driver not set, got '%v'", r)
				}
			}
		}()

		factory := orm1.NewSessionFactory()
		// Don't set driver
		factory.CreateSession() // Should panic
	})
}
