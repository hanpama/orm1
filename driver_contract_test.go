package orm1

import (
	"context"
	"testing"
)

func RunDriverContracts(t *testing.T, drv Driver, schema, table string) {
	ctx := context.Background()

	t.Run("Driver", func(t *testing.T) {
		testDriverContracts(t, drv)
	})

	backend := drv.CreateBackend()

	t.Run("CRUD Operations", func(t *testing.T) {
		testCRUDContracts(t, ctx, backend, schema, table)
	})

	t.Run("Transaction Control", func(t *testing.T) {
		testTransactionContracts(t, ctx, backend, schema, table)
	})

	t.Run("Complex Queries", func(t *testing.T) {
		testComplexQueryContracts(t, ctx, backend, schema, table)
	})

	t.Run("Raw SQL", func(t *testing.T) {
		testRawSQLContracts(t, ctx, backend, schema, table)
	})

	t.Run("Rows Interface", func(t *testing.T) {
		testRowsContracts(t, ctx, backend, schema, table)
	})
}

func testDriverContracts(t *testing.T, drv Driver) {
	t.Run("CreateBackend_Independence", func(t *testing.T) {
		b1 := drv.CreateBackend()
		b2 := drv.CreateBackend()

		if b1 == nil || b2 == nil {
			t.Fatal("CreateBackend returned nil")
		}

		if b1 == b2 {
			t.Error("CreateBackend returned same instance, expected independent backends")
		}
	})

	t.Run("Close_Idempotent", func(t *testing.T) {
		t.Skip("Tested separately to avoid closing shared DB")
	})
}
