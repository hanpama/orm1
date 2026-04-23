package orm1

import (
	"context"
	"testing"
)

func testRowsContracts(t *testing.T, ctx context.Context, backend Backend, schema, table string) {
	// Insert test data
	insertOp := InsertOp{
		IntoSchema: schema,
		IntoTable:  table,
		Insert:     []string{"name", "value"},
		Returning:  []string{"id"},
		Values: [][]any{
			{"rows_test1", 11},
			{"rows_test2", 22},
			{"rows_test3", 33},
		},
	}
	rows, err := backend.Insert(ctx, insertOp)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	var ids []int64
	for rows.Next() {
		var id int64
		rows.Scan(&id)
		ids = append(ids, id)
	}
	rows.Close()

	t.Run("Next_Advancement", func(t *testing.T) {
		testNextAdvancement(t, ctx, backend, schema, table, ids)
	})

	t.Run("Close_Idempotent", func(t *testing.T) {
		testCloseIdempotent(t, ctx, backend, schema, table, ids)
	})
}

func testNextAdvancement(t *testing.T, ctx context.Context, backend Backend, schema, table string, ids []int64) {
	// Contract: Next advances through rows: BeforeFirst -> OnRow -> AfterLast
	selectOp := SelectOp{
		Select:     []string{"id"},
		FromSchema: schema,
		FromTable:  table,
		KeyColumns: []string{"id"},
		Keys:       []Key{New1(ids[0]), New1(ids[1]), New1(ids[2])},
	}

	rows, err := backend.Select(ctx, selectOp)
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}
	defer rows.Close()

	// First Next: BeforeFirst -> OnRow (row 1)
	if !rows.Next() {
		t.Error("First Next returned false, expected true")
	}

	// Second Next: OnRow (row 1) -> OnRow (row 2)
	if !rows.Next() {
		t.Error("Second Next returned false, expected true")
	}

	// Third Next: OnRow (row 2) -> OnRow (row 3)
	if !rows.Next() {
		t.Error("Third Next returned false, expected true")
	}

	// Fourth Next: OnRow (row 3) -> AfterLast
	if rows.Next() {
		t.Error("Fourth Next returned true, expected false (AfterLast)")
	}

	// Fifth Next: AfterLast -> AfterLast (stays)
	if rows.Next() {
		t.Error("Fifth Next returned true, expected false (stay in AfterLast)")
	}
}

func testCloseIdempotent(t *testing.T, ctx context.Context, backend Backend, schema, table string, ids []int64) {
	// Contract: Close is idempotent
	selectOp := SelectOp{
		Select:     []string{"id"},
		FromSchema: schema,
		FromTable:  table,
		KeyColumns: []string{"id"},
		Keys:       []Key{New1(ids[0])},
	}

	rows, err := backend.Select(ctx, selectOp)
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}

	if err := rows.Close(); err != nil {
		t.Errorf("First Close failed: %v", err)
	}

	if err := rows.Close(); err != nil {
		t.Errorf("Second Close failed (not idempotent): %v", err)
	}
}
