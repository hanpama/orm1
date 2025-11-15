package driver_test

import (
	"context"
	"testing"

	"github.com/hanpama/orm1/driver"
	"github.com/hanpama/orm1/key"
)

func testCRUDContracts(t *testing.T, ctx context.Context, backend driver.Backend, schema, table string) {
	insertOp := driver.InsertOp{
		IntoSchema: schema,
		IntoTable:  table,
		Insert:     []string{"name", "value"},
		Returning:  []string{"id"},
		Values: [][]any{
			{"test1", 100},
			{"test2", 200},
			{"test3", 300},
		},
	}

	rows, err := backend.Insert(ctx, insertOp)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		ids = append(ids, id)
	}
	rows.Close()

	if len(ids) != 3 {
		t.Fatalf("Expected 3 IDs, got %d", len(ids))
	}

	t.Run("Select_ReturnsNonNilRows", func(t *testing.T) {
		testSelectReturnsNonNilRows(t, ctx, backend, schema, table, ids)
	})

	t.Run("Insert_ReturningCorrespondence", func(t *testing.T) {
		testInsertReturningCorrespondence(t, ctx, backend, schema, table)
	})

	t.Run("Update_Basic", func(t *testing.T) {
		testUpdateBasic(t, ctx, backend, schema, table, ids)
	})

	t.Run("Update_NonExistentSilent", func(t *testing.T) {
		testUpdateNonExistentSilent(t, ctx, backend, schema, table)
	})

	t.Run("Update_MultipleColumns", func(t *testing.T) {
		testUpdateMultipleColumns(t, ctx, backend, schema, table, ids)
	})

	t.Run("Delete_AccurateCount", func(t *testing.T) {
		testDeleteAccurateCount(t, ctx, backend, schema, table)
	})

	t.Run("Delete_NonExistentCount", func(t *testing.T) {
		testDeleteNonExistentCount(t, ctx, backend, schema, table)
	})

	t.Run("Select_CompositeKey", func(t *testing.T) {
		testSelectCompositeKey(t, ctx, backend, schema, table, ids)
	})

	t.Run("Update_CompositeKey", func(t *testing.T) {
		testUpdateCompositeKey(t, ctx, backend, schema, table, ids)
	})

	t.Run("Delete_CompositeKey", func(t *testing.T) {
		testDeleteCompositeKey(t, ctx, backend, schema, table, ids)
	})

	t.Run("Select_EmptyKeys", func(t *testing.T) {
		testSelectEmptyKeys(t, ctx, backend, schema, table)
	})

	t.Run("Insert_EmptyValues", func(t *testing.T) {
		testInsertEmptyValues(t, ctx, backend, schema, table)
	})

	t.Run("Update_EmptyValues", func(t *testing.T) {
		testUpdateEmptyValues(t, ctx, backend, schema, table)
	})

	t.Run("Delete_EmptyKeys", func(t *testing.T) {
		testDeleteEmptyKeys(t, ctx, backend, schema, table)
	})
}

func testSelectReturnsNonNilRows(t *testing.T, ctx context.Context, backend driver.Backend, schema, table string, ids []int64) {
	selectOp := driver.SelectOp{
		Select:     []string{"id", "name"},
		FromSchema: schema,
		FromTable:  table,
		KeyColumns: []string{"id"},
		Keys: []key.Key{
			key.New1(ids[0]),
			key.New1(ids[1]),
			key.New1(ids[2]),
		},
	}

	rows, err := backend.Select(ctx, selectOp)
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}
	defer rows.Close()

	if rows == nil {
		t.Fatal("Contract violation: Select returned nil Rows on success")
	}

	count := 0
	for rows.Next() {
		var id int64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		count++
	}

	if count != 3 {
		t.Errorf("Expected 3 rows, got %d", count)
	}
}

func testInsertReturningCorrespondence(t *testing.T, ctx context.Context, backend driver.Backend, schema, table string) {
	values := [][]any{
		{"insert_test1", 1001},
		{"insert_test2", 1002},
	}

	insertOp := driver.InsertOp{
		IntoSchema: schema,
		IntoTable:  table,
		Insert:     []string{"name", "value"},
		Returning:  []string{"name", "value"},
		Values:     values,
	}

	rows, err := backend.Insert(ctx, insertOp)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	defer rows.Close()

	i := 0
	for rows.Next() {
		var name string
		var value int
		if err := rows.Scan(&name, &value); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		if name != values[i][0].(string) || value != values[i][1].(int) {
			t.Errorf("Row %d: expected (%v, %v), got (%v, %v)",
				i, values[i][0], values[i][1], name, value)
		}
		i++
	}

	if i != len(values) {
		t.Errorf("Expected %d rows, got %d", len(values), i)
	}
}

func testUpdateBasic(t *testing.T, ctx context.Context, backend driver.Backend, schema, table string, ids []int64) {
	updateOp := driver.UpdateOp{
		Schema: schema,
		Table:  table,
		Sets:   []string{"name"},
		Where:  []string{"id"},
		SetValues: [][]any{
			{"updated1"},
			{"updated2"},
		},
		WhereValues: [][]any{
			{ids[0]},
			{ids[1]},
		},
	}

	err := backend.Update(ctx, updateOp)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	selectOp := driver.SelectOp{
		Select:     []string{"name"},
		FromSchema: schema,
		FromTable:  table,
		KeyColumns: []string{"id"},
		Keys:       []key.Key{key.New1(ids[0]), key.New1(ids[1])},
	}

	rows, err := backend.Select(ctx, selectOp)
	if err != nil {
		t.Fatalf("Select after update failed: %v", err)
	}
	defer rows.Close()

	foundUpdates := 0
	for rows.Next() {
		var name string
		rows.Scan(&name)
		if name == "updated1" || name == "updated2" {
			foundUpdates++
		}
	}

	if foundUpdates != 2 {
		t.Errorf("Expected 2 updated rows, found %d", foundUpdates)
	}
}

func testUpdateNonExistentSilent(t *testing.T, ctx context.Context, backend driver.Backend, schema, table string) {
	updateOp := driver.UpdateOp{
		Schema: schema,
		Table:  table,
		Sets:   []string{"name"},
		Where:  []string{"id"},
		SetValues: [][]any{
			{"nonexistent"},
		},
		WhereValues: [][]any{
			{999999},
		},
	}

	err := backend.Update(ctx, updateOp)
	if err != nil {
		t.Errorf("Update non-existent entity failed, should be silent: %v", err)
	}
}

func testUpdateMultipleColumns(t *testing.T, ctx context.Context, backend driver.Backend, schema, table string, ids []int64) {
	// Contract: Update handles multiple SET columns (tests comma separators)
	updateOp := driver.UpdateOp{
		Schema: schema,
		Table:  table,
		Sets:   []string{"name", "value"}, // Update BOTH columns
		Where:  []string{"id"},
		SetValues: [][]any{
			{"multi_col1", 111},
			{"multi_col2", 222},
		},
		WhereValues: [][]any{
			{ids[0]},
			{ids[1]},
		},
	}

	err := backend.Update(ctx, updateOp)
	if err != nil {
		t.Fatalf("Update with multiple columns failed: %v", err)
	}
}

func testDeleteAccurateCount(t *testing.T, ctx context.Context, backend driver.Backend, schema, table string) {
	insertOp := driver.InsertOp{
		IntoSchema: schema,
		IntoTable:  table,
		Insert:     []string{"name", "value"},
		Returning:  []string{"id"},
		Values: [][]any{
			{"delete_test1", 10},
			{"delete_test2", 20},
		},
	}

	rows, err := backend.Insert(ctx, insertOp)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	var deleteIDs []int64
	for rows.Next() {
		var id int64
		rows.Scan(&id)
		deleteIDs = append(deleteIDs, id)
	}
	rows.Close()

	deleteOp := driver.DeleteOp{
		FromSchema: schema,
		FromTable:  table,
		KeyColumns: []string{"id"},
		Keys: []key.Key{
			key.New1(deleteIDs[0]),
			key.New1(deleteIDs[1]),
		},
	}

	err = backend.Delete(ctx, deleteOp)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
}

func testDeleteNonExistentCount(t *testing.T, ctx context.Context, backend driver.Backend, schema, table string) {
	// Contract: Delete succeeds even for non-existent rows
	deleteOp := driver.DeleteOp{
		FromSchema: schema,
		FromTable:  table,
		KeyColumns: []string{"id"},
		Keys: []key.Key{
			key.New1(999998),
			key.New1(999999),
		},
	}

	err := backend.Delete(ctx, deleteOp)
	if err != nil {
		t.Fatalf("Delete of non-existent rows should succeed, got error: %v", err)
	}
}

func testSelectCompositeKey(t *testing.T, ctx context.Context, backend driver.Backend, schema, table string, ids []int64) {
	// Contract: Select handles composite keys (multiple key columns)
	// Insert test data
	insertOp := driver.InsertOp{
		IntoSchema: schema,
		IntoTable:  table,
		Insert:     []string{"name", "value"},
		Returning:  []string{"id"},
		Values:     [][]any{{"comp_select1", 100}, {"comp_select2", 200}},
	}
	insertRows, err := backend.Insert(ctx, insertOp)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	var testIDs []int64
	for insertRows.Next() {
		var id int64
		insertRows.Scan(&id)
		testIDs = append(testIDs, id)
	}
	insertRows.Close()

	selectOp := driver.SelectOp{
		Select:     []string{"id", "name", "value"},
		FromSchema: schema,
		FromTable:  table,
		KeyColumns: []string{"id", "name"},
		Keys: []key.Key{
			key.New2(testIDs[0], "comp_select1"),
			key.New2(testIDs[1], "comp_select2"),
		},
	}

	rows, err := backend.Select(ctx, selectOp)
	if err != nil {
		t.Fatalf("Select with composite key failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int64
		var name string
		var value int
		if err := rows.Scan(&id, &name, &value); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		count++
	}

	if count != 2 {
		t.Errorf("Expected 2 rows, got %d", count)
	}
}

func testUpdateCompositeKey(t *testing.T, ctx context.Context, backend driver.Backend, schema, table string, ids []int64) {
	// Contract: Update handles composite keys (multiple key columns)
	// Insert test data
	insertOp := driver.InsertOp{
		IntoSchema: schema,
		IntoTable:  table,
		Insert:     []string{"name", "value"},
		Returning:  []string{"id"},
		Values:     [][]any{{"comp_update1", 100}, {"comp_update2", 200}},
	}
	insertRows, err := backend.Insert(ctx, insertOp)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	var testIDs []int64
	for insertRows.Next() {
		var id int64
		insertRows.Scan(&id)
		testIDs = append(testIDs, id)
	}
	insertRows.Close()

	updateOp := driver.UpdateOp{
		Schema: schema,
		Table:  table,
		Sets:   []string{"value"},
		Where:  []string{"id", "name"},
		SetValues: [][]any{
			{999},
			{888},
		},
		WhereValues: [][]any{
			{testIDs[0], "comp_update1"},
			{testIDs[1], "comp_update2"},
		},
	}

	err = backend.Update(ctx, updateOp)
	if err != nil {
		t.Fatalf("Update with composite key failed: %v", err)
	}
}

func testDeleteCompositeKey(t *testing.T, ctx context.Context, backend driver.Backend, schema, table string, ids []int64) {
	// Insert test data for deletion
	insertOp := driver.InsertOp{
		IntoSchema: schema,
		IntoTable:  table,
		Insert:     []string{"name", "value"},
		Returning:  []string{"id"},
		Values:     [][]any{{"delete_comp1", 10}, {"delete_comp2", 20}},
	}
	rows, err := backend.Insert(ctx, insertOp)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	var deleteIDs []int64
	for rows.Next() {
		var id int64
		rows.Scan(&id)
		deleteIDs = append(deleteIDs, id)
	}
	rows.Close()

	// Contract: Delete handles composite keys (multiple key columns)
	deleteOp := driver.DeleteOp{
		FromSchema: schema,
		FromTable:  table,
		KeyColumns: []string{"id", "name"},
		Keys: []key.Key{
			key.New2(deleteIDs[0], "delete_comp1"),
			key.New2(deleteIDs[1], "delete_comp2"),
		},
	}

	err = backend.Delete(ctx, deleteOp)
	if err != nil {
		t.Fatalf("Delete with composite key failed: %v", err)
	}
}

func testSelectEmptyKeys(t *testing.T, ctx context.Context, backend driver.Backend, schema, table string) {
	// Contract: Select with empty Keys returns empty Rows (no error)
	selectOp := driver.SelectOp{
		Select:     []string{"id", "name"},
		FromSchema: schema,
		FromTable:  table,
		KeyColumns: []string{"id"},
		Keys:       []key.Key{}, // Empty
	}

	rows, err := backend.Select(ctx, selectOp)
	if err != nil {
		t.Fatalf("Select with empty keys should not error, got: %v", err)
	}
	defer rows.Close()

	if rows == nil {
		t.Fatal("Contract violation: Select returned nil Rows")
	}

	// Should return zero rows
	count := 0
	for rows.Next() {
		count++
	}

	if count != 0 {
		t.Errorf("Expected 0 rows for empty keys, got %d", count)
	}
}

func testInsertEmptyValues(t *testing.T, ctx context.Context, backend driver.Backend, schema, table string) {
	// Contract: Insert with empty Values returns empty Rows (no error)
	insertOp := driver.InsertOp{
		IntoSchema: schema,
		IntoTable:  table,
		Insert:     []string{"name", "value"},
		Returning:  []string{"id"},
		Values:     [][]any{}, // Empty
	}

	rows, err := backend.Insert(ctx, insertOp)
	if err != nil {
		t.Fatalf("Insert with empty values should not error, got: %v", err)
	}
	defer rows.Close()

	if rows == nil {
		t.Fatal("Contract violation: Insert returned nil Rows")
	}

	// Should return zero rows
	count := 0
	for rows.Next() {
		count++
	}

	if count != 0 {
		t.Errorf("Expected 0 rows for empty values, got %d", count)
	}
}

func testUpdateEmptyValues(t *testing.T, ctx context.Context, backend driver.Backend, schema, table string) {
	// Contract: Update with empty SetValues succeeds (no-op)
	updateOp := driver.UpdateOp{
		Schema:      schema,
		Table:       table,
		Sets:        []string{"name"},
		Where:       []string{"id"},
		SetValues:   [][]any{}, // Empty
		WhereValues: [][]any{}, // Empty
	}

	err := backend.Update(ctx, updateOp)
	if err != nil {
		t.Fatalf("Update with empty values should not error, got: %v", err)
	}
}

func testDeleteEmptyKeys(t *testing.T, ctx context.Context, backend driver.Backend, schema, table string) {
	// Contract: Delete with empty Keys succeeds (no-op)
	deleteOp := driver.DeleteOp{
		FromSchema: schema,
		FromTable:  table,
		KeyColumns: []string{"id"},
		Keys:       []key.Key{}, // Empty
	}

	err := backend.Delete(ctx, deleteOp)
	if err != nil {
		t.Fatalf("Delete with empty keys should not error, got: %v", err)
	}
}
