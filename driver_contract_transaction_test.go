package orm1

import (
	"context"
	"testing"
)

func testTransactionContracts(t *testing.T, ctx context.Context, backend Backend, schema, table string) {
	t.Run("Begin_StateTransition", func(t *testing.T) {
		testBeginStateTransition(t, ctx, backend)
	})

	t.Run("BeginTx_WithOptions", func(t *testing.T) {
		testBeginTxWithOptions(t, ctx, backend, schema, table)
	})

	t.Run("Commit_NoTransaction", func(t *testing.T) {
		testCommitNoTransaction(t, ctx, backend)
	})

	t.Run("Rollback_NoTransaction", func(t *testing.T) {
		testRollbackNoTransaction(t, ctx, backend)
	})

	t.Run("Commit_StateTransition", func(t *testing.T) {
		testCommitStateTransition(t, ctx, backend)
	})

	t.Run("Rollback_StateTransition", func(t *testing.T) {
		testRollbackStateTransition(t, ctx, backend)
	})

	t.Run("Transaction_CommitVisibility", func(t *testing.T) {
		testTransactionCommitVisibility(t, ctx, backend, schema, table)
	})

	t.Run("Transaction_RollbackInvisibility", func(t *testing.T) {
		testTransactionRollbackInvisibility(t, ctx, backend, schema, table)
	})

	t.Run("Savepoint_NoTransaction", func(t *testing.T) {
		testSavepointNoTransaction(t, ctx, backend)
	})

	t.Run("Savepoint_RollbackReverts", func(t *testing.T) {
		testSavepointRollbackReverts(t, ctx, backend, schema, table)
	})

	t.Run("Savepoint_ReleaseCommits", func(t *testing.T) {
		testSavepointReleaseCommits(t, ctx, backend, schema, table)
	})

	t.Run("ReleaseSavepoint_NoTransaction", func(t *testing.T) {
		testReleaseSavepointNoTransaction(t, ctx, backend)
	})

	t.Run("RollbackToSavepoint_NoTransaction", func(t *testing.T) {
		testRollbackToSavepointNoTransaction(t, ctx, backend)
	})

	t.Run("Transaction_CRUDOperations", func(t *testing.T) {
		testTransactionCRUDOperations(t, ctx, backend, schema, table)
	})
}

func testBeginStateTransition(t *testing.T, ctx context.Context, backend Backend) {
	// Contract: Begin transitions from Initial to InTransaction
	err := backend.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	// Verify we're in transaction by attempting commit
	err = backend.Commit(ctx)
	if err != nil {
		t.Errorf("Commit failed: %v", err)
	}
}

func testBeginTxWithOptions(t *testing.T, ctx context.Context, backend Backend, schema, table string) {
	// Contract: BeginTx accepts transaction options (isolation level, read-only)

	// Test 1: BeginTx with nil options
	err := backend.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx with nil options failed: %v", err)
	}
	backend.Commit(ctx)

	// Test 2: BeginTx with serializable isolation level
	opts := &TxOptions{
		Isolation: LevelSerializable,
		ReadOnly:  false,
	}
	err = backend.BeginTx(ctx, opts)
	if err != nil {
		t.Fatalf("BeginTx with serializable isolation failed: %v", err)
	}

	// Verify transaction works by inserting data
	insertOp := InsertOp{
		IntoSchema: schema,
		IntoTable:  table,
		Insert:     []string{"name", "value"},
		Returning:  []string{"id"},
		Values:     [][]any{{"begintx_test", 999}},
	}
	rows, err := backend.Insert(ctx, insertOp)
	if err != nil {
		backend.Rollback(ctx)
		t.Fatalf("Insert in BeginTx failed: %v", err)
	}
	rows.Close()

	backend.Commit(ctx)

	// Test 3: BeginTx with read-only option and default isolation
	optsReadOnly := &TxOptions{
		Isolation: LevelDefault,
		ReadOnly:  true,
	}
	err = backend.BeginTx(ctx, optsReadOnly)
	if err != nil {
		t.Fatalf("BeginTx with read-only failed: %v", err)
	}
	backend.Rollback(ctx)

	// Test 4: BeginTx with various isolation levels
	isolationLevels := []IsolationLevel{
		LevelReadUncommitted,
		LevelReadCommitted,
		LevelWriteCommitted,
		LevelRepeatableRead,
		LevelSnapshot,
		LevelLinearizable,
	}
	for _, level := range isolationLevels {
		opts := &TxOptions{
			Isolation: level,
			ReadOnly:  false,
		}
		err := backend.BeginTx(ctx, opts)
		if err != nil {
			// Some isolation levels may not be supported by all databases
			t.Logf("BeginTx with isolation level %d: %v", level, err)
		}
		if err == nil {
			backend.Rollback(ctx)
		}
	}
}

func testCommitNoTransaction(t *testing.T, ctx context.Context, backend Backend) {
	// Contract: Commit without transaction returns error
	err := backend.Commit(ctx)
	if err == nil {
		t.Error("Commit without transaction succeeded, expected error")
	}
}

func testRollbackNoTransaction(t *testing.T, ctx context.Context, backend Backend) {
	// Contract: Rollback without transaction returns error
	err := backend.Rollback(ctx)
	if err == nil {
		t.Error("Rollback without transaction succeeded, expected error")
	}
}

func testCommitStateTransition(t *testing.T, ctx context.Context, backend Backend) {
	// Contract: Commit transitions from InTransaction to Initial
	if err := backend.Begin(ctx); err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	if err := backend.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify we're back in Initial state
	if err := backend.Begin(ctx); err != nil {
		t.Errorf("Begin after commit failed: %v", err)
	}
	backend.Rollback(ctx)
}

func testRollbackStateTransition(t *testing.T, ctx context.Context, backend Backend) {
	// Contract: Rollback transitions from InTransaction to Initial
	if err := backend.Begin(ctx); err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	if err := backend.Rollback(ctx); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	// Verify we're back in Initial state
	if err := backend.Begin(ctx); err != nil {
		t.Errorf("Begin after rollback failed: %v", err)
	}
	backend.Rollback(ctx)
}

func testTransactionCommitVisibility(t *testing.T, ctx context.Context, backend Backend, schema, table string) {
	// Contract: Committed changes are visible
	if err := backend.Begin(ctx); err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	insertOp := InsertOp{
		IntoSchema: schema,
		IntoTable:  table,
		Insert:     []string{"name", "value"},
		Returning:  []string{"id"},
		Values: [][]any{
			{"tx_commit_test", 777},
		},
	}

	rows, err := backend.Insert(ctx, insertOp)
	if err != nil {
		backend.Rollback(ctx)
		t.Fatalf("Insert failed: %v", err)
	}

	var insertedID int64
	if rows.Next() {
		rows.Scan(&insertedID)
	}
	rows.Close()

	if err := backend.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify data is visible after commit
	selectOp := SelectOp{
		Select:     []string{"name"},
		FromSchema: schema,
		FromTable:  table,
		KeyColumns: []string{"id"},
		Keys:       []Key{New1(insertedID)},
	}

	rows, err = backend.Select(ctx, selectOp)
	if err != nil {
		t.Fatalf("Select after commit failed: %v", err)
	}

	found := false
	for rows.Next() {
		var name string
		rows.Scan(&name)
		if name == "tx_commit_test" {
			found = true
		}
	}
	rows.Close()

	if !found {
		t.Error("Committed data not visible")
	}
}

func testTransactionRollbackInvisibility(t *testing.T, ctx context.Context, backend Backend, schema, table string) {
	// Contract: Rolled back changes are not visible
	if err := backend.Begin(ctx); err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	insertOp := InsertOp{
		IntoSchema: schema,
		IntoTable:  table,
		Insert:     []string{"name", "value"},
		Returning:  []string{"id"},
		Values: [][]any{
			{"tx_rollback_test", 888},
		},
	}

	rows, err := backend.Insert(ctx, insertOp)
	if err != nil {
		backend.Rollback(ctx)
		t.Fatalf("Insert failed: %v", err)
	}

	var insertedID int64
	if rows.Next() {
		rows.Scan(&insertedID)
	}
	rows.Close()

	if err := backend.Rollback(ctx); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	// Verify data is NOT visible after rollback
	selectOp := SelectOp{
		Select:     []string{"name"},
		FromSchema: schema,
		FromTable:  table,
		KeyColumns: []string{"id"},
		Keys:       []Key{New1(insertedID)},
	}

	rows, err = backend.Select(ctx, selectOp)
	if err != nil {
		t.Fatalf("Select after rollback failed: %v", err)
	}

	found := false
	for rows.Next() {
		found = true
	}
	rows.Close()

	if found {
		t.Error("Rolled back data is visible")
	}
}

func testSavepointNoTransaction(t *testing.T, ctx context.Context, backend Backend) {
	// Contract: Savepoint without transaction returns error
	err := backend.Savepoint(ctx, "sp1")
	if err == nil {
		t.Error("Savepoint without transaction succeeded, expected error")
	}
}

func testSavepointRollbackReverts(t *testing.T, ctx context.Context, backend Backend, schema, table string) {
	// Contract: Rollback to savepoint reverts changes after savepoint
	if err := backend.Begin(ctx); err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	// Insert 1
	insertOp1 := InsertOp{
		IntoSchema: schema,
		IntoTable:  table,
		Insert:     []string{"name", "value"},
		Returning:  []string{"id"},
		Values:     [][]any{{"sp_test1", 111}},
	}
	rows, err := backend.Insert(ctx, insertOp1)
	if err != nil {
		backend.Rollback(ctx)
		t.Fatalf("Insert 1 failed: %v", err)
	}
	var id1 int64
	if rows.Next() {
		rows.Scan(&id1)
	}
	rows.Close()

	// Create savepoint
	if err := backend.Savepoint(ctx, "sp1"); err != nil {
		backend.Rollback(ctx)
		t.Fatalf("Savepoint failed: %v", err)
	}

	// Insert 2
	insertOp2 := InsertOp{
		IntoSchema: schema,
		IntoTable:  table,
		Insert:     []string{"name", "value"},
		Returning:  []string{"id"},
		Values:     [][]any{{"sp_test2", 222}},
	}
	rows, err = backend.Insert(ctx, insertOp2)
	if err != nil {
		backend.Rollback(ctx)
		t.Fatalf("Insert 2 failed: %v", err)
	}
	var id2 int64
	if rows.Next() {
		rows.Scan(&id2)
	}
	rows.Close()

	// Rollback to savepoint
	if err := backend.RollbackToSavepoint(ctx, "sp1"); err != nil {
		backend.Rollback(ctx)
		t.Fatalf("RollbackToSavepoint failed: %v", err)
	}

	// Commit transaction
	if err := backend.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify: id1 should exist, id2 should not
	selectOp := SelectOp{
		Select:     []string{"id"},
		FromSchema: schema,
		FromTable:  table,
		KeyColumns: []string{"id"},
		Keys:       []Key{New1(id1), New1(id2)},
	}

	rows, err = backend.Select(ctx, selectOp)
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}

	count := 0
	for rows.Next() {
		var id int64
		rows.Scan(&id)
		if id == id2 {
			t.Error("Insert 2 visible after rollback to savepoint")
		}
		count++
	}
	rows.Close()

	if count != 1 {
		t.Errorf("Expected 1 row (id1), got %d", count)
	}
}

func testSavepointReleaseCommits(t *testing.T, ctx context.Context, backend Backend, schema, table string) {
	// Contract: Release savepoint commits changes
	if err := backend.Begin(ctx); err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	// Create savepoint
	if err := backend.Savepoint(ctx, "sp2"); err != nil {
		backend.Rollback(ctx)
		t.Fatalf("Savepoint failed: %v", err)
	}

	// Insert
	insertOp := InsertOp{
		IntoSchema: schema,
		IntoTable:  table,
		Insert:     []string{"name", "value"},
		Returning:  []string{"id"},
		Values:     [][]any{{"sp_release_test", 333}},
	}
	rows, err := backend.Insert(ctx, insertOp)
	if err != nil {
		backend.Rollback(ctx)
		t.Fatalf("Insert failed: %v", err)
	}
	var insertedID int64
	if rows.Next() {
		rows.Scan(&insertedID)
	}
	rows.Close()

	// Release savepoint
	if err := backend.ReleaseSavepoint(ctx, "sp2"); err != nil {
		backend.Rollback(ctx)
		t.Fatalf("ReleaseSavepoint failed: %v", err)
	}

	// Commit transaction
	if err := backend.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Verify data is visible
	selectOp := SelectOp{
		Select:     []string{"name"},
		FromSchema: schema,
		FromTable:  table,
		KeyColumns: []string{"id"},
		Keys:       []Key{New1(insertedID)},
	}

	rows, err = backend.Select(ctx, selectOp)
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}

	found := false
	for rows.Next() {
		found = true
	}
	rows.Close()

	if !found {
		t.Error("Released savepoint data not visible")
	}
}

func testReleaseSavepointNoTransaction(t *testing.T, ctx context.Context, backend Backend) {
	// Contract: ReleaseSavepoint without transaction returns error
	err := backend.ReleaseSavepoint(ctx, "sp_test")
	if err == nil {
		t.Error("ReleaseSavepoint without transaction succeeded, expected error")
	}
}

func testRollbackToSavepointNoTransaction(t *testing.T, ctx context.Context, backend Backend) {
	// Contract: RollbackToSavepoint without transaction returns error
	err := backend.RollbackToSavepoint(ctx, "sp_test")
	if err == nil {
		t.Error("RollbackToSavepoint without transaction succeeded, expected error")
	}
}

func testTransactionCRUDOperations(t *testing.T, ctx context.Context, backend Backend, schema, table string) {
	// Contract: CRUD operations work correctly within transactions
	err := backend.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	// Test Insert within transaction
	insertOp := InsertOp{
		IntoSchema: schema,
		IntoTable:  table,
		Insert:     []string{"name", "value"},
		Returning:  []string{"id"},
		Values:     [][]any{{"tx_crud_test", 555}},
	}
	rows, err := backend.Insert(ctx, insertOp)
	if err != nil {
		backend.Rollback(ctx)
		t.Fatalf("Insert in transaction failed: %v", err)
	}
	var insertedID int64
	if rows.Next() {
		rows.Scan(&insertedID)
	}
	rows.Close()

	// Test Update within transaction
	updateOp := UpdateOp{
		Schema:      schema,
		Table:       table,
		Sets:        []string{"value"},
		Where:       []string{"id"},
		SetValues:   [][]any{{666}},
		WhereValues: [][]any{{insertedID}},
	}
	err = backend.Update(ctx, updateOp)
	if err != nil {
		backend.Rollback(ctx)
		t.Fatalf("Update in transaction failed: %v", err)
	}

	// Test Delete within transaction
	deleteOp := DeleteOp{
		FromSchema: schema,
		FromTable:  table,
		KeyColumns: []string{"id"},
		Keys:       []Key{New1(insertedID)},
	}
	err = backend.Delete(ctx, deleteOp)
	if err != nil {
		backend.Rollback(ctx)
		t.Fatalf("Delete in transaction failed: %v", err)
	}

	backend.Commit(ctx)
}
