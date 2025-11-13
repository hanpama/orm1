package orm1

import (
	"context"
	"fmt"

	"github.com/hanpama/orm1/driver"
)

// IsolationLevel represents the isolation level for a transaction.
type IsolationLevel int

const (
	LevelDefault IsolationLevel = iota
	LevelReadUncommitted
	LevelReadCommitted
	LevelWriteCommitted
	LevelRepeatableRead
	LevelSnapshot
	LevelSerializable
	LevelLinearizable
)

// TxOptions holds transaction options for Begin.
type TxOptions struct {
	Isolation IsolationLevel
	ReadOnly  bool
}

// ReadOnlyTxOptions returns TxOptions for a read-only transaction.
func ReadOnlyTxOptions() *TxOptions {
	return &TxOptions{ReadOnly: true}
}

// SerializableTxOptions returns TxOptions for a serializable transaction.
func SerializableTxOptions() *TxOptions {
	return &TxOptions{Isolation: LevelSerializable}
}

// RepeatableReadTxOptions returns TxOptions for a repeatable read transaction.
func RepeatableReadTxOptions() *TxOptions {
	return &TxOptions{Isolation: LevelRepeatableRead}
}

// Transaction represents a database transaction that can be committed or rolled back.
// It is safe to call Rollback in a defer statement even after Commit has been called.
type Transaction struct {
	session    *Session
	depth      int
	committed  bool
	rolledback bool
}

// Begin starts a transaction with optional transaction options.
// For the first transaction, it calls backend.BeginTx().
// For nested transactions, it creates a SAVEPOINT.
// It backs up the current session state onto a stack for potential restoration on Rollback.
//
// Default isolation level: READ COMMITTED
// SQLite: Only supports SERIALIZABLE (ignores other levels)
// PostgreSQL: Supports all standard isolation levels
//
// Nested transactions inherit the isolation level from the parent and cannot change it.
func (s *Session) Begin(ctx context.Context, opts *TxOptions) (*Transaction, error) {
	// Check for invalid options in nested transactions
	if len(s.txStack) > 0 && opts != nil {
		if opts.Isolation != 0 {
			return nil, fmt.Errorf("cannot specify isolation level in nested transaction")
		}
		if opts.ReadOnly {
			return nil, fmt.Errorf("cannot specify read-only in nested transaction")
		}
	}

	// Copy the maps because they will be modified during the transaction
	persisted := make(map[any]struct{}, len(s.persisted))
	for k, v := range s.persisted {
		persisted[k] = v
	}

	children := make(map[childrenKey][]any, len(s.children))
	for k, v := range s.children {
		children[k] = v // Slice reference is fine since we replace the whole map on rollback
	}

	state := txState{
		persisted: persisted,
		children:  children,
	}

	// Push state onto stack
	s.txStack = append(s.txStack, state)
	depth := len(s.txStack)

	tx := &Transaction{
		session: s,
		depth:   depth,
	}

	// First transaction: BEGIN
	if depth == 1 {
		var driverOpts *driver.TxOptions
		if opts != nil {
			driverOpts = &driver.TxOptions{
				Isolation: driver.IsolationLevel(opts.Isolation),
				ReadOnly:  opts.ReadOnly,
			}
		}
		if err := s.backend.BeginTx(ctx, driverOpts); err != nil {
			s.txStack = s.txStack[:depth-1]
			return nil, err
		}
		return tx, nil
	}

	// Nested transaction: SAVEPOINT
	name := fmt.Sprintf("sp_%d", depth-1)
	if err := s.backend.Savepoint(ctx, name); err != nil {
		s.txStack = s.txStack[:depth-1]
		return nil, err
	}

	return tx, nil
}

// Commit commits the transaction.
// For the outermost transaction, it calls backend.Commit().
// For nested transactions, it releases the SAVEPOINT.
// After Commit succeeds, subsequent calls to Commit or Rollback are no-ops (safe for defer).
func (tx *Transaction) Commit(ctx context.Context) error {
	if tx.committed || tx.rolledback {
		return nil // Already processed, safe for defer
	}

	currentDepth := len(tx.session.txStack)

	// Depth validation
	if currentDepth != tx.depth {
		if currentDepth < tx.depth {
			// Already rolled back by parent
			return fmt.Errorf("transaction already rolled back by parent")
		}
		// currentDepth > tx.depth: nested transaction still active
		return fmt.Errorf("cannot commit: nested transaction still active")
	}

	tx.committed = true

	// Pop state from stack
	tx.session.txStack = tx.session.txStack[:currentDepth-1]

	// Outermost transaction: COMMIT
	if tx.depth == 1 {
		return tx.session.backend.Commit(ctx)
	}

	// Nested transaction: RELEASE SAVEPOINT
	name := fmt.Sprintf("sp_%d", tx.depth-1)
	return tx.session.backend.ReleaseSavepoint(ctx, name)
}

// Rollback rolls back the transaction.
// For the outermost transaction, it calls backend.Rollback().
// For nested transactions, it rolls back to the SAVEPOINT.
// It restores the session state to what it was before Begin was called.
// After Rollback succeeds or if already committed, subsequent calls are no-ops (safe for defer).
func (tx *Transaction) Rollback(ctx context.Context) error {
	if tx.committed || tx.rolledback {
		return nil // Already processed, safe for defer
	}

	currentDepth := len(tx.session.txStack)

	// Already rolled back by parent
	if currentDepth < tx.depth {
		return nil
	}

	// Nested transactions still active - roll them all back
	if currentDepth > tx.depth {
		for currentDepth > tx.depth {
			state := tx.session.txStack[currentDepth-1]
			tx.session.txStack = tx.session.txStack[:currentDepth-1]
			tx.session.persisted = state.persisted
			tx.session.children = state.children
			currentDepth--
		}
	}

	tx.rolledback = true

	// Restore state
	state := tx.session.txStack[tx.depth-1]
	tx.session.txStack = tx.session.txStack[:tx.depth-1]
	tx.session.persisted = state.persisted
	tx.session.children = state.children

	// Outermost transaction: ROLLBACK
	if tx.depth == 1 {
		return tx.session.backend.Rollback(ctx)
	}

	// Nested transaction: ROLLBACK TO SAVEPOINT
	name := fmt.Sprintf("sp_%d", tx.depth-1)
	return tx.session.backend.RollbackToSavepoint(ctx, name)
}
