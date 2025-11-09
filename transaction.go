package orm1

import "context"

// Begin starts a transaction.
func (s *Session) Begin(ctx context.Context) error {
	return s.backend.Begin(ctx)
}

// Commit commits the current transaction.
func (s *Session) Commit(ctx context.Context) error {
	return s.backend.Commit(ctx)
}

// Rollback rolls back the current transaction.
func (s *Session) Rollback(ctx context.Context) error {
	return s.backend.Rollback(ctx)
}
