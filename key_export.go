package orm1

import "github.com/hanpama/orm1/key"

// Key represents an entity's primary or foreign key value(s).
// It is a comparable interface that can be used as a map key.
//
// Examples:
//
//	session.Get(ctx, &user, orm1.NewKey(1))              // single-column key
//	session.Get(ctx, &item, orm1.NewKey(orderId, itemId)) // composite key
type Key = key.Key

// NewKey creates a new Key from the given values.
// Supports up to 9 column values.
var NewKey = key.NewKey
