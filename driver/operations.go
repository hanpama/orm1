package driver

import "github.com/hanpama/orm1/key"

// SelectOp represents a batch SELECT operation for entity retrieval.
// Used by Get and BatchGet operations to load entities by primary key.
//
// This is a command from Session to Backend, specifying:
// - Which table to query (FromSchema, FromTable)
// - Which columns to return (Select)
// - Which key columns to filter by (KeyColumns)
// - The actual key values for batch retrieval (Keys)
//
// Keys[i] provides the key values for each entity to retrieve.
type SelectOp struct {
	Select     []string  // Columns to select
	FromSchema string    // Database schema (optional)
	FromTable  string    // Table name
	KeyColumns []string  // Columns used for WHERE clause (typically primary key)
	Keys       []key.Key // Batch key values
}

// InsertOp represents a batch INSERT operation for entity creation.
// Used by Save operations when creating new entities.
//
// This is a command from Session to Backend, specifying:
// - Which table to insert into (IntoSchema, IntoTable)
// - Which columns to insert (Insert)
// - Which columns to return after insert (Returning, e.g., auto-generated IDs)
// - The actual values to insert (Values)
//
// Values[i] corresponds to Insert columns order: Values[i][j] maps to Insert[j].
type InsertOp struct {
	IntoSchema string    // Database schema (optional)
	IntoTable  string    // Table name
	Insert     []string  // Columns to insert
	Returning  []string  // Columns to return (e.g., auto-generated columns)
	Values     [][]any   // Batch insert values, Values[i][j] maps to Insert[j]
}

// UpdateOp represents a batch UPDATE operation for entity modification.
// Used by Save operations when updating existing entities.
//
// This is a command from Session to Backend, specifying:
// - Which table to update (Schema, Table)
// - Which columns to update (Sets)
// - Which columns to use in WHERE clause (Where, typically primary key)
// - Which columns to return after update (Returning)
// - The actual values for SET and WHERE clauses (SetValues, WhereValues)
//
// SetValues[i][j] corresponds to Sets[j]
// WhereValues[i][j] corresponds to Where[j]
type UpdateOp struct {
	Schema      string    // Database schema (optional)
	Table       string    // Table name
	Sets        []string  // Columns to update (SET clause)
	Where       []string  // Columns for WHERE clause (typically primary key)
	Returning   []string  // Columns to return after update
	SetValues   [][]any   // Values for SET clause: SetValues[i][j] maps to Sets[j]
	WhereValues [][]any   // Values for WHERE clause: WhereValues[i][j] maps to Where[j]
}

// DeleteOp represents a batch DELETE operation for entity removal.
// Used by Delete operations to remove entities from the database.
//
// This is a command from Session to Backend, specifying:
// - Which table to delete from (FromSchema, FromTable)
// - Which key columns to use in WHERE clause (KeyColumns)
// - The actual key values for batch deletion (Keys)
//
// Keys[i] provides the key values for each entity to delete.
type DeleteOp struct {
	FromSchema string    // Database schema (optional)
	FromTable  string    // Table name
	KeyColumns []string  // Columns used for WHERE clause (typically primary key)
	Keys       []key.Key // Batch key values
}
