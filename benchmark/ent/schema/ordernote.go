package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
)

// OrderNote holds the schema definition for the OrderNote entity.
type OrderNote struct {
	ent.Schema
}

// Fields of the OrderNote.
func (OrderNote) Fields() []ent.Field {
	return []ent.Field{
		field.Int64("id").
			StorageKey("id"),
		field.Int64("order_id"),
		field.String("content").
			NotEmpty(),
	}
}

// Edges of the OrderNote.
func (OrderNote) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("order", Order.Type).
			Ref("notes").
			Field("order_id").
			Unique().
			Required(),
	}
}
