package orm1

import (
	"reflect"

	"github.com/hanpama/orm1/mapping"
)

// Registry is a storage for entity type metadata.
// It collects struct field metadata through registration and provides
// this metadata to SessionFactory for building EntityMapping instances.
type Registry struct {
	registered map[reflect.Type]bool
	metadata   map[reflect.Type]mapping.EntityMetadata
}

// NewRegistry creates a new registry.
func NewRegistry() *Registry {
	return &Registry{
		registered: make(map[reflect.Type]bool),
		metadata:   make(map[reflect.Type]mapping.EntityMetadata),
	}
}

// DefaultRegistry is the global registry for entity types.
var DefaultRegistry = NewRegistry()

// Register adds an entity type to the registry with optional configuration.
//
// # Entity Model
//
// An entity represents a database table row and its relationships. Entities are defined
// as Go structs with fields mapped to database columns. orm1 uses struct tags and naming
// conventions to determine how fields map to the database.
//
// # Parameters
//
// entityPtr must be a pointer to a struct instance (e.g., &User{}). The struct type
// defines the entity's schema.
//
// Options can customize the mapping:
//   - WithTable("table_name"): Set table name (default: snake_case of struct name)
//   - WithSchema("schema_name"): Set database schema
//   - WithPrimaryKey("Field1", "Field2"): Define composite primary key
//
// # Struct Tags
//
// Fields can be annotated with `orm1` struct tags to control mapping behavior:
//
//	type User struct {
//	    ID        int64  `orm1:"auto"`              // Auto-managed (AUTOINCREMENT, etc.)
//	    ParentID  int64  `orm1:"parental"`          // Foreign key to parent
//	    Email     string `orm1:"column:user_email"` // Custom column name
//	    CreatedAt time.Time `orm1:"skip_insert"`    // Not inserted (e.g., trigger-managed)
//	    UpdatedAt time.Time `orm1:"skip_update"`    // Not updated (e.g., immutable)
//	    Internal  string `orm1:"ignore"`            // Not persisted at all
//	    Posts     []*Post `orm1:"child"`            // One-to-many relationship
//	    Profile   *Profile `orm1:"child"`           // One-to-one relationship
//	}
//
// Available tags:
//   - auto: Exclude from both INSERT and UPDATE (for DB-managed values like AUTOINCREMENT)
//   - primary: Mark as primary key field
//   - parental: Mark as foreign key to parent entity
//   - column:name: Specify database column name (default: snake_case of field name)
//   - skip_insert: Exclude from INSERT statements (for DB-generated values)
//   - skip_update: Exclude from UPDATE statements (for immutable columns)
//   - ignore: Exclude field from persistence entirely
//   - child: Mark as relationship field (auto-detected for registered entity types)
//
// # Auto-Detection Rules
//
// When tags are not specified, orm1 applies these conventions:
//   - Field named "ID" is the primary key
//   - Field names map to snake_case column names
//   - Fields of registered entity types ([]*T or *T) are children
//   - Parental keys must be explicitly tagged with `orm1:"parental"`
//
// # Relationships
//
// Child entities are loaded and saved cascading from their parents. Define relationships
// as fields with registered entity types:
//   - []*ChildType: One-to-many (plural children)
//   - *ChildType: One-to-one (singular child)
//
// Child entities must have parental key fields matching the parent's primary key.
//
// # Example
//
//	registry := orm1.NewRegistry()
//
//	// Register parent entity
//	registry.Register(&User{},
//	    orm1.WithTable("users"),
//	    orm1.WithPrimaryKey("ID"))
//
//	// Register child entity
//	registry.Register(&Post{},
//	    orm1.WithTable("posts"))
//
//	// Create factory with registry
//	factory := orm1.NewSessionFactory(registry, driver)
//	session := factory.CreateSession()
func (r *Registry) Register(entityPtr any, opts ...MappingOption) {
	val := reflect.ValueOf(entityPtr)
	if val.Kind() != reflect.Ptr {
		panic("entityPtr must be a pointer to struct")
	}
	entityType := val.Elem().Type()
	if entityType.Kind() != reflect.Struct {
		panic("entityPtr must point to a struct")
	}

	r.registered[entityType] = true

	// Analyze struct to get field metadata
	fields := mapping.AnalyzeStruct(entityType)

	meta := mapping.EntityMetadata{
		Schema: "",
		Table:  mapping.ToSnakeCase(entityType.Name()),
		Fields: fields,
	}

	// Apply options to configure metadata
	for _, opt := range opts {
		opt(&meta)
	}

	r.metadata[entityType] = meta
}

// GetMetadata returns the collected metadata for building.
// This is used by SessionFactory to build EntityMapping instances.
func (r *Registry) GetMetadata() map[reflect.Type]mapping.EntityMetadata {
	return r.metadata
}

// GetRegistered returns the set of registered entity types.
// This is used by SessionFactory to detect child relationships.
func (r *Registry) GetRegistered() map[reflect.Type]bool {
	return r.registered
}

// MappingOption is a function that configures an EntityMetadata.
type MappingOption func(*mapping.EntityMetadata)

// WithSchema sets the database schema for the entity's table.
func WithSchema(schema string) MappingOption {
	return func(m *mapping.EntityMetadata) {
		m.Schema = schema
	}
}

// WithTable sets the table name for the entity.
// If not specified, defaults to snake_case of the struct name.
func WithTable(table string) MappingOption {
	return func(m *mapping.EntityMetadata) {
		m.Table = table
	}
}

