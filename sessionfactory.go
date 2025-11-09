package orm1

import (
	"reflect"

	"github.com/hanpama/orm1/mapping"
)

// DefaultSessionFactory is the global SessionFactory instance for convenient entity registration.
// Most applications can use this instead of creating their own SessionFactory.
var DefaultSessionFactory = NewSessionFactory()

// SessionFactory constructs entity mappings and creates sessions.
// It uses EntityMappingBuilder internally to build mappings from registered entities.
type SessionFactory struct {
	builder *mapping.EntityMappingBuilder
	driver  Driver
}

// NewSessionFactory creates a new session factory without a driver.
// Use SetDriver to set the driver before creating sessions.
func NewSessionFactory() *SessionFactory {
	return &SessionFactory{
		builder: mapping.NewEntityMappingBuilder(),
	}
}

// NewSessionFactoryWithDriver creates a new session factory with a driver.
func NewSessionFactoryWithDriver(driver Driver) *SessionFactory {
	sf := NewSessionFactory()
	sf.driver = driver
	return sf
}

// SetDriver sets the driver for this factory.
func (sf *SessionFactory) SetDriver(driver Driver) *SessionFactory {
	sf.driver = driver
	return sf
}

// RegisterEntity defines a struct type as an entity and adds it to the builder.
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
//	sf := orm1.NewSessionFactory()
//
//	// Register parent entity
//	sf.RegisterEntity(&User{},
//	    orm1.WithTable("users"),
//	    orm1.WithPrimaryKey("ID"))
//
//	// Register child entity
//	sf.RegisterEntity(&Post{},
//	    orm1.WithTable("posts"))
//
//	session := sf.CreateSession()
func (sf *SessionFactory) RegisterEntity(entityPtr any, opts ...mapping.MappingOption) {
	val := reflect.ValueOf(entityPtr)
	if val.Kind() != reflect.Ptr {
		panic("entityPtr must be a pointer to struct")
	}
	entityType := val.Elem().Type()
	if entityType.Kind() != reflect.Struct {
		panic("entityPtr must point to a struct")
	}

	sf.builder.Register(entityType, opts...)
}

// CreateSession creates a new Session with a fresh backend instance.
// The backend is created using the Driver that was configured
// via NewSessionFactoryWithDriver or SetDriver.
//
// Each call to CreateSession creates a new backend instance, as backends
// are not safe for concurrent use across multiple sessions.
//
// Entity mappings are built lazily on the first call and cached for
// subsequent calls, making session creation very efficient.
//
// Panics if the driver has not been set.
func (sf *SessionFactory) CreateSession() *Session {
	if sf.driver == nil {
		panic("Driver not set. Use SetDriver() or NewSessionFactoryWithDriver() before creating sessions.")
	}
	backend := sf.driver.CreateBackend()
	mappings := sf.builder.Build()
	return newSession(backend, mappings)
}

// WithTable sets the table name for the entity.
// If not specified, defaults to snake_case of the struct name.
func WithTable(table string) mapping.MappingOption {
	return mapping.WithTable(table)
}

// WithSchema sets the database schema for the entity's table.
func WithSchema(schema string) mapping.MappingOption {
	return mapping.WithSchema(schema)
}

// WithPrimaryKey specifies the field names that form the primary key.
// If not specified, defaults to the "ID" field.
func WithPrimaryKey(fieldNames ...string) mapping.MappingOption {
	return mapping.WithPrimaryKey(fieldNames...)
}
