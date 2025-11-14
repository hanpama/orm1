package orm1

import (
	"reflect"

	"github.com/hanpama/orm1/mapping"
)

// Registry constructs EntityMapping instances from struct types.
// It analyzes struct fields to determine columns, relationships, and keys.
type Registry struct {
	registered map[reflect.Type]bool
	configs    map[reflect.Type]*entityConfig
	mappings   map[reflect.Type]*mapping.EntityMapping
	built      bool
}

// entityConfig holds configuration for an entity mapping.
type entityConfig struct {
	Schema      string
	Table       string
	PrimaryKey  []string
	ParentalKey []string
}

// NewRegistry creates a new registry.
func NewRegistry() *Registry {
	return &Registry{
		registered: make(map[reflect.Type]bool),
		configs:    make(map[reflect.Type]*entityConfig),
		mappings:   make(map[reflect.Type]*mapping.EntityMapping),
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

	config := &entityConfig{
		Schema:      "",
		Table:       mapping.ToSnakeCase(entityType.Name()),
		PrimaryKey:  nil,
		ParentalKey: nil,
	}
	r.configs[entityType] = config

	tempMapping := &mapping.EntityMapping{
		Schema:     config.Schema,
		Table:      config.Table,
		PrimaryKey: config.PrimaryKey,
	}
	for _, opt := range opts {
		opt(tempMapping)
	}
	config.Schema = tempMapping.Schema
	config.Table = tempMapping.Table
	config.PrimaryKey = tempMapping.PrimaryKey
}

// Build constructs EntityMapping instances for all registered types.
// Returns a map of entity type to mapping for fast lookup without additional allocations.
func (r *Registry) Build() map[reflect.Type]*mapping.EntityMapping {
	if r.built {
		return r.mappings
	}

	for entityType := range r.registered {
		if _, ok := r.mappings[entityType]; !ok {
			entityMapping := r.buildMapping(entityType)
			r.mappings[entityType] = entityMapping
		}
	}

	r.built = true

	return r.mappings
}

// buildMapping constructs an EntityMapping for a single entity type.
func (r *Registry) buildMapping(entityType reflect.Type) *mapping.EntityMapping {
	config := r.configs[entityType]

	fieldMap := make(map[string]*mapping.Field)
	childMap := make(map[string]*mapping.Child)
	allFields := []string{}
	primaryKey := []string{}
	parentalKey := []string{}
	insertable := []string{}
	updatable := []string{}

	fieldsMetadata := mapping.AnalyzeStruct(entityType)

	for _, metadata := range fieldsMetadata {
		fieldName := metadata.Name
		fieldType := metadata.Typ

		if metadata.IgnoreTag {
			continue
		}

		isChild := metadata.ChildTag
		var childTarget reflect.Type
		var childSingular bool

		if !isChild {
			// Auto-detect child relationship if not explicitly tagged
			// Check for slice of registered type: []*Post
			if fieldType.Kind() == reflect.Slice {
				elemType := fieldType.Elem()
				if elemType.Kind() == reflect.Ptr {
					targetType := elemType.Elem()
					if r.registered[targetType] {
						isChild = true
						childTarget = targetType
						childSingular = false
					}
				}
			}

			if !isChild && fieldType.Kind() == reflect.Ptr {
				targetType := fieldType.Elem()
				if r.registered[targetType] {
					isChild = true
					childTarget = targetType
					childSingular = true
				}
			}
		} else {
			if fieldType.Kind() == reflect.Slice {
				elemType := fieldType.Elem()
				if elemType.Kind() == reflect.Ptr {
					childTarget = elemType.Elem()
					childSingular = false
				}
			} else if fieldType.Kind() == reflect.Ptr {
				childTarget = fieldType.Elem()
				childSingular = true
			}
		}

		if isChild {
			child := mapping.Child{
				Target:     childTarget,
				Singular:   childSingular,
				Type:       fieldType,
				ByteOffset: metadata.ByteOffset,
			}
			childMap[fieldName] = &child
			continue
		}

		if fieldType.Kind() == reflect.Slice {
			continue
		}

		if fieldType.Kind() == reflect.Ptr && fieldType.Elem().Kind() == reflect.Struct {
			continue
		}

		columnName := metadata.ColumnTag
		if columnName == "" {
			columnName = metadata.DefaultColumn
		}

		field := mapping.Field{
			Name:       metadata.Name,
			Column:     columnName,
			Type:       metadata.Typ,
			ByteOffset: metadata.ByteOffset,
		}

		fieldMap[fieldName] = &field

		isPrimaryKey := metadata.PrimaryTag
		isParentalKey := metadata.ParentalTag

		if !isPrimaryKey && !isParentalKey {
			if len(config.PrimaryKey) > 0 {
				// Use configured primary key
				for _, pkField := range config.PrimaryKey {
					if pkField == fieldName {
						isPrimaryKey = true
						break
					}
				}
			} else {
				// Default: ID field is primary key
				isPrimaryKey = (fieldName == "ID")
			}
		}

		// Add to AllFields (all fields are included regardless of role)
		allFields = append(allFields, fieldName)

		// A field can be both primary and parental (e.g., 1:1 relationship where PK=FK)
		if isPrimaryKey {
			primaryKey = append(primaryKey, fieldName)
		}
		if isParentalKey {
			parentalKey = append(parentalKey, fieldName)
		}

		if isPrimaryKey || isParentalKey {
			// Primary/parental keys are insertable unless tagged skip_insert
			if !metadata.SkipInsertTag {
				insertable = append(insertable, fieldName)
			}
			// Primary/parental keys are not updatable (they define identity)
		} else {
			// Regular fields are insertable and updatable unless tagged otherwise
			if !metadata.SkipInsertTag {
				insertable = append(insertable, fieldName)
			}
			if !metadata.SkipUpdateTag {
				updatable = append(updatable, fieldName)
			}
		}
	}

	return mapping.NewEntityMapping(
		entityType,
		config.Schema,
		config.Table,
		fieldMap,
		childMap,
		allFields,
		primaryKey,
		parentalKey,
		insertable,
		updatable,
	)
}

// MappingOption is a function that configures an EntityMapping.
type MappingOption func(*mapping.EntityMapping)

// WithSchema sets the database schema for the entity's table.
func WithSchema(schema string) MappingOption {
	return func(m *mapping.EntityMapping) {
		m.Schema = schema
	}
}

// WithTable sets the table name for the entity.
// If not specified, defaults to snake_case of the struct name.
func WithTable(table string) MappingOption {
	return func(m *mapping.EntityMapping) {
		m.Table = table
	}
}

// WithPrimaryKey specifies the field names that form the primary key.
// If not specified, defaults to the "ID" field.
func WithPrimaryKey(fieldNames ...string) MappingOption {
	return func(m *mapping.EntityMapping) {
		m.PrimaryKey = fieldNames
	}
}
