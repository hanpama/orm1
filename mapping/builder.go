package mapping

import (
	"reflect"
)

// EntityMappingBuilder constructs EntityMapping instances from struct types.
// It analyzes struct fields to determine columns, relationships, and keys.
type EntityMappingBuilder struct {
	registered map[reflect.Type]bool
	configs    map[reflect.Type]*EntityConfig
	mappings   map[reflect.Type]*EntityMapping
	built      bool
}

// EntityConfig holds configuration for an entity mapping.
type EntityConfig struct {
	Schema      string
	Table       string
	PrimaryKey  []string
	ParentalKey []string
}

// NewEntityMappingBuilder creates a new builder.
func NewEntityMappingBuilder() *EntityMappingBuilder {
	return &EntityMappingBuilder{
		registered: make(map[reflect.Type]bool),
		configs:    make(map[reflect.Type]*EntityConfig),
		mappings:   make(map[reflect.Type]*EntityMapping),
	}
}

// Register adds an entity type to the builder with optional configuration.
func (b *EntityMappingBuilder) Register(entityType reflect.Type, opts ...MappingOption) {
	if entityType.Kind() != reflect.Struct {
		panic("entityType must be a struct type")
	}

	b.registered[entityType] = true

	config := &EntityConfig{
		Schema:      "",
		Table:       ToSnakeCase(entityType.Name()),
		PrimaryKey:  nil,
		ParentalKey: nil,
	}
	b.configs[entityType] = config

	tempMapping := &EntityMapping{
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
func (b *EntityMappingBuilder) Build() map[reflect.Type]*EntityMapping {
	if b.built {
		return b.mappings
	}

	for entityType := range b.registered {
		if _, ok := b.mappings[entityType]; !ok {
			mapping := b.buildMapping(entityType)
			b.mappings[entityType] = mapping
		}
	}

	b.built = true

	return b.mappings
}

// GetMapping returns the mapping for a specific entity type.
// Returns nil if the type is not registered or not yet built.
func (b *EntityMappingBuilder) GetMapping(entityType reflect.Type) *EntityMapping {
	return b.mappings[entityType]
}

// buildMapping constructs an EntityMapping for a single entity type.
func (b *EntityMappingBuilder) buildMapping(entityType reflect.Type) *EntityMapping {
	config := b.configs[entityType]

	fieldMap := make(map[string]*Field)
	childMap := make(map[string]*Child)
	allFields := []string{}
	primaryKey := []string{}
	parentalKey := []string{}
	insertable := []string{}
	updatable := []string{}

	fieldsMetadata := AnalyzeStruct(entityType)

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
					if b.registered[targetType] {
						isChild = true
						childTarget = targetType
						childSingular = false
					}
				}
			}

			if !isChild && fieldType.Kind() == reflect.Ptr {
				targetType := fieldType.Elem()
				if b.registered[targetType] {
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
			child := Child{
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

		field := Field{
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

	return NewEntityMapping(
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
type MappingOption func(*EntityMapping)

// WithSchema sets the database schema for the entity's table.
func WithSchema(schema string) MappingOption {
	return func(m *EntityMapping) {
		m.Schema = schema
	}
}

// WithTable sets the table name for the entity.
// If not specified, defaults to snake_case of the struct name.
func WithTable(table string) MappingOption {
	return func(m *EntityMapping) {
		m.Table = table
	}
}

// WithPrimaryKey specifies the field names that form the primary key.
// If not specified, defaults to the "ID" field.
func WithPrimaryKey(fieldNames ...string) MappingOption {
	return func(m *EntityMapping) {
		m.PrimaryKey = fieldNames
	}
}
