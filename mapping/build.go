package mapping

import "reflect"

// EntityMetadata contains all configuration needed to build an EntityMapping.
// This is a pure data structure with no dependencies on the orm1 package.
type EntityMetadata struct {
	Schema string
	Table  string
	Fields []FieldMetadata // All fields analyzed from the struct
}

// BuildEntityMappings constructs EntityMapping instances from metadata.
// This function is the core builder that transforms metadata into entity mappings.
//
// The build process:
//  1. Analyze each entity's struct fields using AnalyzeStruct
//  2. Detect child relationships based on registered types
//  3. Construct field maps, child maps, and key configurations
//  4. Create EntityMapping instances
//
// Parameters:
//   - meta: Map of entity types to their metadata
//   - registered: Set of all registered entity types (for child detection)
//
// Returns: Map of entity types to their complete EntityMapping instances
func BuildEntityMappings(
	meta map[reflect.Type]EntityMetadata,
	registered map[reflect.Type]bool,
) map[reflect.Type]*EntityMapping {
	result := make(map[reflect.Type]*EntityMapping, len(meta))

	for entityType, entityMeta := range meta {
		mapping := buildSingleMapping(entityType, entityMeta, registered)
		result[entityType] = mapping
	}

	return result
}

// buildSingleMapping constructs an EntityMapping for a single entity type.
func buildSingleMapping(
	entityType reflect.Type,
	entityMeta EntityMetadata,
	registered map[reflect.Type]bool,
) *EntityMapping {
	fieldMap := make(map[string]*Field)
	childMap := make(map[string]*Child)
	allFields := []string{}
	primaryKey := []string{}
	parentalKey := []string{}
	insertable := []string{}
	updatable := []string{}

	for _, metadata := range entityMeta.Fields {
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
					if registered[targetType] {
						isChild = true
						childTarget = targetType
						childSingular = false
					}
				}
			}

			if !isChild && fieldType.Kind() == reflect.Ptr {
				targetType := fieldType.Elem()
				if registered[targetType] {
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
				FieldIndex: metadata.FieldIndex,
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
			FieldIndex: metadata.FieldIndex,
		}

		fieldMap[fieldName] = &field

		isPrimaryKey := metadata.PrimaryTag
		isParentalKey := metadata.ParentalTag

		// Default: ID field is primary key if no explicit primary tag
		if !isPrimaryKey && !isParentalKey && fieldName == "ID" {
			isPrimaryKey = true
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
		entityMeta.Schema,
		entityMeta.Table,
		fieldMap,
		childMap,
		allFields,
		primaryKey,
		parentalKey,
		insertable,
		updatable,
	)
}
