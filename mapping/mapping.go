// Package mapping provides the entity mapping metadata types used by orm1.
//
// This package defines the core types that describe how Go struct types
// map to database tables, including field mappings, relationships, and
// key configurations.
//
// Most users will not need to use this package directly, as the orm1
// package provides higher-level APIs through SessionFactory and Session.
package mapping

import (
	"reflect"
)

// Field represents a mapping between a struct field and a database column.
type Field struct {
	Name       string
	Column     string
	Type       reflect.Type
	ByteOffset uintptr
}

// GetPtr returns a pointer to the field value in the given entity.
func (f *Field) GetPtr(entityPtr any) any {
	return getFieldPtr(entityPtr, f.Type, f.ByteOffset)
}

// GetValue returns the field value from the given entity.
func (f *Field) GetValue(entityPtr any) any {
	return getFieldValue(entityPtr, f.Type, f.ByteOffset)
}

// SetValue sets the field value on the given entity.
func (f *Field) SetValue(entityPtr any, value any) {
	setFieldValue(entityPtr, f.Type, f.ByteOffset, value)
}

// Child represents a parent-child relationship between entities.
// Can be either singular (*Child) or plural ([]*Child).
type Child struct {
	Target     reflect.Type
	Singular   bool
	Type       reflect.Type
	ByteOffset uintptr
}

// Get returns the child entities from the given parent entity.
// Returns a slice of child entities, even if the relationship is singular.
func (c *Child) Get(entityPtr any) []any {
	if c.Singular {
		value := getPtrFieldValue(entityPtr, c.Type, c.ByteOffset)
		if value == nil {
			return []any{}
		}
		return []any{value}
	} else {
		return getSliceFieldValues(entityPtr, c.Type, c.ByteOffset)
	}
}

// Set assigns child entities to the given parent entity.
// For singular relationships, sets the first child or nil.
// For plural relationships, sets the slice of children.
func (c *Child) Set(entityPtr any, children []any) {
	if c.Singular {
		if len(children) > 0 {
			setPtrFieldValue(entityPtr, c.Type, c.ByteOffset, children[0])
		} else {
			setPtrFieldValue(entityPtr, c.Type, c.ByteOffset, nil)
		}
	} else {
		setSliceFieldValues(entityPtr, c.Type, c.ByteOffset, children)
	}
}

// EntityMapping defines how a struct type maps to a database table.
// It includes column mappings, relationships, and key configurations.
// All column lists are pre-computed at initialization for zero-allocation runtime access.
// EntityMapping instances should only be created via NewEntityMapping.
type EntityMapping struct {
	EntityType  reflect.Type
	Schema      string
	Table       string
	FieldMap    map[string]*Field
	ChildMap    map[string]*Child
	AllFields   []string
	PrimaryKey  []string
	ParentalKey []string
	Insertable  []string
	Updatable   []string

	// Pre-computed column lists (private, immutable after initialization)
	allColumns             []string
	primaryColumns         []string
	parentalColumns        []string
	insertableColumns      []string
	updatableColumns       []string
	insertReturningColumns []string
	updateReturningColumns []string

	// Pre-computed field name lists for RETURNING operations
	insertReturning []string
	updateReturning []string
}

// NewEntityMapping creates a new EntityMapping with pre-computed column lists.
// This is the only way to create a valid EntityMapping instance.
func NewEntityMapping(
	entityType reflect.Type,
	schema string,
	table string,
	fieldMap map[string]*Field,
	childMap map[string]*Child,
	allFields []string,
	primaryKey []string,
	parentalKey []string,
	insertable []string,
	updatable []string,
) *EntityMapping {
	em := &EntityMapping{
		EntityType:  entityType,
		Schema:      schema,
		Table:       table,
		FieldMap:    fieldMap,
		ChildMap:    childMap,
		AllFields:   allFields,
		PrimaryKey:  primaryKey,
		ParentalKey: parentalKey,
		Insertable:  insertable,
		Updatable:   updatable,
	}

	em.allColumns = ComputeColumns(allFields, fieldMap)
	em.primaryColumns = ComputeColumns(primaryKey, fieldMap)
	em.parentalColumns = ComputeColumns(parentalKey, fieldMap)
	em.insertableColumns = ComputeColumns(insertable, fieldMap)
	em.updatableColumns = ComputeColumns(updatable, fieldMap)
	em.insertReturningColumns = computeInsertReturning(allFields, insertable, fieldMap)
	em.updateReturningColumns = computeUpdateReturning(allFields, updatable, primaryKey, fieldMap)

	em.insertReturning = computeFieldDifference(allFields, insertable)

	excludeFields := make([]string, 0, len(updatable)+len(primaryKey))
	excludeFields = append(excludeFields, updatable...)
	excludeFields = append(excludeFields, primaryKey...)
	em.updateReturning = computeFieldDifference(allFields, excludeFields)

	return em
}

// Columns returns all column names (pre-computed, zero allocation).
func (em *EntityMapping) Columns() []string { return em.allColumns }

// PrimaryColumns returns primary key column names (pre-computed, zero allocation).
func (em *EntityMapping) PrimaryColumns() []string { return em.primaryColumns }

// ParentalColumns returns parental key column names (pre-computed, zero allocation).
func (em *EntityMapping) ParentalColumns() []string { return em.parentalColumns }

// InsertableColumns returns insertable column names (pre-computed, zero allocation).
func (em *EntityMapping) InsertableColumns() []string { return em.insertableColumns }

// UpdatableColumns returns updatable column names (pre-computed, zero allocation).
func (em *EntityMapping) UpdatableColumns() []string { return em.updatableColumns }

// InsertReturningColumns returns columns that need to be returned after INSERT.
// These are auto-generated columns (AllFields - Insertable).
func (em *EntityMapping) InsertReturningColumns() []string { return em.insertReturningColumns }

// UpdateReturningColumns returns columns that need to be returned after UPDATE.
// These are auto-generated columns excluding primary keys (AllFields - Updatable - PrimaryKey).
func (em *EntityMapping) UpdateReturningColumns() []string { return em.updateReturningColumns }

// InsertReturning returns field names that need to be returned after INSERT (pre-computed, zero allocation).
// These correspond to InsertReturningColumns but as field names, not column names.
func (em *EntityMapping) InsertReturning() []string { return em.insertReturning }

// UpdateReturning returns field names that need to be returned after UPDATE (pre-computed, zero allocation).
// These correspond to UpdateReturningColumns but as field names, not column names.
func (em *EntityMapping) UpdateReturning() []string { return em.updateReturning }

// ComputeColumns builds a column name list from field names.
// This is called during EntityMapping initialization to pre-compute column lists.
func ComputeColumns(fieldNames []string, fieldMap map[string]*Field) []string {
	columns := make([]string, 0, len(fieldNames))
	for _, name := range fieldNames {
		columns = append(columns, fieldMap[name].Column)
	}
	return columns
}

// computeFieldDifference returns fields in allFields that are not in excludeFields.
// This is a common pattern for computing RETURNING field lists.
func computeFieldDifference(allFields, excludeFields []string) []string {
	excludeSet := make(map[string]bool, len(excludeFields))
	for _, f := range excludeFields {
		excludeSet[f] = true
	}

	result := make([]string, 0, len(allFields)-len(excludeFields))
	for _, f := range allFields {
		if !excludeSet[f] {
			result = append(result, f)
		}
	}
	return result
}

// computeInsertReturning computes columns needed for INSERT RETURNING.
// Logic: AllFields - Insertable = auto-generated columns only
func computeInsertReturning(allFields, insertable []string, fieldMap map[string]*Field) []string {
	fields := computeFieldDifference(allFields, insertable)
	columns := make([]string, len(fields))
	for i, f := range fields {
		columns[i] = fieldMap[f].Column
	}
	return columns
}

// computeUpdateReturning computes columns needed for UPDATE RETURNING.
// Logic: AllFields - Updatable - PrimaryKey = auto-generated columns only (excluding PKs)
func computeUpdateReturning(allFields, updatable, primaryKey []string, fieldMap map[string]*Field) []string {
	excludeFields := make([]string, 0, len(updatable)+len(primaryKey))
	excludeFields = append(excludeFields, updatable...)
	excludeFields = append(excludeFields, primaryKey...)

	fields := computeFieldDifference(allFields, excludeFields)
	columns := make([]string, len(fields))
	for i, f := range fields {
		columns[i] = fieldMap[f].Column
	}
	return columns
}
