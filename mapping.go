// Package mapping provides the entity mapping metadata types used by orm1.
//
// This package defines the core types that describe how Go struct types
// map to database tables, including field mappings, relationships, and
// key configurations.
//
// Most users will not need to use this package directly, as the orm1
// package provides higher-level APIs through SessionFactory and Session.
package orm1

import (
	"reflect"
)

// Field represents a mapping between a struct field and a database column.
type Field struct {
	Name       string
	Column     string
	Type       reflect.Type
	FieldIndex int // Index of the field in the struct for direct reflect access
}

// GetPtr returns a pointer to the field value in the given entity.
func (f *Field) GetPtr(entityPtr any) any {
	return getFieldPtr(entityPtr, f.FieldIndex)
}

// GetValue returns the field value from the given entity.
func (f *Field) GetValue(entityPtr any) any {
	return getFieldValue(entityPtr, f.FieldIndex)
}

// SetValue sets the field value on the given entity.
func (f *Field) SetValue(entityPtr any, value any) {
	setFieldValue(entityPtr, f.FieldIndex, value)
}

// Child represents a parent-child relationship between entities.
// Can be either singular (*Child) or plural ([]*Child).
type Child struct {
	Target     reflect.Type
	Singular   bool
	Type       reflect.Type
	FieldIndex int // Index of the field in the struct for direct reflect access
}

// Get returns the child entities from the given parent entity.
// Returns a slice of child entities, even if the relationship is singular.
func (c *Child) Get(entityPtr any) []any {
	if c.Singular {
		value := getPtrFieldValue(entityPtr, c.Type, c.FieldIndex)
		if value == nil {
			return []any{}
		}
		return []any{value}
	} else {
		return getSliceFieldValues(entityPtr, c.Type, c.FieldIndex)
	}
}

// Set assigns child entities to the given parent entity.
// For singular relationships, sets the first child or nil.
// For plural relationships, sets the slice of children.
func (c *Child) Set(entityPtr any, children []any) {
	if c.Singular {
		if len(children) > 0 {
			setPtrFieldValue(entityPtr, c.Type, c.FieldIndex, children[0])
		} else {
			setPtrFieldValue(entityPtr, c.Type, c.FieldIndex, nil)
		}
	} else {
		setSliceFieldValues(entityPtr, c.Type, c.FieldIndex, children)
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

	// Pre-computed field name lists for RETURNING operations
	insertReturning []string
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
	em.insertReturningColumns = computeInsertReturning(allFields, insertable, primaryKey, fieldMap)

	// Compute field names for INSERT RETURNING (primary key + auto fields)
	insertReturningSet := make(map[string]bool)
	for _, pk := range primaryKey {
		insertReturningSet[pk] = true
	}
	autoFields := computeFieldDifference(allFields, insertable)
	for _, f := range autoFields {
		insertReturningSet[f] = true
	}
	em.insertReturning = make([]string, 0, len(insertReturningSet))
	for _, pk := range primaryKey {
		if insertReturningSet[pk] {
			em.insertReturning = append(em.insertReturning, pk)
			delete(insertReturningSet, pk)
		}
	}
	for _, f := range autoFields {
		if insertReturningSet[f] {
			em.insertReturning = append(em.insertReturning, f)
		}
	}

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
// Always includes primary key plus any auto-generated columns.
func (em *EntityMapping) InsertReturningColumns() []string { return em.insertReturningColumns }

// InsertReturning returns field names that need to be returned after INSERT (pre-computed, zero allocation).
// These correspond to InsertReturningColumns but as field names, not column names.
func (em *EntityMapping) InsertReturning() []string { return em.insertReturning }

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
// Logic: PrimaryKey + (AllFields - Insertable)
// Always returns primary key columns plus any auto-generated fields.
func computeInsertReturning(allFields, insertable, primaryKey []string, fieldMap map[string]*Field) []string {
	// Start with primary key fields
	fieldSet := make(map[string]bool, len(primaryKey))
	for _, pk := range primaryKey {
		fieldSet[pk] = true
	}

	// Add auto-generated fields (fields not in insertable)
	autoFields := computeFieldDifference(allFields, insertable)
	for _, f := range autoFields {
		fieldSet[f] = true
	}

	// Convert set to ordered slice (primary key first, then auto fields)
	fields := make([]string, 0, len(fieldSet))
	for _, pk := range primaryKey {
		if fieldSet[pk] {
			fields = append(fields, pk)
			delete(fieldSet, pk) // Remove to avoid duplicates
		}
	}
	for _, f := range autoFields {
		if fieldSet[f] {
			fields = append(fields, f)
		}
	}

	columns := make([]string, len(fields))
	for i, f := range fields {
		columns[i] = fieldMap[f].Column
	}
	return columns
}

// getFieldPtr returns a pointer to a struct field value using reflect.
// structPtr must be a pointer to a struct.
// fieldIndex is the index of the field in the struct.
func getFieldPtr(structPtr any, fieldIndex int) any {
	sv := reflect.ValueOf(structPtr).Elem()
	return sv.Field(fieldIndex).Addr().Interface()
}

// getFieldValue returns a struct field value using reflect.
// structPtr must be a pointer to a struct.
// fieldIndex is the index of the field in the struct.
func getFieldValue(structPtr any, fieldIndex int) any {
	sv := reflect.ValueOf(structPtr).Elem()
	return sv.Field(fieldIndex).Interface()
}

// setFieldValue sets a struct field value using reflect.
// structPtr must be a pointer to a struct.
// fieldIndex is the index of the field in the struct.
// value is the new value to set.
func setFieldValue(structPtr any, fieldIndex int, value any) {
	sv := reflect.ValueOf(structPtr).Elem()
	sv.Field(fieldIndex).Set(reflect.ValueOf(value))
}

// getPtrFieldValue returns the value of a pointer field (*T) using reflect.
// structPtr must be a pointer to a struct.
// ptrFieldType is the reflect.Type of the field (must be *T).
// fieldIndex is the index of the field in the struct.
// Returns the pointer value, or nil if the pointer is nil.
// Note: Returns untyped nil (not typed nil like (*T)(nil)).
func getPtrFieldValue(structPtr any, ptrFieldType reflect.Type, fieldIndex int) any {
	sv := reflect.ValueOf(structPtr).Elem()
	fieldPtr := sv.Field(fieldIndex)
	// Check for nil pointer and return untyped nil
	if fieldPtr.IsNil() {
		return nil
	}
	return fieldPtr.Interface()
}

// getSliceFieldValues returns the values from a slice field ([]*T or []T) using reflect.
// structPtr must be a pointer to a struct.
// sliceFieldType is the reflect.Type of the field (must be slice type).
// fieldIndex is the index of the field in the struct.
// Returns []any containing all slice elements (requires reflect for type conversion).
func getSliceFieldValues(structPtr any, sliceFieldType reflect.Type, fieldIndex int) []any {
	sv := reflect.ValueOf(structPtr).Elem()
	fieldPtr := sv.Field(fieldIndex)
	sliceLen := fieldPtr.Len()
	result := make([]any, sliceLen)
	for j := 0; j < sliceLen; j++ {
		result[j] = fieldPtr.Index(j).Interface()
	}
	return result
}

// setPtrFieldValue sets a pointer field (*T) to the given value using reflect.
// structPtr must be a pointer to a struct.
// ptrFieldType is the reflect.Type of the field (must be *T).
// fieldIndex is the index of the field in the struct.
// value is the value to set (may be nil).
func setPtrFieldValue(structPtr any, ptrFieldType reflect.Type, fieldIndex int, value any) {
	sv := reflect.ValueOf(structPtr).Elem()
	fieldPtr := sv.Field(fieldIndex)
	if value == nil {
		fieldPtr.Set(reflect.Zero(ptrFieldType))
	} else {
		fieldPtr.Set(reflect.ValueOf(value))
	}
}

// setSliceFieldValues sets a slice field ([]*T or []T) to the given values using reflect.
// structPtr must be a pointer to a struct.
// sliceFieldType is the reflect.Type of the field (must be slice type).
// fieldIndex is the index of the field in the struct.
// values is the slice of values to set (requires reflect for type conversion).
func setSliceFieldValues(structPtr any, sliceFieldType reflect.Type, fieldIndex int, values []any) {
	sv := reflect.ValueOf(structPtr).Elem()
	fieldPtr := sv.Field(fieldIndex)
	sliceVal := reflect.MakeSlice(sliceFieldType, len(values), len(values))
	for j, val := range values {
		sliceVal.Index(j).Set(reflect.ValueOf(val))
	}
	fieldPtr.Set(sliceVal)
}
