package mapping_test

import (
	"reflect"
	"testing"

	"github.com/hanpama/orm1/key"
	"github.com/hanpama/orm1/mapping"
)

// Entity is a generic test entity with flexible fields
type Entity struct {
	F1   int64
	F2   int64
	F3   int64
	F4   int64
	F5   int64
	F6   int64
	F7   int64
	F8   int64
	F9   int64
	Name string
}

// TestExtractKey_AllArities tests ExtractKey with 0-9 fields
func TestExtractKey_AllArities(t *testing.T) {
	tests := []struct {
		name       string
		fieldNames []string
		entity     *Entity
		verify     func(t *testing.T, result key.Key)
	}{
		{
			name:       "0 fields",
			fieldNames: []string{},
			entity:     &Entity{Name: "test"},
			verify: func(t *testing.T, result key.Key) {
				if result.Length() != 0 {
					t.Errorf("Key length = %d, want 0", result.Length())
				}
			},
		},
		{
			name:       "1 field",
			fieldNames: []string{"F1"},
			entity:     &Entity{F1: 123, Name: "test"},
			verify: func(t *testing.T, result key.Key) {
				if result.Length() != 1 {
					t.Errorf("Key length = %d, want 1", result.Length())
				}
				if result.At(0) != int64(123) {
					t.Errorf("Key[0] = %v, want 123", result.At(0))
				}
			},
		},
		{
			name:       "2 fields",
			fieldNames: []string{"F1", "F2"},
			entity:     &Entity{F1: 100, F2: 200, Name: "test"},
			verify: func(t *testing.T, result key.Key) {
				if result.Length() != 2 {
					t.Errorf("Key length = %d, want 2", result.Length())
				}
				if result.At(0) != int64(100) {
					t.Errorf("Key[0] = %v, want 100", result.At(0))
				}
				if result.At(1) != int64(200) {
					t.Errorf("Key[1] = %v, want 200", result.At(1))
				}
			},
		},
		{
			name:       "3 fields",
			fieldNames: []string{"F1", "F2", "F3"},
			entity:     &Entity{F1: 1, F2: 2, F3: 3},
			verify: func(t *testing.T, result key.Key) {
				if result.Length() != 3 {
					t.Errorf("Key length = %d, want 3", result.Length())
				}
			},
		},
		{
			name:       "4 fields",
			fieldNames: []string{"F1", "F2", "F3", "F4"},
			entity:     &Entity{F1: 1, F2: 2, F3: 3, F4: 4},
			verify: func(t *testing.T, result key.Key) {
				if result.Length() != 4 {
					t.Errorf("Key length = %d, want 4", result.Length())
				}
			},
		},
		{
			name:       "5 fields",
			fieldNames: []string{"F1", "F2", "F3", "F4", "F5"},
			entity:     &Entity{F1: 1, F2: 2, F3: 3, F4: 4, F5: 5},
			verify: func(t *testing.T, result key.Key) {
				if result.Length() != 5 {
					t.Errorf("Key length = %d, want 5", result.Length())
				}
			},
		},
		{
			name:       "6 fields",
			fieldNames: []string{"F1", "F2", "F3", "F4", "F5", "F6"},
			entity:     &Entity{F1: 1, F2: 2, F3: 3, F4: 4, F5: 5, F6: 6},
			verify: func(t *testing.T, result key.Key) {
				if result.Length() != 6 {
					t.Errorf("Key length = %d, want 6", result.Length())
				}
			},
		},
		{
			name:       "7 fields",
			fieldNames: []string{"F1", "F2", "F3", "F4", "F5", "F6", "F7"},
			entity:     &Entity{F1: 1, F2: 2, F3: 3, F4: 4, F5: 5, F6: 6, F7: 7},
			verify: func(t *testing.T, result key.Key) {
				if result.Length() != 7 {
					t.Errorf("Key length = %d, want 7", result.Length())
				}
			},
		},
		{
			name:       "8 fields",
			fieldNames: []string{"F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8"},
			entity:     &Entity{F1: 1, F2: 2, F3: 3, F4: 4, F5: 5, F6: 6, F7: 7, F8: 8},
			verify: func(t *testing.T, result key.Key) {
				if result.Length() != 8 {
					t.Errorf("Key length = %d, want 8", result.Length())
				}
			},
		},
		{
			name:       "9 fields",
			fieldNames: []string{"F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "F9"},
			entity:     &Entity{F1: 1, F2: 2, F3: 3, F4: 4, F5: 5, F6: 6, F7: 7, F8: 8, F9: 9},
			verify: func(t *testing.T, result key.Key) {
				if result.Length() != 9 {
					t.Errorf("Key length = %d, want 9", result.Length())
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Manually create EntityMapping
			em := createEntityMapping(tt.fieldNames)
			result := em.ExtractKey(tt.entity, tt.fieldNames)
			tt.verify(t, result)
		})
	}
}

// TestExtractKey_PrimaryKey tests ExtractKey with PrimaryKey fields
func TestExtractKey_PrimaryKey(t *testing.T) {
	entity := &Entity{F1: 999, Name: "Alice"}

	em := createEntityMapping([]string{"F1"})
	pk := em.ExtractKey(entity, []string{"F1"})

	if pk.Length() != 1 {
		t.Errorf("Primary key length = %d, want 1", pk.Length())
	}
	if pk.At(0) != int64(999) {
		t.Errorf("Primary key value = %v, want 999", pk.At(0))
	}
}

// TestExtractKey_CompositeKey tests ExtractKey with composite keys
func TestExtractKey_CompositeKey(t *testing.T) {
	entity := &Entity{F1: 100, F2: 200, Name: "test"}

	em := createEntityMapping([]string{"F1", "F2"})
	result := em.ExtractKey(entity, []string{"F1", "F2"})

	if result.Length() != 2 {
		t.Errorf("Key length = %d, want 2", result.Length())
	}
	if result.At(0) != int64(100) {
		t.Errorf("Key[0] = %v, want 100", result.At(0))
	}
	if result.At(1) != int64(200) {
		t.Errorf("Key[1] = %v, want 200", result.At(1))
	}
}

// TestExtractKey_MixedTypes tests ExtractKey with mixed field types
func TestExtractKey_MixedTypes(t *testing.T) {
	type MixedEntity struct {
		IntField    int64
		StringField string
		BoolField   bool
	}

	entity := &MixedEntity{
		IntField:    123,
		StringField: "abc",
		BoolField:   true,
	}

	fieldMap := map[string]*mapping.Field{
		"IntField":    {Name: "IntField", Column: "int_field", Type: reflect.TypeOf(int64(0)), ByteOffset: 0},
		"StringField": {Name: "StringField", Column: "string_field", Type: reflect.TypeOf(""), ByteOffset: 8},
	}

	em := mapping.NewEntityMapping(
		reflect.TypeOf(MixedEntity{}),
		"", "mixed_entity",
		fieldMap,
		map[string]*mapping.Child{},
		[]string{"IntField", "StringField"},
		[]string{"IntField", "StringField"},
		[]string{},
		[]string{"IntField", "StringField"},
		[]string{},
	)

	result := em.ExtractKey(entity, []string{"IntField", "StringField"})

	if result.Length() != 2 {
		t.Errorf("Key length = %d, want 2", result.Length())
	}
	if result.At(0) != int64(123) {
		t.Errorf("Key[0] = %v, want 123", result.At(0))
	}
	if result.At(1) != "abc" {
		t.Errorf("Key[1] = %v, want 'abc'", result.At(1))
	}
}

// TestExtractKey_PanicOnTooManyFields tests panic with >9 fields
func TestExtractKey_PanicOnTooManyFields(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic when extracting >9 fields")
		}
	}()

	type Entity10 struct {
		F1, F2, F3, F4, F5, F6, F7, F8, F9, F10 int64
	}

	entity := &Entity10{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	// Create mapping with 10 fields
	fieldMap := make(map[string]*mapping.Field)
	fieldNames := []string{"F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "F9", "F10"}
	for i, name := range fieldNames {
		fieldMap[name] = &mapping.Field{
			Name:       name,
			Column:     name,
			Type:       reflect.TypeOf(int64(0)),
			ByteOffset: uintptr(i * 8),
		}
	}

	em := mapping.NewEntityMapping(
		reflect.TypeOf(Entity10{}),
		"", "entity10",
		fieldMap,
		map[string]*mapping.Child{},
		fieldNames,
		fieldNames,
		[]string{},
		fieldNames,
		[]string{},
	)

	// This should panic
	em.ExtractKey(entity, fieldNames)
}

// TestExtractKey_KeyEquality tests that extracted keys can be compared
func TestExtractKey_KeyEquality(t *testing.T) {
	entity1 := &Entity{F1: 100, F2: 200}
	entity2 := &Entity{F1: 100, F2: 200}
	entity3 := &Entity{F1: 100, F2: 999}

	em := createEntityMapping([]string{"F1", "F2"})

	key1 := em.ExtractKey(entity1, []string{"F1", "F2"})
	key2 := em.ExtractKey(entity2, []string{"F1", "F2"})
	key3 := em.ExtractKey(entity3, []string{"F1", "F2"})

	// Key1 and Key2 should be equal (same values)
	if key1 != key2 {
		t.Errorf("key1 and key2 should be equal")
	}

	// Key1 and Key3 should not be equal (different F2)
	if key1 == key3 {
		t.Errorf("key1 and key3 should not be equal")
	}
}

// Helper function to create EntityMapping for test
func createEntityMapping(fieldNames []string) *mapping.EntityMapping {
	entityType := reflect.TypeOf(Entity{})

	// Analyze struct and mark specified fields as primary
	fields := mapping.AnalyzeStruct(entityType)

	// Override PrimaryTag for specified fields
	isPrimaryField := make(map[string]bool)
	for _, name := range fieldNames {
		isPrimaryField[name] = true
	}

	for i := range fields {
		if isPrimaryField[fields[i].Name] {
			fields[i].PrimaryTag = true
		}
	}

	// Use BuildEntityMappings to create the mapping
	metadata := map[reflect.Type]mapping.EntityMetadata{
		entityType: {
			Schema: "",
			Table:  "entity",
			Fields: fields,
		},
	}
	registered := map[reflect.Type]bool{
		entityType: true,
	}

	mappings := mapping.BuildEntityMappings(metadata, registered)
	return mappings[entityType]
}
