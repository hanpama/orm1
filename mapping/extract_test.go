package mapping_test

import (
	"reflect"
	"testing"

	"github.com/hanpama/orm1/key"
	"github.com/hanpama/orm1/mapping"
)

// Test entities for ExtractKey
type Entity0 struct {
	Name string
}

type Entity1 struct {
	ID   int64 `orm1:"primary"`
	Name string
}

type Entity2 struct {
	ID       int64 `orm1:"primary"`
	TenantID int64 `orm1:"primary"`
	Name     string
}

type Entity3 struct {
	ID     int64 `orm1:"primary"`
	UserID int64 `orm1:"primary"`
	ShopID int64 `orm1:"primary"`
	Name   string
}

type Entity4 struct {
	F1   int64 `orm1:"primary"`
	F2   int64 `orm1:"primary"`
	F3   int64 `orm1:"primary"`
	F4   int64 `orm1:"primary"`
	Name string
}

type Entity5 struct {
	F1   int64 `orm1:"primary"`
	F2   int64 `orm1:"primary"`
	F3   int64 `orm1:"primary"`
	F4   int64 `orm1:"primary"`
	F5   int64 `orm1:"primary"`
	Name string
}

type Entity6 struct {
	F1   int64 `orm1:"primary"`
	F2   int64 `orm1:"primary"`
	F3   int64 `orm1:"primary"`
	F4   int64 `orm1:"primary"`
	F5   int64 `orm1:"primary"`
	F6   int64 `orm1:"primary"`
	Name string
}

type Entity7 struct {
	F1   int64 `orm1:"primary"`
	F2   int64 `orm1:"primary"`
	F3   int64 `orm1:"primary"`
	F4   int64 `orm1:"primary"`
	F5   int64 `orm1:"primary"`
	F6   int64 `orm1:"primary"`
	F7   int64 `orm1:"primary"`
	Name string
}

type Entity8 struct {
	F1   int64 `orm1:"primary"`
	F2   int64 `orm1:"primary"`
	F3   int64 `orm1:"primary"`
	F4   int64 `orm1:"primary"`
	F5   int64 `orm1:"primary"`
	F6   int64 `orm1:"primary"`
	F7   int64 `orm1:"primary"`
	F8   int64 `orm1:"primary"`
	Name string
}

type Entity9 struct {
	F1   int64 `orm1:"primary"`
	F2   int64 `orm1:"primary"`
	F3   int64 `orm1:"primary"`
	F4   int64 `orm1:"primary"`
	F5   int64 `orm1:"primary"`
	F6   int64 `orm1:"primary"`
	F7   int64 `orm1:"primary"`
	F8   int64 `orm1:"primary"`
	F9   int64 `orm1:"primary"`
	Name string
}

// TestExtractKey_AllArities tests ExtractKey with 0-9 fields
func TestExtractKey_AllArities(t *testing.T) {
	tests := []struct {
		name       string
		entityType reflect.Type
		entity     any
		fieldNames []string
		verify     func(t *testing.T, result key.Key)
	}{
		{
			name:       "0 fields",
			entityType: reflect.TypeOf(Entity0{}),
			entity:     &Entity0{Name: "test"},
			fieldNames: []string{},
			verify: func(t *testing.T, result key.Key) {
				if result.Length() != 0 {
					t.Errorf("Key length = %d, want 0", result.Length())
				}
			},
		},
		{
			name:       "1 field",
			entityType: reflect.TypeOf(Entity1{}),
			entity:     &Entity1{ID: 123, Name: "test"},
			fieldNames: []string{"ID"},
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
			entityType: reflect.TypeOf(Entity2{}),
			entity:     &Entity2{ID: 123, TenantID: 456, Name: "test"},
			fieldNames: []string{"ID", "TenantID"},
			verify: func(t *testing.T, result key.Key) {
				if result.Length() != 2 {
					t.Errorf("Key length = %d, want 2", result.Length())
				}
				if result.At(0) != int64(123) || result.At(1) != int64(456) {
					t.Errorf("Key = [%v, %v], want [123, 456]", result.At(0), result.At(1))
				}
			},
		},
		{
			name:       "3 fields",
			entityType: reflect.TypeOf(Entity3{}),
			entity:     &Entity3{ID: 1, UserID: 2, ShopID: 3, Name: "test"},
			fieldNames: []string{"ID", "UserID", "ShopID"},
			verify: func(t *testing.T, result key.Key) {
				if result.Length() != 3 {
					t.Errorf("Key length = %d, want 3", result.Length())
				}
				if result.At(0) != int64(1) || result.At(1) != int64(2) || result.At(2) != int64(3) {
					t.Errorf("Key = [%v, %v, %v], want [1, 2, 3]", result.At(0), result.At(1), result.At(2))
				}
			},
		},
		{
			name:       "4 fields",
			entityType: reflect.TypeOf(Entity4{}),
			entity:     &Entity4{F1: 1, F2: 2, F3: 3, F4: 4, Name: "test"},
			fieldNames: []string{"F1", "F2", "F3", "F4"},
			verify: func(t *testing.T, result key.Key) {
				if result.Length() != 4 {
					t.Errorf("Key length = %d, want 4", result.Length())
				}
			},
		},
		{
			name:       "5 fields",
			entityType: reflect.TypeOf(Entity5{}),
			entity:     &Entity5{F1: 1, F2: 2, F3: 3, F4: 4, F5: 5, Name: "test"},
			fieldNames: []string{"F1", "F2", "F3", "F4", "F5"},
			verify: func(t *testing.T, result key.Key) {
				if result.Length() != 5 {
					t.Errorf("Key length = %d, want 5", result.Length())
				}
			},
		},
		{
			name:       "6 fields",
			entityType: reflect.TypeOf(Entity6{}),
			entity:     &Entity6{F1: 1, F2: 2, F3: 3, F4: 4, F5: 5, F6: 6, Name: "test"},
			fieldNames: []string{"F1", "F2", "F3", "F4", "F5", "F6"},
			verify: func(t *testing.T, result key.Key) {
				if result.Length() != 6 {
					t.Errorf("Key length = %d, want 6", result.Length())
				}
			},
		},
		{
			name:       "7 fields",
			entityType: reflect.TypeOf(Entity7{}),
			entity:     &Entity7{F1: 1, F2: 2, F3: 3, F4: 4, F5: 5, F6: 6, F7: 7, Name: "test"},
			fieldNames: []string{"F1", "F2", "F3", "F4", "F5", "F6", "F7"},
			verify: func(t *testing.T, result key.Key) {
				if result.Length() != 7 {
					t.Errorf("Key length = %d, want 7", result.Length())
				}
			},
		},
		{
			name:       "8 fields",
			entityType: reflect.TypeOf(Entity8{}),
			entity:     &Entity8{F1: 1, F2: 2, F3: 3, F4: 4, F5: 5, F6: 6, F7: 7, F8: 8, Name: "test"},
			fieldNames: []string{"F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8"},
			verify: func(t *testing.T, result key.Key) {
				if result.Length() != 8 {
					t.Errorf("Key length = %d, want 8", result.Length())
				}
			},
		},
		{
			name:       "9 fields",
			entityType: reflect.TypeOf(Entity9{}),
			entity:     &Entity9{F1: 1, F2: 2, F3: 3, F4: 4, F5: 5, F6: 6, F7: 7, F8: 8, F9: 9, Name: "test"},
			fieldNames: []string{"F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "F9"},
			verify: func(t *testing.T, result key.Key) {
				if result.Length() != 9 {
					t.Errorf("Key length = %d, want 9", result.Length())
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := mapping.NewEntityMappingBuilder()
			builder.Register(tt.entityType)
			mappings := builder.Build()

			em := mappings[tt.entityType]
			result := em.ExtractKey(tt.entity, tt.fieldNames)

			tt.verify(t, result)
		})
	}
}

// TestExtractKey_PrimaryKey tests ExtractKey with PrimaryKey fields
func TestExtractKey_PrimaryKey(t *testing.T) {
	user := &Entity1{ID: 999, Name: "Alice"}

	builder := mapping.NewEntityMappingBuilder()
	builder.Register(reflect.TypeOf(Entity1{}))
	mappings := builder.Build()
	em := mappings[reflect.TypeOf(Entity1{})]

	pk := em.ExtractKey(user, em.PrimaryKey)

	if pk.Length() != 1 {
		t.Errorf("Primary key length = %d, want 1", pk.Length())
	}

	if pk.At(0) != int64(999) {
		t.Errorf("Primary key value = %v, want 999", pk.At(0))
	}
}

// TestExtractKey_CompositeKey tests ExtractKey with composite keys
func TestExtractKey_CompositeKey(t *testing.T) {
	entity := &Entity2{ID: 100, TenantID: 200, Name: "test"}

	builder := mapping.NewEntityMappingBuilder()
	builder.Register(reflect.TypeOf(Entity2{}))
	mappings := builder.Build()
	em := mappings[reflect.TypeOf(Entity2{})]

	result := em.ExtractKey(entity, []string{"ID", "TenantID"})

	if result.Length() != 2 {
		t.Errorf("Key length = %d, want 2", result.Length())
	}

	if result.At(0) != int64(100) || result.At(1) != int64(200) {
		t.Errorf("Key = [%v, %v], want [100, 200]", result.At(0), result.At(1))
	}
}

// TestExtractKey_DifferentTypes tests ExtractKey with different field types
func TestExtractKey_DifferentTypes(t *testing.T) {
	type MixedEntity struct {
		IntField    int64  `orm1:"primary"`
		StringField string `orm1:"primary"`
		BoolField   bool
	}

	entity := &MixedEntity{
		IntField:    123,
		StringField: "abc",
		BoolField:   true,
	}

	builder := mapping.NewEntityMappingBuilder()
	builder.Register(reflect.TypeOf(MixedEntity{}))
	mappings := builder.Build()
	em := mappings[reflect.TypeOf(MixedEntity{})]

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

// TestExtractKey_PanicOn10Fields tests ExtractKey panics with 10+ fields
func TestExtractKey_PanicOn10Fields(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic for 10 fields, but didn't panic")
		}
	}()

	type Entity10 struct {
		F1, F2, F3, F4, F5, F6, F7, F8, F9, F10 int64
	}

	entity := &Entity10{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	builder := mapping.NewEntityMappingBuilder()
	builder.Register(reflect.TypeOf(Entity10{}))
	mappings := builder.Build()
	em := mappings[reflect.TypeOf(Entity10{})]

	// This should panic
	em.ExtractKey(entity, []string{"F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "F9", "F10"})
}

// TestExtractKey_KeyEquality tests that extracted keys can be compared
func TestExtractKey_KeyEquality(t *testing.T) {
	entity1 := &Entity2{ID: 100, TenantID: 200}
	entity2 := &Entity2{ID: 100, TenantID: 200}
	entity3 := &Entity2{ID: 100, TenantID: 999}

	builder := mapping.NewEntityMappingBuilder()
	builder.Register(reflect.TypeOf(Entity2{}))
	mappings := builder.Build()
	em := mappings[reflect.TypeOf(Entity2{})]

	key1 := em.ExtractKey(entity1, em.PrimaryKey)
	key2 := em.ExtractKey(entity2, em.PrimaryKey)
	key3 := em.ExtractKey(entity3, em.PrimaryKey)

	// Same values should produce equal keys
	if key1 != key2 {
		t.Errorf("Keys with same values should be equal")
	}

	// Different values should produce different keys
	if key1 == key3 {
		t.Errorf("Keys with different values should not be equal")
	}
}
