package mapping_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/hanpama/orm1/mapping"
)

// Test entities
type User struct {
	ID        int64  `orm1:"primary"`
	Name      string
	Email     string
	CreatedAt time.Time `orm1:"skip_insert"`
	UpdatedAt time.Time `orm1:"skip_update"`
	Version   int       `orm1:"skip_insert,skip_update"`
}

type Post struct {
	ID       int64  `orm1:"primary"`
	UserID   int64  `orm1:"parental"`
	Title    string
	Author   *User  `orm1:"child"`
	Comments []*Comment `orm1:"child"`
}

type Comment struct {
	ID     int64 `orm1:"primary"`
	PostID int64 `orm1:"parental"`
	Text   string
}

// buildTestMappings creates EntityMappings for testing without importing orm1.
func buildTestMappings(entityTypes ...reflect.Type) map[reflect.Type]*mapping.EntityMapping {
	metadata := make(map[reflect.Type]mapping.EntityMetadata)
	registered := make(map[reflect.Type]bool)

	for _, entityType := range entityTypes {
		registered[entityType] = true
		fields := mapping.AnalyzeStruct(entityType)
		metadata[entityType] = mapping.EntityMetadata{
			Schema: "",
			Table:  mapping.ToSnakeCase(entityType.Name()),
			Fields: fields,
		}
	}

	return mapping.BuildEntityMappings(metadata, registered)
}

// TestField_GetValue tests Field.GetValue() with various types
func TestField_GetValue(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name     string
		entity   any
		field    string
		expected any
	}{
		{
			name:     "int64",
			entity:   &User{ID: 123},
			field:    "ID",
			expected: int64(123),
		},
		{
			name:     "string",
			entity:   &User{Name: "Alice"},
			field:    "Name",
			expected: "Alice",
		},
		{
			name:     "time.Time",
			entity:   &User{CreatedAt: now},
			field:    "CreatedAt",
			expected: now,
		},
	}

	mappings := buildTestMappings(reflect.TypeOf(User{}))
	em := mappings[reflect.TypeOf(User{})]

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field := em.FieldMap[tt.field]
			result := field.GetValue(tt.entity)
			if result != tt.expected {
				t.Errorf("GetValue() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// TestField_SetValue tests Field.SetValue() with various types
func TestField_SetValue(t *testing.T) {
	mappings := buildTestMappings(reflect.TypeOf(User{}))
	em := mappings[reflect.TypeOf(User{})]

	tests := []struct {
		name     string
		field    string
		setValue any
	}{
		{"int64", "ID", int64(999)},
		{"string", "Name", "Bob"},
		{"int", "Version", int(5)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			user := &User{}
			field := em.FieldMap[tt.field]
			field.SetValue(user, tt.setValue)

			// Verify value was set
			result := field.GetValue(user)
			if result != tt.setValue {
				t.Errorf("After SetValue(), GetValue() = %v, want %v", result, tt.setValue)
			}
		})
	}
}

// TestField_GetPtr tests Field.GetPtr() returns correct pointer
func TestField_GetPtr(t *testing.T) {
	user := &User{ID: 123}

	mappings := buildTestMappings(reflect.TypeOf(User{}))
	em := mappings[reflect.TypeOf(User{})]

	field := em.FieldMap["ID"]
	ptr := field.GetPtr(user)

	// Modify via pointer
	*(ptr.(*int64)) = 999

	// Verify user.ID changed
	if user.ID != 999 {
		t.Errorf("Pointer didn't point to actual field, user.ID = %d, want 999", user.ID)
	}
}

// TestEntityMapping_Columns tests Columns() accessor
func TestEntityMapping_Columns(t *testing.T) {
	mappings := buildTestMappings(reflect.TypeOf(User{}))
	em := mappings[reflect.TypeOf(User{})]

	columns := em.Columns()

	expected := []string{"id", "name", "email", "created_at", "updated_at", "version"}
	if len(columns) != len(expected) {
		t.Errorf("Columns() length = %d, want %d", len(columns), len(expected))
	}

	// Check all expected columns present
	columnSet := make(map[string]bool)
	for _, col := range columns {
		columnSet[col] = true
	}
	for _, exp := range expected {
		if !columnSet[exp] {
			t.Errorf("Expected column %q not found in Columns()", exp)
		}
	}
}

// TestEntityMapping_PrimaryColumns tests PrimaryColumns() accessor
func TestEntityMapping_PrimaryColumns(t *testing.T) {
	mappings := buildTestMappings(reflect.TypeOf(User{}))
	em := mappings[reflect.TypeOf(User{})]

	primaryColumns := em.PrimaryColumns()

	expected := []string{"id"}
	if !reflect.DeepEqual(primaryColumns, expected) {
		t.Errorf("PrimaryColumns() = %v, want %v", primaryColumns, expected)
	}
}

// TestEntityMapping_PrimaryColumns_Composite tests composite primary key
func TestEntityMapping_PrimaryColumns_Composite(t *testing.T) {
	type OrderItem struct {
		OrderID   int64 `orm1:"primary"`
		ProductID int64 `orm1:"primary"`
		Quantity  int
	}

	mappings := buildTestMappings(reflect.TypeOf(OrderItem{}))
	em := mappings[reflect.TypeOf(OrderItem{})]

	primaryColumns := em.PrimaryColumns()

	expected := []string{"order_id", "product_id"}
	if !reflect.DeepEqual(primaryColumns, expected) {
		t.Errorf("PrimaryColumns() = %v, want %v", primaryColumns, expected)
	}
}

// TestEntityMapping_InsertableColumns tests InsertableColumns() excludes skip_insert
func TestEntityMapping_InsertableColumns(t *testing.T) {
	mappings := buildTestMappings(reflect.TypeOf(User{}))
	em := mappings[reflect.TypeOf(User{})]

	insertable := em.InsertableColumns()

	// Should exclude CreatedAt (skip_insert) and Version (skip_insert)
	excluded := map[string]bool{"created_at": true, "version": true}

	for _, col := range insertable {
		if excluded[col] {
			t.Errorf("InsertableColumns() contains %q which has skip_insert tag", col)
		}
	}

	// Should include id, name, email, updated_at
	expected := map[string]bool{
		"id":         true,
		"name":       true,
		"email":      true,
		"updated_at": true,
	}

	for _, col := range insertable {
		if expected[col] {
			delete(expected, col)
		}
	}

	if len(expected) > 0 {
		t.Errorf("InsertableColumns() missing expected columns: %v", expected)
	}
}

// TestEntityMapping_UpdatableColumns tests UpdatableColumns() excludes skip_update and primary
func TestEntityMapping_UpdatableColumns(t *testing.T) {
	mappings := buildTestMappings(reflect.TypeOf(User{}))
	em := mappings[reflect.TypeOf(User{})]

	updatable := em.UpdatableColumns()

	// Should exclude ID (primary), UpdatedAt (skip_update), Version (skip_update)
	excluded := map[string]bool{"id": true, "updated_at": true, "version": true}

	for _, col := range updatable {
		if excluded[col] {
			t.Errorf("UpdatableColumns() contains %q which should be excluded", col)
		}
	}

	// Should include name, email, created_at
	expected := map[string]bool{
		"name":       true,
		"email":      true,
		"created_at": true,
	}

	for _, col := range updatable {
		if expected[col] {
			delete(expected, col)
		}
	}

	if len(expected) > 0 {
		t.Errorf("UpdatableColumns() missing expected columns: %v", expected)
	}
}

// TestEntityMapping_ParentalColumns tests ParentalColumns()
func TestEntityMapping_ParentalColumns(t *testing.T) {
	mappings := buildTestMappings(reflect.TypeOf(User{}), reflect.TypeOf(Post{}), reflect.TypeOf(Comment{}))

	postMapping := mappings[reflect.TypeOf(Post{})]
	parentalColumns := postMapping.ParentalColumns()

	expected := []string{"user_id"}
	if !reflect.DeepEqual(parentalColumns, expected) {
		t.Errorf("ParentalColumns() = %v, want %v", parentalColumns, expected)
	}
}

// TestEntityMapping_InsertReturningColumns tests InsertReturningColumns()
func TestEntityMapping_InsertReturningColumns(t *testing.T) {
	mappings := buildTestMappings(reflect.TypeOf(User{}))
	em := mappings[reflect.TypeOf(User{})]

	returning := em.InsertReturningColumns()

	// Should include primary key (id) + auto fields (created_at, version)
	expected := map[string]bool{
		"id":         true,
		"created_at": true,
		"version":    true,
	}

	if len(returning) != len(expected) {
		t.Errorf("InsertReturningColumns() length = %d, want %d", len(returning), len(expected))
	}

	for _, col := range returning {
		if !expected[col] {
			t.Errorf("InsertReturningColumns() contains unexpected column %q", col)
		}
	}
}

// TestEntityMapping_InsertReturning tests InsertReturning() field names
func TestEntityMapping_InsertReturning(t *testing.T) {
	mappings := buildTestMappings(reflect.TypeOf(User{}))
	em := mappings[reflect.TypeOf(User{})]

	returning := em.InsertReturning()

	// Should include primary key fields + auto fields
	expected := map[string]bool{
		"ID":        true,
		"CreatedAt": true,
		"Version":   true,
	}

	if len(returning) != len(expected) {
		t.Errorf("InsertReturning() length = %d, want %d", len(returning), len(expected))
	}

	for _, field := range returning {
		if !expected[field] {
			t.Errorf("InsertReturning() contains unexpected field %q", field)
		}
	}
}

// TestChild_Get_Singular tests Child.Get() for singular relationship
func TestChild_Get_Singular(t *testing.T) {
	author := &User{ID: 1, Name: "Alice"}
	post := &Post{ID: 100, Author: author}

	mappings := buildTestMappings(reflect.TypeOf(User{}), reflect.TypeOf(Post{}), reflect.TypeOf(Comment{}))

	postMapping := mappings[reflect.TypeOf(Post{})]
	child := postMapping.ChildMap["Author"]

	result := child.Get(post)

	if len(result) != 1 {
		t.Errorf("Get() returned %d items, want 1", len(result))
	}

	if result[0] != author {
		t.Errorf("Get() returned wrong author")
	}
}

// TestChild_Get_Plural tests Child.Get() for plural relationship
func TestChild_Get_Plural(t *testing.T) {
	comment1 := &Comment{ID: 1, Text: "First"}
	comment2 := &Comment{ID: 2, Text: "Second"}
	post := &Post{ID: 100, Comments: []*Comment{comment1, comment2}}

	mappings := buildTestMappings(reflect.TypeOf(User{}), reflect.TypeOf(Post{}), reflect.TypeOf(Comment{}))

	postMapping := mappings[reflect.TypeOf(Post{})]
	child := postMapping.ChildMap["Comments"]

	result := child.Get(post)

	if len(result) != 2 {
		t.Errorf("Get() returned %d items, want 2", len(result))
	}

	if result[0] != comment1 || result[1] != comment2 {
		t.Errorf("Get() returned wrong comments")
	}
}

// TestChild_Get_NilSingular tests Child.Get() when singular child is nil
func TestChild_Get_NilSingular(t *testing.T) {
	post := &Post{ID: 100, Author: nil}

	mappings := buildTestMappings(reflect.TypeOf(User{}), reflect.TypeOf(Post{}), reflect.TypeOf(Comment{}))

	postMapping := mappings[reflect.TypeOf(Post{})]
	child := postMapping.ChildMap["Author"]

	result := child.Get(post)

	if len(result) != 0 {
		t.Errorf("Get() for nil singular returned %d items, want 0", len(result))
	}
}

// TestChild_Set_Singular tests Child.Set() for singular relationship
func TestChild_Set_Singular(t *testing.T) {
	post := &Post{ID: 100}
	author := &User{ID: 1, Name: "Alice"}

	mappings := buildTestMappings(reflect.TypeOf(User{}), reflect.TypeOf(Post{}), reflect.TypeOf(Comment{}))

	postMapping := mappings[reflect.TypeOf(Post{})]
	child := postMapping.ChildMap["Author"]

	child.Set(post, []any{author})

	if post.Author != author {
		t.Errorf("Set() didn't set Author correctly")
	}
}

// TestChild_Set_Plural tests Child.Set() for plural relationship
func TestChild_Set_Plural(t *testing.T) {
	post := &Post{ID: 100}
	comment1 := &Comment{ID: 1, Text: "First"}
	comment2 := &Comment{ID: 2, Text: "Second"}

	mappings := buildTestMappings(reflect.TypeOf(User{}), reflect.TypeOf(Post{}), reflect.TypeOf(Comment{}))

	postMapping := mappings[reflect.TypeOf(Post{})]
	child := postMapping.ChildMap["Comments"]

	child.Set(post, []any{comment1, comment2})

	if len(post.Comments) != 2 {
		t.Errorf("Set() resulted in %d comments, want 2", len(post.Comments))
	}

	if post.Comments[0] != comment1 || post.Comments[1] != comment2 {
		t.Errorf("Set() didn't set Comments correctly")
	}
}

// TestComputeColumns tests ComputeColumns helper function
func TestComputeColumns(t *testing.T) {
	fieldMap := map[string]*mapping.Field{
		"ID":   {Name: "ID", Column: "id"},
		"Name": {Name: "Name", Column: "name"},
	}

	columns := mapping.ComputeColumns([]string{"ID", "Name"}, fieldMap)

	expected := []string{"id", "name"}
	if !reflect.DeepEqual(columns, expected) {
		t.Errorf("ComputeColumns() = %v, want %v", columns, expected)
	}
}

// TestChild_Set_NilPlural tests Child.Set() when plural field is initially nil
func TestChild_Set_NilPlural(t *testing.T) {
	// Test setting plural children when the slice is initially nil
	post := &Post{ID: 100, Comments: nil}
	comment1 := &Comment{ID: 1, Text: "First"}

	mappings := buildTestMappings(reflect.TypeOf(User{}), reflect.TypeOf(Post{}), reflect.TypeOf(Comment{}))

	postMapping := mappings[reflect.TypeOf(Post{})]
	child := postMapping.ChildMap["Comments"]

	// Set children when slice is nil (should handle gracefully)
	child.Set(post, []any{comment1})

	if len(post.Comments) != 1 {
		t.Errorf("Set() on nil slice resulted in %d comments, want 1", len(post.Comments))
	}

	if post.Comments[0] != comment1 {
		t.Errorf("Set() didn't set comment correctly")
	}
}

// TestChild_Set_EmptySingular tests Child.Set() with empty slice for singular child
func TestChild_Set_EmptySingular(t *testing.T) {
	// Test setting singular child to nil by providing empty slice
	author := &User{ID: 1, Name: "Alice"}
	post := &Post{ID: 100, Author: author}

	mappings := buildTestMappings(reflect.TypeOf(User{}), reflect.TypeOf(Post{}), reflect.TypeOf(Comment{}))

	postMapping := mappings[reflect.TypeOf(Post{})]
	child := postMapping.ChildMap["Author"]

	// Set empty slice should set Author to nil
	child.Set(post, []any{})

	if post.Author != nil {
		t.Errorf("Set() with empty slice should set Author to nil, got %v", post.Author)
	}
}
