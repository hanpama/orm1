package orm1

import (
	"reflect"
	"testing"
)

// TestBuildEntityMappings_IgnoreTag tests that fields with orm1:"-" are excluded
func TestBuildEntityMappings_IgnoreTag(t *testing.T) {
	type Entity struct {
		ID      int64 `orm1:"primary"`
		Name    string
		Ignored string `orm1:"-"`
	}

	entityType := reflect.TypeOf(Entity{})
	fields := AnalyzeStruct(entityType)

	metadata := map[reflect.Type]EntityMetadata{
		entityType: {
			Schema: "",
			Table:  "entity",
			Fields: fields,
		},
	}
	registered := map[reflect.Type]bool{entityType: true}

	mappings := BuildEntityMappings(metadata, registered)
	em := mappings[entityType]

	// Ignored field should not be in FieldMap
	if _, ok := em.FieldMap["Ignored"]; ok {
		t.Error("Ignored field should not be in FieldMap")
	}

	// Ignored field should not be in AllFields
	for _, fieldName := range em.AllFields {
		if fieldName == "Ignored" {
			t.Error("Ignored field should not be in AllFields")
		}
	}

	// Only ID and Name should be present
	if len(em.FieldMap) != 2 {
		t.Errorf("Expected 2 fields in FieldMap, got %d", len(em.FieldMap))
	}
	if len(em.AllFields) != 2 {
		t.Errorf("Expected 2 fields in AllFields, got %d", len(em.AllFields))
	}
}

// TestBuildEntityMappings_AutoDetectChild_Slice tests auto-detection of []*Child
func TestBuildEntityMappings_AutoDetectChild_Slice(t *testing.T) {
	type Child struct {
		ID int64 `orm1:"primary"`
	}

	type Parent struct {
		ID       int64
		Children []*Child // No explicit child tag
	}

	childType := reflect.TypeOf(Child{})
	parentType := reflect.TypeOf(Parent{})

	childFields := AnalyzeStruct(childType)
	parentFields := AnalyzeStruct(parentType)

	metadata := map[reflect.Type]EntityMetadata{
		childType: {
			Schema: "",
			Table:  "child",
			Fields: childFields,
		},
		parentType: {
			Schema: "",
			Table:  "parent",
			Fields: parentFields,
		},
	}
	registered := map[reflect.Type]bool{
		childType:  true,
		parentType: true,
	}

	mappings := BuildEntityMappings(metadata, registered)
	parentMapping := mappings[parentType]

	// Children should be auto-detected as child relationship
	child, ok := parentMapping.ChildMap["Children"]
	if !ok {
		t.Fatal("Children should be auto-detected as child relationship")
	}

	if child.Singular {
		t.Error("Children should be plural (not singular)")
	}

	if child.Target != childType {
		t.Errorf("Child target type = %v, want %v", child.Target, childType)
	}

	// Children should NOT be in FieldMap
	if _, ok := parentMapping.FieldMap["Children"]; ok {
		t.Error("Children should not be in FieldMap")
	}
}

// TestBuildEntityMappings_AutoDetectChild_Singular tests auto-detection of *Child
func TestBuildEntityMappings_AutoDetectChild_Singular(t *testing.T) {
	type Child struct {
		ID int64 `orm1:"primary"`
	}

	type Parent struct {
		ID    int64
		Child *Child // No explicit child tag
	}

	childType := reflect.TypeOf(Child{})
	parentType := reflect.TypeOf(Parent{})

	childFields := AnalyzeStruct(childType)
	parentFields := AnalyzeStruct(parentType)

	metadata := map[reflect.Type]EntityMetadata{
		childType: {
			Schema: "",
			Table:  "child",
			Fields: childFields,
		},
		parentType: {
			Schema: "",
			Table:  "parent",
			Fields: parentFields,
		},
	}
	registered := map[reflect.Type]bool{
		childType:  true,
		parentType: true,
	}

	mappings := BuildEntityMappings(metadata, registered)
	parentMapping := mappings[parentType]

	// Child should be auto-detected as child relationship
	child, ok := parentMapping.ChildMap["Child"]
	if !ok {
		t.Fatal("Child should be auto-detected as child relationship")
	}

	if !child.Singular {
		t.Error("Child should be singular")
	}

	if child.Target != childType {
		t.Errorf("Child target type = %v, want %v", child.Target, childType)
	}
}

// TestBuildEntityMappings_IDAutoPK tests that ID field is auto-detected as primary key
func TestBuildEntityMappings_IDAutoPK(t *testing.T) {
	type Entity struct {
		ID   int64 // No explicit primary tag
		Name string
	}

	entityType := reflect.TypeOf(Entity{})
	fields := AnalyzeStruct(entityType)

	metadata := map[reflect.Type]EntityMetadata{
		entityType: {
			Schema: "",
			Table:  "entity",
			Fields: fields,
		},
	}
	registered := map[reflect.Type]bool{entityType: true}

	mappings := BuildEntityMappings(metadata, registered)
	em := mappings[entityType]

	// ID should be auto-detected as primary key
	if len(em.PrimaryKey) != 1 || em.PrimaryKey[0] != "ID" {
		t.Errorf("Expected PrimaryKey = [ID], got %v", em.PrimaryKey)
	}

	// ID should be insertable (not skip_insert)
	found := false
	for _, fieldName := range em.Insertable {
		if fieldName == "ID" {
			found = true
			break
		}
	}
	if !found {
		t.Error("ID should be in Insertable")
	}

	// ID should NOT be updatable (primary keys are immutable)
	for _, fieldName := range em.Updatable {
		if fieldName == "ID" {
			t.Error("ID should not be in Updatable")
		}
	}
}

// TestBuildEntityMappings_SkipInsert tests skip_insert tag
func TestBuildEntityMappings_SkipInsert(t *testing.T) {
	type Entity struct {
		ID        int64 `orm1:"primary"`
		Name      string
		CreatedAt int64 `orm1:"skip_insert"`
	}

	entityType := reflect.TypeOf(Entity{})
	fields := AnalyzeStruct(entityType)

	metadata := map[reflect.Type]EntityMetadata{
		entityType: {
			Schema: "",
			Table:  "entity",
			Fields: fields,
		},
	}
	registered := map[reflect.Type]bool{entityType: true}

	mappings := BuildEntityMappings(metadata, registered)
	em := mappings[entityType]

	// CreatedAt should NOT be in Insertable
	for _, fieldName := range em.Insertable {
		if fieldName == "CreatedAt" {
			t.Error("CreatedAt should not be in Insertable")
		}
	}

	// CreatedAt should be in Updatable
	found := false
	for _, fieldName := range em.Updatable {
		if fieldName == "CreatedAt" {
			found = true
			break
		}
	}
	if !found {
		t.Error("CreatedAt should be in Updatable")
	}

	// CreatedAt should be in AllFields
	found = false
	for _, fieldName := range em.AllFields {
		if fieldName == "CreatedAt" {
			found = true
			break
		}
	}
	if !found {
		t.Error("CreatedAt should be in AllFields")
	}
}

// TestBuildEntityMappings_SkipUpdate tests skip_update tag
func TestBuildEntityMappings_SkipUpdate(t *testing.T) {
	type Entity struct {
		ID        int64 `orm1:"primary"`
		Name      string
		UpdatedAt int64 `orm1:"skip_update"`
	}

	entityType := reflect.TypeOf(Entity{})
	fields := AnalyzeStruct(entityType)

	metadata := map[reflect.Type]EntityMetadata{
		entityType: {
			Schema: "",
			Table:  "entity",
			Fields: fields,
		},
	}
	registered := map[reflect.Type]bool{entityType: true}

	mappings := BuildEntityMappings(metadata, registered)
	em := mappings[entityType]

	// UpdatedAt should be in Insertable
	found := false
	for _, fieldName := range em.Insertable {
		if fieldName == "UpdatedAt" {
			found = true
			break
		}
	}
	if !found {
		t.Error("UpdatedAt should be in Insertable")
	}

	// UpdatedAt should NOT be in Updatable
	for _, fieldName := range em.Updatable {
		if fieldName == "UpdatedAt" {
			t.Error("UpdatedAt should not be in Updatable")
		}
	}
}

// TestBuildEntityMappings_AutoTag tests auto tag (skip_insert + skip_update)
func TestBuildEntityMappings_AutoTag(t *testing.T) {
	type Entity struct {
		ID      int64 `orm1:"primary"`
		Name    string
		Version int64 `orm1:"auto"`
	}

	entityType := reflect.TypeOf(Entity{})
	fields := AnalyzeStruct(entityType)

	metadata := map[reflect.Type]EntityMetadata{
		entityType: {
			Schema: "",
			Table:  "entity",
			Fields: fields,
		},
	}
	registered := map[reflect.Type]bool{entityType: true}

	mappings := BuildEntityMappings(metadata, registered)
	em := mappings[entityType]

	// Version should NOT be in Insertable
	for _, fieldName := range em.Insertable {
		if fieldName == "Version" {
			t.Error("Version with auto tag should not be in Insertable")
		}
	}

	// Version should NOT be in Updatable
	for _, fieldName := range em.Updatable {
		if fieldName == "Version" {
			t.Error("Version with auto tag should not be in Updatable")
		}
	}

	// Version should be in AllFields
	found := false
	for _, fieldName := range em.AllFields {
		if fieldName == "Version" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Version should be in AllFields")
	}
}

// TestBuildEntityMappings_UnregisteredStructPtr tests that unregistered struct pointers are ignored
func TestBuildEntityMappings_UnregisteredStructPtr(t *testing.T) {
	type Other struct {
		Data string
	}

	type Entity struct {
		ID      int64 `orm1:"primary"`
		Name    string
		Unknown *Other // Not registered
	}

	entityType := reflect.TypeOf(Entity{})
	fields := AnalyzeStruct(entityType)

	metadata := map[reflect.Type]EntityMetadata{
		entityType: {
			Schema: "",
			Table:  "entity",
			Fields: fields,
		},
	}
	registered := map[reflect.Type]bool{entityType: true}
	// Note: Other is NOT registered

	mappings := BuildEntityMappings(metadata, registered)
	em := mappings[entityType]

	// Unknown should not be in FieldMap
	if _, ok := em.FieldMap["Unknown"]; ok {
		t.Error("Unknown should not be in FieldMap")
	}

	// Unknown should not be in ChildMap
	if _, ok := em.ChildMap["Unknown"]; ok {
		t.Error("Unknown should not be in ChildMap")
	}
}

// TestBuildEntityMappings_SliceOfNonEntity tests that slices of unregistered types are ignored
func TestBuildEntityMappings_SliceOfNonEntity(t *testing.T) {
	type Other struct {
		Data string
	}

	type Entity struct {
		ID      int64 `orm1:"primary"`
		Name    string
		Strings []string // Slice of primitives
		Others  []*Other // Slice of unregistered structs
	}

	entityType := reflect.TypeOf(Entity{})
	fields := AnalyzeStruct(entityType)

	metadata := map[reflect.Type]EntityMetadata{
		entityType: {
			Schema: "",
			Table:  "entity",
			Fields: fields,
		},
	}
	registered := map[reflect.Type]bool{entityType: true}

	mappings := BuildEntityMappings(metadata, registered)
	em := mappings[entityType]

	// Strings should not be in FieldMap (it's a slice)
	if _, ok := em.FieldMap["Strings"]; ok {
		t.Error("Strings should not be in FieldMap")
	}

	// Others should not be in ChildMap (not registered)
	if _, ok := em.ChildMap["Others"]; ok {
		t.Error("Others should not be in ChildMap")
	}
}

// TestBuildEntityMappings_ExplicitChildTag tests explicit orm1:"child" tag
func TestBuildEntityMappings_ExplicitChildTag(t *testing.T) {
	type Child struct {
		ID int64 `orm1:"primary"`
	}

	type Parent struct {
		ID       int64
		Children []*Child `orm1:"child"` // Explicit child tag
	}

	childType := reflect.TypeOf(Child{})
	parentType := reflect.TypeOf(Parent{})

	childFields := AnalyzeStruct(childType)
	parentFields := AnalyzeStruct(parentType)

	metadata := map[reflect.Type]EntityMetadata{
		childType: {
			Schema: "",
			Table:  "child",
			Fields: childFields,
		},
		parentType: {
			Schema: "",
			Table:  "parent",
			Fields: parentFields,
		},
	}
	registered := map[reflect.Type]bool{
		childType:  true,
		parentType: true,
	}

	mappings := BuildEntityMappings(metadata, registered)
	parentMapping := mappings[parentType]

	// Children should be in ChildMap
	child, ok := parentMapping.ChildMap["Children"]
	if !ok {
		t.Fatal("Children should be in ChildMap with explicit child tag")
	}

	if child.Target != childType {
		t.Errorf("Child target type = %v, want %v", child.Target, childType)
	}
}

// TestBuildEntityMappings_ParentalKey tests parental tag
func TestBuildEntityMappings_ParentalKey(t *testing.T) {
	type Entity struct {
		ID       int64 `orm1:"primary"`
		ParentID int64 `orm1:"parental"`
		Name     string
	}

	entityType := reflect.TypeOf(Entity{})
	fields := AnalyzeStruct(entityType)

	metadata := map[reflect.Type]EntityMetadata{
		entityType: {
			Schema: "",
			Table:  "entity",
			Fields: fields,
		},
	}
	registered := map[reflect.Type]bool{entityType: true}

	mappings := BuildEntityMappings(metadata, registered)
	em := mappings[entityType]

	// ParentID should be in ParentalKey
	if len(em.ParentalKey) != 1 || em.ParentalKey[0] != "ParentID" {
		t.Errorf("Expected ParentalKey = [ParentID], got %v", em.ParentalKey)
	}

	// ParentID should be insertable
	found := false
	for _, fieldName := range em.Insertable {
		if fieldName == "ParentID" {
			found = true
			break
		}
	}
	if !found {
		t.Error("ParentID should be in Insertable")
	}

	// ParentID should NOT be updatable (parental keys are immutable)
	for _, fieldName := range em.Updatable {
		if fieldName == "ParentID" {
			t.Error("ParentID should not be in Updatable")
		}
	}
}

// TestBuildEntityMappings_CompositeKey tests composite primary key
func TestBuildEntityMappings_CompositeKey(t *testing.T) {
	type Entity struct {
		Key1 int64 `orm1:"primary"`
		Key2 int64 `orm1:"primary"`
		Name string
	}

	entityType := reflect.TypeOf(Entity{})
	fields := AnalyzeStruct(entityType)

	metadata := map[reflect.Type]EntityMetadata{
		entityType: {
			Schema: "",
			Table:  "entity",
			Fields: fields,
		},
	}
	registered := map[reflect.Type]bool{entityType: true}

	mappings := BuildEntityMappings(metadata, registered)
	em := mappings[entityType]

	// PrimaryKey should have both Key1 and Key2
	if len(em.PrimaryKey) != 2 {
		t.Fatalf("Expected PrimaryKey length 2, got %d", len(em.PrimaryKey))
	}

	// Check that both keys are present (order doesn't matter for this test)
	hasKey1, hasKey2 := false, false
	for _, key := range em.PrimaryKey {
		if key == "Key1" {
			hasKey1 = true
		}
		if key == "Key2" {
			hasKey2 = true
		}
	}
	if !hasKey1 || !hasKey2 {
		t.Errorf("Expected PrimaryKey to contain both Key1 and Key2, got %v", em.PrimaryKey)
	}

	// Both keys should be insertable
	insertableMap := make(map[string]bool)
	for _, fieldName := range em.Insertable {
		insertableMap[fieldName] = true
	}
	if !insertableMap["Key1"] || !insertableMap["Key2"] {
		t.Error("Both Key1 and Key2 should be in Insertable")
	}

	// Neither key should be updatable
	for _, fieldName := range em.Updatable {
		if fieldName == "Key1" || fieldName == "Key2" {
			t.Errorf("%s should not be in Updatable", fieldName)
		}
	}
}

// TestBuildEntityMappings_ColumnTag tests custom column names
func TestBuildEntityMappings_ColumnTag(t *testing.T) {
	type Entity struct {
		ID       int64  `orm1:"primary"`
		FullName string `orm1:"column:full_name"`
	}

	entityType := reflect.TypeOf(Entity{})
	fields := AnalyzeStruct(entityType)

	metadata := map[reflect.Type]EntityMetadata{
		entityType: {
			Schema: "",
			Table:  "entity",
			Fields: fields,
		},
	}
	registered := map[reflect.Type]bool{entityType: true}

	mappings := BuildEntityMappings(metadata, registered)
	em := mappings[entityType]

	// FullName field should have custom column name
	field, ok := em.FieldMap["FullName"]
	if !ok {
		t.Fatal("FullName should be in FieldMap")
	}

	if field.Column != "full_name" {
		t.Errorf("Expected column name 'full_name', got '%s'", field.Column)
	}
}

// TestBuildEntityMappings_MultipleEntities tests building multiple entities
func TestBuildEntityMappings_MultipleEntities(t *testing.T) {
	type User struct {
		ID int64 `orm1:"primary"`
	}

	type Post struct {
		ID int64 `orm1:"primary"`
	}

	userType := reflect.TypeOf(User{})
	postType := reflect.TypeOf(Post{})

	userFields := AnalyzeStruct(userType)
	postFields := AnalyzeStruct(postType)

	metadata := map[reflect.Type]EntityMetadata{
		userType: {
			Schema: "",
			Table:  "users",
			Fields: userFields,
		},
		postType: {
			Schema: "",
			Table:  "posts",
			Fields: postFields,
		},
	}
	registered := map[reflect.Type]bool{
		userType: true,
		postType: true,
	}

	mappings := BuildEntityMappings(metadata, registered)

	if len(mappings) != 2 {
		t.Errorf("Expected 2 mappings, got %d", len(mappings))
	}

	if mappings[userType] == nil {
		t.Error("User mapping should not be nil")
	}

	if mappings[postType] == nil {
		t.Error("Post mapping should not be nil")
	}

	if mappings[userType].Table != "users" {
		t.Errorf("Expected User table 'users', got '%s'", mappings[userType].Table)
	}

	if mappings[postType].Table != "posts" {
		t.Errorf("Expected Post table 'posts', got '%s'", mappings[postType].Table)
	}
}

// TestBuildEntityMappings_EmptyMetadata tests building with empty metadata
func TestBuildEntityMappings_EmptyMetadata(t *testing.T) {
	metadata := map[reflect.Type]EntityMetadata{}
	registered := map[reflect.Type]bool{}

	mappings := BuildEntityMappings(metadata, registered)

	if len(mappings) != 0 {
		t.Errorf("Expected 0 mappings, got %d", len(mappings))
	}
}
