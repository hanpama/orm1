package orm1_test

import (
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/hanpama/orm1"
	"github.com/hanpama/orm1/mapping"
)

// TestMappingBuilder tests entity mapping with various struct tag configurations
func TestMappingBuilder(t *testing.T) {
	tests := []struct {
		name   string
		entity any
		want   *mapping.EntityMapping
	}{
		{
			name: "primary tag",
			entity: &struct {
				ID   int64 `orm1:"primary"`
				Name string
			}{},
			want: &mapping.EntityMapping{
				FieldMap: map[string]*mapping.Field{
					"ID":   {Name: "ID", Column: "id", Type: reflect.TypeOf(int64(0)), ByteOffset: 0x00},
					"Name": {Name: "Name", Column: "name", Type: reflect.TypeOf(""), ByteOffset: 0x08},
				},
				ChildMap:    map[string]*mapping.Child{},
				AllFields:   []string{"ID", "Name"},
				PrimaryKey:  []string{"ID"},
				ParentalKey: []string{},
				Insertable:  []string{"ID", "Name"},
				Updatable:   []string{"Name"},
			},
		},
		{
			name: "parental tag",
			entity: &struct {
				ID       int64 `orm1:"primary"`
				ParentID int64 `orm1:"parental"`
				Name     string
			}{},
			want: &mapping.EntityMapping{
				FieldMap: map[string]*mapping.Field{
					"ID":       {Name: "ID", Column: "id", Type: reflect.TypeOf(int64(0)), ByteOffset: 0x00},
					"ParentID": {Name: "ParentID", Column: "parent_id", Type: reflect.TypeOf(int64(0)), ByteOffset: 0x08},
					"Name":     {Name: "Name", Column: "name", Type: reflect.TypeOf(""), ByteOffset: 0x10},
				},
				ChildMap:    map[string]*mapping.Child{},
				AllFields:   []string{"ID", "ParentID", "Name"},
				PrimaryKey:  []string{"ID"},
				ParentalKey: []string{"ParentID"},
				Insertable:  []string{"ID", "ParentID", "Name"},
				Updatable:   []string{"Name"},
			},
		},
		{
			name: "column tag",
			entity: &struct {
				ID       int64  `orm1:"primary"`
				FullName string `orm1:"column:full_name"`
			}{},
			want: &mapping.EntityMapping{
				FieldMap: map[string]*mapping.Field{
					"ID":       {Name: "ID", Column: "id", Type: reflect.TypeOf(int64(0)), ByteOffset: 0x00},
					"FullName": {Name: "FullName", Column: "full_name", Type: reflect.TypeOf(""), ByteOffset: 0x08},
				},
				ChildMap:    map[string]*mapping.Child{},
				AllFields:   []string{"ID", "FullName"},
				PrimaryKey:  []string{"ID"},
				ParentalKey: []string{},
				Insertable:  []string{"ID", "FullName"},
				Updatable:   []string{"FullName"},
			},
		},
		{
			name: "skip_insert tag",
			entity: &struct {
				ID        int64 `orm1:"primary"`
				Name      string
				CreatedAt string `orm1:"skip_insert"`
			}{},
			want: &mapping.EntityMapping{
				FieldMap: map[string]*mapping.Field{
					"ID":        {Name: "ID", Column: "id", Type: reflect.TypeOf(int64(0)), ByteOffset: 0x00},
					"Name":      {Name: "Name", Column: "name", Type: reflect.TypeOf(""), ByteOffset: 0x08},
					"CreatedAt": {Name: "CreatedAt", Column: "created_at", Type: reflect.TypeOf(""), ByteOffset: 0x18},
				},
				ChildMap:    map[string]*mapping.Child{},
				AllFields:   []string{"ID", "Name", "CreatedAt"},
				PrimaryKey:  []string{"ID"},
				ParentalKey: []string{},
				Insertable:  []string{"ID", "Name"},
				Updatable:   []string{"Name", "CreatedAt"},
			},
		},
		{
			name: "skip_update tag",
			entity: &struct {
				ID        int64 `orm1:"primary"`
				Name      string
				UpdatedAt string `orm1:"skip_update"`
			}{},
			want: &mapping.EntityMapping{
				FieldMap: map[string]*mapping.Field{
					"ID":        {Name: "ID", Column: "id", Type: reflect.TypeOf(int64(0)), ByteOffset: 0x00},
					"Name":      {Name: "Name", Column: "name", Type: reflect.TypeOf(""), ByteOffset: 0x08},
					"UpdatedAt": {Name: "UpdatedAt", Column: "updated_at", Type: reflect.TypeOf(""), ByteOffset: 0x18},
				},
				ChildMap:    map[string]*mapping.Child{},
				AllFields:   []string{"ID", "Name", "UpdatedAt"},
				PrimaryKey:  []string{"ID"},
				ParentalKey: []string{},
				Insertable:  []string{"ID", "Name", "UpdatedAt"},
				Updatable:   []string{"Name"},
			},
		},
		{
			name: "skip_insert and skip_update",
			entity: &struct {
				ID      int64 `orm1:"primary"`
				Name    string
				Version int `orm1:"skip_insert,skip_update"`
			}{},
			want: &mapping.EntityMapping{
				FieldMap: map[string]*mapping.Field{
					"ID":      {Name: "ID", Column: "id", Type: reflect.TypeOf(int64(0)), ByteOffset: 0x00},
					"Name":    {Name: "Name", Column: "name", Type: reflect.TypeOf(""), ByteOffset: 0x08},
					"Version": {Name: "Version", Column: "version", Type: reflect.TypeOf(int(0)), ByteOffset: 0x18},
				},
				ChildMap:    map[string]*mapping.Child{},
				AllFields:   []string{"ID", "Name", "Version"},
				PrimaryKey:  []string{"ID"},
				ParentalKey: []string{},
				Insertable:  []string{"ID", "Name"},
				Updatable:   []string{"Name"},
			},
		},
		{
			name: "ignore tag",
			entity: &struct {
				ID      int64 `orm1:"primary"`
				Name    string
				Ignored string `orm1:"-"`
			}{},
			want: &mapping.EntityMapping{
				FieldMap: map[string]*mapping.Field{
					"ID":   {Name: "ID", Column: "id", Type: reflect.TypeOf(int64(0)), ByteOffset: 0x00},
					"Name": {Name: "Name", Column: "name", Type: reflect.TypeOf(""), ByteOffset: 0x08},
				},
				ChildMap:    map[string]*mapping.Child{},
				AllFields:   []string{"ID", "Name"},
				PrimaryKey:  []string{"ID"},
				ParentalKey: []string{},
				Insertable:  []string{"ID", "Name"},
				Updatable:   []string{"Name"},
			},
		},
		{
			name: "multiple tags",
			entity: &struct {
				ID          int64  `orm1:"primary"`
				UserID      int64  `orm1:"parental,skip_update"`
				DisplayName string `orm1:"column:display_name,skip_insert"`
				Name        string
			}{},
			want: &mapping.EntityMapping{
				FieldMap: map[string]*mapping.Field{
					"ID":          {Name: "ID", Column: "id", Type: reflect.TypeOf(int64(0)), ByteOffset: 0x00},
					"UserID":      {Name: "UserID", Column: "user_id", Type: reflect.TypeOf(int64(0)), ByteOffset: 0x08},
					"DisplayName": {Name: "DisplayName", Column: "display_name", Type: reflect.TypeOf(""), ByteOffset: 0x10},
					"Name":        {Name: "Name", Column: "name", Type: reflect.TypeOf(""), ByteOffset: 0x20},
				},
				ChildMap:    map[string]*mapping.Child{},
				AllFields:   []string{"ID", "UserID", "DisplayName", "Name"},
				PrimaryKey:  []string{"ID"},
				ParentalKey: []string{"UserID"},
				Insertable:  []string{"ID", "UserID", "Name"},
				Updatable:   []string{"DisplayName", "Name"},
			},
		},
		{
			name: "auto-detect ID only (ParentID needs explicit tag)",
			entity: &struct {
				ID       int64
				ParentID int64
				Name     string
			}{},
			want: &mapping.EntityMapping{
				FieldMap: map[string]*mapping.Field{
					"ID":       {Name: "ID", Column: "id", Type: reflect.TypeOf(int64(0)), ByteOffset: 0x00},
					"ParentID": {Name: "ParentID", Column: "parent_id", Type: reflect.TypeOf(int64(0)), ByteOffset: 0x08},
					"Name":     {Name: "Name", Column: "name", Type: reflect.TypeOf(""), ByteOffset: 0x10},
				},
				ChildMap:    map[string]*mapping.Child{},
				AllFields:   []string{"ID", "ParentID", "Name"},
				PrimaryKey:  []string{"ID"},
				ParentalKey: []string{},
				Insertable:  []string{"ID", "ParentID", "Name"},
				Updatable:   []string{"ParentID", "Name"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			registry := orm1.NewRegistry()
			registry.Register(tt.entity)

			// Build mappings using the mapping package directly
			mappings := mapping.BuildEntityMappings(
				registry.GetMetadata(),
				registry.GetRegistered(),
			)

			if len(mappings) != 1 {
				t.Fatalf("Expected 1 mapping, got %d", len(mappings))
			}

			val := reflect.ValueOf(tt.entity)
			entityType := val.Elem().Type()
			got := mappings[entityType]

			// Ignore EntityType as it varies per anonymous struct
			// Ignore unexported pre-computed fields (tested via accessor methods)
			// Add custom comparer for reflect.Type
			opts := []cmp.Option{
				cmpopts.IgnoreFields(mapping.EntityMapping{}, "EntityType"),
				cmpopts.IgnoreUnexported(mapping.EntityMapping{}),
				cmp.Comparer(func(a, b reflect.Type) bool {
					if a == nil && b == nil {
						return true
					}
					if a == nil || b == nil {
						return false
					}
					return a == b
				}),
			}

			if diff := cmp.Diff(tt.want, got, opts...); diff != "" {
				t.Errorf("EntityMapping mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestMappingBuilder_Child(t *testing.T) {
	type Child struct {
		ID int64 `orm1:"primary"`
	}

	type Parent struct {
		ID       int64    `orm1:"primary"`
		Children []*Child `orm1:"child"`
	}

	registry := orm1.NewRegistry()
	registry.Register(&Parent{})
	registry.Register(&Child{})

	mappings := mapping.BuildEntityMappings(
		registry.GetMetadata(),
		registry.GetRegistered(),
	)

	parentMapping := mappings[reflect.TypeOf(Parent{})]

	if parentMapping == nil {
		t.Fatal("Parent mapping not found")
	}

	if _, ok := parentMapping.ChildMap["Children"]; !ok {
		t.Error("Children should be in ChildMap")
	}
}

// TestRegister_PanicOnNonStruct tests Register panics when given non-struct type
func TestRegister_PanicOnNonStruct(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic when registering non-struct type")
		}
	}()

	registry := orm1.NewRegistry()
	// Try to register int pointer (not a struct)
	var num int = 42
	registry.Register(&num)
}

// TestBuild_Idempotent tests that BuildEntityMappings is deterministic
func TestBuild_Idempotent(t *testing.T) {
	type User struct {
		ID int64 `orm1:"primary"`
	}

	registry := orm1.NewRegistry()
	registry.Register(&User{})

	mappings1 := mapping.BuildEntityMappings(
		registry.GetMetadata(),
		registry.GetRegistered(),
	)
	mappings2 := mapping.BuildEntityMappings(
		registry.GetMetadata(),
		registry.GetRegistered(),
	)

	// Should return same result
	if len(mappings1) != len(mappings2) {
		t.Errorf("BuildEntityMappings returned different number of mappings")
	}

	em1 := mappings1[reflect.TypeOf(User{})]
	em2 := mappings2[reflect.TypeOf(User{})]

	// Different instances but same content
	if em1.Table != em2.Table || len(em1.AllFields) != len(em2.AllFields) {
		t.Errorf("BuildEntityMappings returned inconsistent results")
	}
}

// TestWithSchema tests WithSchema option
func TestWithSchema(t *testing.T) {
	type User struct {
		ID int64 `orm1:"primary"`
	}

	registry := orm1.NewRegistry()
	registry.Register(&User{}, orm1.WithSchema("public"))
	mappings := mapping.BuildEntityMappings(registry.GetMetadata(), registry.GetRegistered())

	em := mappings[reflect.TypeOf(User{})]

	if em.Schema != "public" {
		t.Errorf("Schema = %q, want 'public'", em.Schema)
	}
}

// TestWithTable tests WithTable option
func TestWithTable(t *testing.T) {
	type User struct {
		ID int64 `orm1:"primary"`
	}

	registry := orm1.NewRegistry()
	registry.Register(&User{}, orm1.WithTable("users"))
	mappings := mapping.BuildEntityMappings(registry.GetMetadata(), registry.GetRegistered())

	em := mappings[reflect.TypeOf(User{})]

	if em.Table != "users" {
		t.Errorf("Table = %q, want 'users'", em.Table)
	}
}

// TestBuilder_MultipleEntities tests registering multiple entities
func TestBuilder_MultipleEntities(t *testing.T) {
	type User struct {
		ID int64 `orm1:"primary"`
	}

	type Post struct {
		ID     int64 `orm1:"primary"`
		UserID int64 `orm1:"parental"`
	}

	type Comment struct {
		ID     int64 `orm1:"primary"`
		PostID int64 `orm1:"parental"`
	}

	registry := orm1.NewRegistry()
	registry.Register(&User{})
	registry.Register(&Post{})
	registry.Register(&Comment{})
	mappings := mapping.BuildEntityMappings(registry.GetMetadata(), registry.GetRegistered())

	if len(mappings) != 3 {
		t.Errorf("Expected 3 mappings, got %d", len(mappings))
	}

	if mappings[reflect.TypeOf(User{})] == nil {
		t.Error("User mapping not found")
	}
	if mappings[reflect.TypeOf(Post{})] == nil {
		t.Error("Post mapping not found")
	}
	if mappings[reflect.TypeOf(Comment{})] == nil {
		t.Error("Comment mapping not found")
	}
}

// TestBuilder_MultipleOptions tests combining multiple options
func TestBuilder_MultipleOptions(t *testing.T) {
	type User struct {
		UserID int64 `orm1:"primary"`
		Name   string
	}

	registry := orm1.NewRegistry()
	registry.Register(
		&User{},
		orm1.WithSchema("public"),
		orm1.WithTable("users"),
	)
	mappings := mapping.BuildEntityMappings(registry.GetMetadata(), registry.GetRegistered())

	em := mappings[reflect.TypeOf(User{})]

	if em.Schema != "public" {
		t.Errorf("Schema = %q, want 'public'", em.Schema)
	}
	if em.Table != "users" {
		t.Errorf("Table = %q, want 'users'", em.Table)
	}
	if len(em.PrimaryKey) != 1 || em.PrimaryKey[0] != "UserID" {
		t.Errorf("PrimaryKey = %v, want ['UserID']", em.PrimaryKey)
	}
}

// TestBuilder_AutoDetectChild_Singular tests auto-detection of singular child relationship
func TestBuilder_AutoDetectChild_Singular(t *testing.T) {
	type Child struct {
		ID int64 `orm1:"primary"`
	}

	type Parent struct {
		ID        int64  `orm1:"primary"`
		MyChild   *Child // No explicit child tag - should auto-detect
		OtherData string
	}

	registry := orm1.NewRegistry()
	registry.Register(&Child{})
	registry.Register(&Parent{})
	mappings := mapping.BuildEntityMappings(registry.GetMetadata(), registry.GetRegistered())

	parentMapping := mappings[reflect.TypeOf(Parent{})]

	// Should auto-detect MyChild as a child relationship
	if _, ok := parentMapping.ChildMap["MyChild"]; !ok {
		t.Error("MyChild should be auto-detected as child relationship")
	}

	child := parentMapping.ChildMap["MyChild"]
	if !child.Singular {
		t.Error("MyChild should be singular child")
	}
	if child.Target != reflect.TypeOf(Child{}) {
		t.Errorf("MyChild target type = %v, want Child", child.Target)
	}
}

// TestBuilder_AutoDetectChild_Plural tests auto-detection of plural child relationship
func TestBuilder_AutoDetectChild_Plural(t *testing.T) {
	type Child struct {
		ID int64 `orm1:"primary"`
	}

	type Parent struct {
		ID         int64    `orm1:"primary"`
		MyChildren []*Child // No explicit child tag - should auto-detect
		OtherData  string
	}

	registry := orm1.NewRegistry()
	registry.Register(&Child{})
	registry.Register(&Parent{})
	mappings := mapping.BuildEntityMappings(registry.GetMetadata(), registry.GetRegistered())

	parentMapping := mappings[reflect.TypeOf(Parent{})]

	// Should auto-detect MyChildren as a child relationship
	if _, ok := parentMapping.ChildMap["MyChildren"]; !ok {
		t.Error("MyChildren should be auto-detected as child relationship")
	}

	child := parentMapping.ChildMap["MyChildren"]
	if child.Singular {
		t.Error("MyChildren should be plural child")
	}
	if child.Target != reflect.TypeOf(Child{}) {
		t.Errorf("MyChildren target type = %v, want Child", child.Target)
	}
}

// TestBuilder_IgnoreUnregisteredPtr tests that pointer fields to unregistered types are ignored
func TestBuilder_IgnoreUnregisteredPtr(t *testing.T) {
	type OtherStruct struct {
		Data string
	}

	type MyEntity struct {
		ID            int64 `orm1:"primary"`
		Name          string
		UnknownPtr    *OtherStruct // Not registered, should be ignored
		UnknownSlice  []*OtherStruct
		RegularString string
	}

	registry := orm1.NewRegistry()
	registry.Register(&MyEntity{})
	mappings := mapping.BuildEntityMappings(registry.GetMetadata(), registry.GetRegistered())

	em := mappings[reflect.TypeOf(MyEntity{})]

	// UnknownPtr and UnknownSlice should not be in FieldMap or ChildMap
	if _, ok := em.FieldMap["UnknownPtr"]; ok {
		t.Error("UnknownPtr should not be in FieldMap")
	}
	if _, ok := em.FieldMap["UnknownSlice"]; ok {
		t.Error("UnknownSlice should not be in FieldMap")
	}
	if _, ok := em.ChildMap["UnknownPtr"]; ok {
		t.Error("UnknownPtr should not be in ChildMap")
	}
	if _, ok := em.ChildMap["UnknownSlice"]; ok {
		t.Error("UnknownSlice should not be in ChildMap")
	}

	// But RegularString should be in FieldMap
	if _, ok := em.FieldMap["RegularString"]; !ok {
		t.Error("RegularString should be in FieldMap")
	}
}
