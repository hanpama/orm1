package mapping_test

import (
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
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
			builder := mapping.NewEntityMappingBuilder()
			val := reflect.ValueOf(tt.entity)
			entityType := val.Elem().Type()
			builder.Register(entityType)
			mappings := builder.Build()

			if len(mappings) != 1 {
				t.Fatalf("Expected 1 mapping, got %d", len(mappings))
			}

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

	builder := mapping.NewEntityMappingBuilder()
	builder.Register(reflect.TypeOf(Parent{}))
	builder.Register(reflect.TypeOf(Child{}))
	mappings := builder.Build()

	parentMapping := mappings[reflect.TypeOf(Parent{})]

	if parentMapping == nil {
		t.Fatal("Parent mapping not found")
	}

	if _, ok := parentMapping.ChildMap["Children"]; !ok {
		t.Error("Children should be in ChildMap")
	}
}
