package mapping_test

import (
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/hanpama/orm1/mapping"
)

func TestToSnakeCase(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"ID", "id"},
		{"UserID", "user_id"},
		{"HTTPServer", "http_server"},
		{"Name", "name"},
		{"FirstName", "first_name"},
		{"XMLParser", "xml_parser"},
		{"IOError", "io_error"},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			result := mapping.ToSnakeCase(tc.input)
			if result != tc.expected {
				t.Errorf("ToSnakeCase(%q) = %q, want %q", tc.input, result, tc.expected)
			}
		})
	}
}

func TestAnalyzeStruct(t *testing.T) {
	type Post struct {
		ID int64
	}

	type TestStruct struct {
		ID          int64 `orm1:"primary"`
		UserID      int64 `orm1:"parental"`
		Name        string
		DisplayName string  `orm1:"column:display_name"`
		CreatedAt   string  `orm1:"skip_insert"`
		UpdatedAt   string  `orm1:"skip_update"`
		Version     int     `orm1:"skip_insert,skip_update"`
		AutoField   int64   `orm1:"auto"`
		Internal    string  `orm1:"-"`
		Posts       []*Post `orm1:"child"`
		unexported  string
	}

	structType := reflect.TypeOf(TestStruct{})
	got := mapping.AnalyzeStruct(structType)

	// Expected result
	want := []mapping.FieldMetadata{
		{
			Name:          "ID",
			Typ:           reflect.TypeOf(int64(0)),
			ByteOffset:    0,
			PrimaryTag:    true,
			DefaultColumn: "id",
		},
		{
			Name:          "UserID",
			Typ:           reflect.TypeOf(int64(0)),
			ByteOffset:    8,
			ParentalTag:   true,
			DefaultColumn: "user_id",
		},
		{
			Name:          "Name",
			Typ:           reflect.TypeOf(""),
			ByteOffset:    16,
			DefaultColumn: "name",
		},
		{
			Name:          "DisplayName",
			Typ:           reflect.TypeOf(""),
			ByteOffset:    32,
			ColumnTag:     "display_name",
			DefaultColumn: "display_name",
		},
		{
			Name:          "CreatedAt",
			Typ:           reflect.TypeOf(""),
			ByteOffset:    48,
			SkipInsertTag: true,
			DefaultColumn: "created_at",
		},
		{
			Name:          "UpdatedAt",
			Typ:           reflect.TypeOf(""),
			ByteOffset:    64,
			SkipUpdateTag: true,
			DefaultColumn: "updated_at",
		},
		{
			Name:          "Version",
			Typ:           reflect.TypeOf(int(0)),
			ByteOffset:    80,
			SkipInsertTag: true,
			SkipUpdateTag: true,
			DefaultColumn: "version",
		},
		{
			Name:          "AutoField",
			Typ:           reflect.TypeOf(int64(0)),
			ByteOffset:    88,
			SkipInsertTag: true,
			SkipUpdateTag: true,
			DefaultColumn: "auto_field",
		},
		{
			Name:          "Internal",
			Typ:           reflect.TypeOf(""),
			ByteOffset:    96,
			IgnoreTag:     true,
			DefaultColumn: "internal",
		},
		{
			Name:          "Posts",
			Typ:           reflect.TypeOf([]*Post{}),
			ByteOffset:    112,
			ChildTag:      true,
			DefaultColumn: "posts",
		},
	}

	// Use go-cmp to compare
	opts := []cmp.Option{
		cmp.Comparer(func(a, b reflect.Type) bool {
			return a == b
		}),
	}

	if diff := cmp.Diff(want, got, opts...); diff != "" {
		t.Errorf("AnalyzeStruct() mismatch (-want +got):\n%s", diff)
	}
}

// TestAnalyzeStruct_NonStruct tests AnalyzeStruct with non-struct type
func TestAnalyzeStruct_NonStruct(t *testing.T) {
	result := mapping.AnalyzeStruct(reflect.TypeOf(42))
	if result != nil {
		t.Errorf("AnalyzeStruct(int) should return nil, got %v", result)
	}
}

// TestAnalyzeStruct_EmptyStruct tests AnalyzeStruct with empty struct
func TestAnalyzeStruct_EmptyStruct(t *testing.T) {
	type Empty struct{}
	result := mapping.AnalyzeStruct(reflect.TypeOf(Empty{}))
	if len(result) != 0 {
		t.Errorf("AnalyzeStruct(Empty{}) should return empty slice, got %d fields", len(result))
	}
}

// TestAnalyzeStruct_UnexportedOnly tests AnalyzeStruct with only unexported fields
func TestAnalyzeStruct_UnexportedOnly(t *testing.T) {
	type OnlyUnexported struct {
		unexported1 string
		unexported2 int
	}
	result := mapping.AnalyzeStruct(reflect.TypeOf(OnlyUnexported{}))
	if len(result) != 0 {
		t.Errorf("AnalyzeStruct with only unexported fields should return empty slice, got %d fields", len(result))
	}
}
