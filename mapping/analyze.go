package mapping

import (
	"reflect"
	"strings"
)

// FieldMetadata represents complete structural analysis of a single struct field.
// This is a pure data structure containing everything that can be extracted
// from a struct field definition, including reflection metadata and tag parsing.
type FieldMetadata struct {
	// Structural metadata (from reflect)
	Name       string
	Typ        reflect.Type
	ByteOffset uintptr

	// Tag parsing results
	PrimaryTag    bool
	ParentalTag   bool
	ChildTag      bool
	SkipInsertTag bool
	SkipUpdateTag bool
	IgnoreTag     bool
	ColumnTag     string // Custom column name from tag, "" if not specified

	// Computed values
	DefaultColumn string // snake_case version of name
}

// ToSnakeCase converts a CamelCase string to snake_case.
// Examples: "UserID" -> "user_id", "HTTPServer" -> "http_server"
func ToSnakeCase(s string) string {
	var result strings.Builder
	for i, r := range s {
		if i > 0 && r >= 'A' && r <= 'Z' {
			if i > 0 {
				prevRune := rune(s[i-1])
				if prevRune >= 'a' && prevRune <= 'z' {
					result.WriteRune('_')
				} else if i+1 < len(s) {
					nextRune := rune(s[i+1])
					if nextRune >= 'a' && nextRune <= 'z' {
						result.WriteRune('_')
					}
				}
			}
		}
		result.WriteRune(r)
	}
	return strings.ToLower(result.String())
}

// AnalyzeStruct analyzes a struct type and returns complete metadata for all fields.
// This performs a pure structural analysis: reflection metadata + tag parsing + computed values.
// Only exported fields are included in the analysis.
func AnalyzeStruct(structType reflect.Type) []FieldMetadata {
	if structType.Kind() != reflect.Struct {
		return nil
	}

	var fields []FieldMetadata

	for i := 0; i < structType.NumField(); i++ {
		structField := structType.Field(i)

		if !structField.IsExported() {
			continue
		}

		metadata := FieldMetadata{
			Name:          structField.Name,
			Typ:           structField.Type,
			ByteOffset:    structField.Offset,
			DefaultColumn: ToSnakeCase(structField.Name),
		}

		tagValue := structField.Tag.Get("orm1")
		if tagValue != "" {
			if tagValue == "-" {
				metadata.IgnoreTag = true
			} else {
				parts := strings.Split(tagValue, ",")
				for _, part := range parts {
					part = strings.TrimSpace(part)

					switch {
					case part == "primary":
						metadata.PrimaryTag = true
					case part == "parental":
						metadata.ParentalTag = true
					case part == "child":
						metadata.ChildTag = true
					case part == "auto":
						// auto is shorthand for skip_insert,skip_update
						metadata.SkipInsertTag = true
						metadata.SkipUpdateTag = true
					case part == "skip_insert":
						metadata.SkipInsertTag = true
					case part == "skip_update":
						metadata.SkipUpdateTag = true
					case strings.HasPrefix(part, "column:"):
						metadata.ColumnTag = strings.TrimPrefix(part, "column:")
					}
				}
			}
		}

		fields = append(fields, metadata)
	}

	return fields
}
