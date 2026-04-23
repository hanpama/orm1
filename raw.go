package orm1

import (
	"context"
	"fmt"
	"reflect"
)

// RawQuery represents a raw SQL query with parameter interpolation.
type RawQuery struct {
	session  *Session
	fragment SQL
}

// NewRawQuery creates a new raw query with the given SQL and parameters.
func NewRawQuery(session *Session, query string, params ...any) *RawQuery {
	fragment := ParseSQL(query, params...)

	return &RawQuery{
		session:  session,
		fragment: fragment,
	}
}

// Rows executes the raw query and returns the result rows.
// The caller is responsible for calling Close() on the returned Rows.
func (r *RawQuery) Rows(ctx context.Context) (Rows, error) {
	return r.session.backend.FetchRaw(ctx, r.fragment)
}

// Exec executes the raw query and returns the number of rows affected.
// Use this for INSERT, UPDATE, DELETE, and other non-SELECT queries.
//
// Example:
//
//	affected, err := orm1.NewRawQuery(session, "UPDATE users SET age = ? WHERE id = ?", 30, 1).Exec(ctx)
func (r *RawQuery) Exec(ctx context.Context) (int64, error) {
	return r.session.backend.ExecRaw(ctx, r.fragment)
}

// ScanOne executes the raw query and scans the first row into dest.
// dest must be a pointer to a struct.
// Returns nil if no rows are found.
//
// Columns are mapped to struct fields by name. By default, field names are
// converted to snake_case for matching (e.g., UserID matches user_id column).
//
// The `orm1:"column:name"` tag can specify a custom column name:
//
//	type Result struct {
//	    UserEmail string `orm1:"column:email"`  // Maps to "email" column
//	    PostCount int64  `orm1:"column:count"`  // Maps to "count" column
//	}
//
// The `orm1:"ignore"` tag excludes fields from scanning:
//
//	type Result struct {
//	    Name     string
//	    Computed string `orm1:"ignore"`  // Not populated from query
//	}
//
// Unmapped columns in the result set are silently ignored.
func (r *RawQuery) ScanOne(ctx context.Context, dest any) error {
	rows, err := r.Rows(ctx)
	if err != nil {
		return err
	}
	defer rows.Close()

	if !rows.Next() {
		return nil
	}

	return r.scanStruct(dest, rows)
}

// ScanAll executes the raw query and scans all rows into dest.
// dest must be a pointer to a slice of struct pointers (*[]*Struct).
//
// Columns are mapped to struct fields by name. By default, field names are
// converted to snake_case for matching (e.g., UserID matches user_id column).
//
// Use the `orm1:"column:name"` tag to specify custom column names, and
// `orm1:"ignore"` to exclude fields. See ScanOne for tag examples.
func (r *RawQuery) ScanAll(ctx context.Context, dest any) error {
	rows, err := r.Rows(ctx)
	if err != nil {
		return err
	}
	defer rows.Close()

	destVal := reflect.ValueOf(dest)
	if destVal.Kind() != reflect.Ptr {
		return fmt.Errorf("ScanAll: dest must be a pointer to slice, got %s", destVal.Type())
	}
	sliceVal := destVal.Elem()
	if sliceVal.Kind() != reflect.Slice {
		return fmt.Errorf("ScanAll: dest must be a pointer to slice, got %s", destVal.Type())
	}

	elemType := sliceVal.Type().Elem()
	if elemType.Kind() != reflect.Ptr {
		return fmt.Errorf("ScanAll: slice elements must be pointers, got %s", elemType)
	}
	structType := elemType.Elem()
	if structType.Kind() != reflect.Struct {
		return fmt.Errorf("ScanAll: slice elements must be pointers to structs, got %s", elemType)
	}

	// Prepare scan metadata once before the loop
	columns, fieldMap, scanDest, err := r.prepareScanMetadata(rows, structType)
	if err != nil {
		return err
	}

	for rows.Next() {
		structPtr := reflect.New(structType).Interface()
		if err := r.scanStructWithMetadata(structPtr, rows, columns, fieldMap, scanDest); err != nil {
			return err
		}
		sliceVal.Set(reflect.Append(sliceVal, reflect.ValueOf(structPtr)))
	}

	return nil
}

// fieldInfo stores minimal field reflection data needed for scanning
type fieldInfo struct {
	typ        reflect.Type
	fieldIndex int
}

// prepareScanMetadata analyzes struct and prepares metadata for scanning.
// This should be called once before scanning multiple rows.
func (r *RawQuery) prepareScanMetadata(rows Rows, structType reflect.Type) ([]string, map[string]fieldInfo, []any, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("prepareScanMetadata: failed to get columns: %w", err)
	}

	fieldsMetadata := AnalyzeStruct(structType)
	fieldMap := make(map[string]fieldInfo, len(fieldsMetadata))

	for _, metadata := range fieldsMetadata {
		if metadata.IgnoreTag {
			continue
		}

		if metadata.ChildTag {
			continue
		}

		// Skip slice and struct pointer fields (likely relationships)
		if metadata.Typ.Kind() == reflect.Slice {
			continue
		}
		if metadata.Typ.Kind() == reflect.Ptr && metadata.Typ.Elem().Kind() == reflect.Struct {
			continue
		}

		columnName := metadata.ColumnTag
		if columnName == "" {
			columnName = metadata.DefaultColumn
		}

		fieldMap[columnName] = fieldInfo{
			typ:        metadata.Typ,
			fieldIndex: metadata.FieldIndex,
		}
	}

	scanDest := make([]any, len(columns))

	return columns, fieldMap, scanDest, nil
}

// scanStructWithMetadata scans a row into a struct pointer using pre-built metadata.
// This is used by ScanAll to avoid repeated struct analysis.
func (r *RawQuery) scanStructWithMetadata(dest any, rows Rows, columns []string, fieldMap map[string]fieldInfo, scanDest []any) error {
	destVal := reflect.ValueOf(dest)
	if destVal.Kind() != reflect.Ptr {
		return fmt.Errorf("scanStructWithMetadata: dest must be a pointer to struct, got %s", destVal.Type())
	}
	structVal := destVal.Elem()
	if structVal.Kind() != reflect.Struct {
		return fmt.Errorf("scanStructWithMetadata: dest must be a pointer to struct, got %s", destVal.Type())
	}

	for i, columnName := range columns {
		if info, ok := fieldMap[columnName]; ok {
			fieldPtr := structVal.Field(info.fieldIndex).Addr().Interface()
			scanDest[i] = fieldPtr
		} else {
			// No matching field - scan into a dummy variable to avoid errors
			var dummy any
			scanDest[i] = &dummy
		}
	}

	return rows.Scan(scanDest...)
}

// scanStruct scans a row into a struct pointer using column-based
// This is used by ScanOne for single-row queries.
func (r *RawQuery) scanStruct(dest any, rows Rows) error {
	destVal := reflect.ValueOf(dest)
	if destVal.Kind() != reflect.Ptr {
		return fmt.Errorf("scanStruct: dest must be a pointer to struct, got %s", destVal.Type())
	}
	structVal := destVal.Elem()
	if structVal.Kind() != reflect.Struct {
		return fmt.Errorf("scanStruct: dest must be a pointer to struct, got %s", destVal.Type())
	}

	structType := structVal.Type()

	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("scanStruct: failed to get columns: %w", err)
	}

	fieldsMetadata := AnalyzeStruct(structType)
	fieldMap := make(map[string]fieldInfo)

	for _, metadata := range fieldsMetadata {
		if metadata.IgnoreTag {
			continue
		}

		if metadata.ChildTag {
			continue
		}

		// Skip slice and struct pointer fields (likely relationships)
		if metadata.Typ.Kind() == reflect.Slice {
			continue
		}
		if metadata.Typ.Kind() == reflect.Ptr && metadata.Typ.Elem().Kind() == reflect.Struct {
			continue
		}

		columnName := metadata.ColumnTag
		if columnName == "" {
			columnName = metadata.DefaultColumn
		}

		fieldMap[columnName] = fieldInfo{
			typ:        metadata.Typ,
			fieldIndex: metadata.FieldIndex,
		}
	}

	scanDest := make([]any, len(columns))
	for i, columnName := range columns {
		if info, ok := fieldMap[columnName]; ok {
			fieldPtr := structVal.Field(info.fieldIndex).Addr().Interface()
			scanDest[i] = fieldPtr
		} else {
			// No matching field - scan into a dummy variable to avoid errors
			var dummy any
			scanDest[i] = &dummy
		}
	}

	return rows.Scan(scanDest...)
}
