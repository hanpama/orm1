package mapping

import (
	"reflect"
	"unsafe"
)

// getFieldPtr returns a pointer to a struct field value using unsafe pointer arithmetic.
// structPtr must be a pointer to a struct.
// fieldType is the reflect.Type of the field.
// byteOffset is the field's byte offset within the struct.
func getFieldPtr(structPtr any, fieldType reflect.Type, byteOffset uintptr) any {
	sv := reflect.ValueOf(structPtr).Elem()
	p := unsafe.Add(unsafe.Pointer(sv.UnsafeAddr()), byteOffset)
	return reflect.NewAt(fieldType, p).Interface()
}

// getFieldValue returns a struct field value using unsafe pointer arithmetic.
// structPtr must be a pointer to a struct.
// fieldType is the reflect.Type of the field.
// byteOffset is the field's byte offset within the struct.
func getFieldValue(structPtr any, fieldType reflect.Type, byteOffset uintptr) any {
	sv := reflect.ValueOf(structPtr).Elem()
	p := unsafe.Add(unsafe.Pointer(sv.UnsafeAddr()), byteOffset)
	return reflect.NewAt(fieldType, p).Elem().Interface()
}

// setFieldValue sets a struct field value using unsafe pointer arithmetic.
// structPtr must be a pointer to a struct.
// fieldType is the reflect.Type of the field.
// byteOffset is the field's byte offset within the struct.
// value is the new value to set.
func setFieldValue(structPtr any, fieldType reflect.Type, byteOffset uintptr, value any) {
	sv := reflect.ValueOf(structPtr).Elem()
	p := unsafe.Add(unsafe.Pointer(sv.UnsafeAddr()), byteOffset)
	fieldPtr := reflect.NewAt(fieldType, p).Elem()
	fieldPtr.Set(reflect.ValueOf(value))
}

// getPtrFieldValue returns the value of a pointer field (*T).
// structPtr must be a pointer to a struct.
// ptrFieldType is the reflect.Type of the field (must be *T).
// byteOffset is the field's byte offset within the struct.
// Returns the pointer value, or nil if the pointer is nil.
// Note: Returns untyped nil (not typed nil like (*T)(nil)).
func getPtrFieldValue(structPtr any, ptrFieldType reflect.Type, byteOffset uintptr) any {
	sv := reflect.ValueOf(structPtr).Elem()
	p := unsafe.Add(unsafe.Pointer(sv.UnsafeAddr()), byteOffset)
	fieldPtr := reflect.NewAt(ptrFieldType, p).Elem()

	// Check for nil pointer and return untyped nil
	if fieldPtr.IsNil() {
		return nil
	}
	return fieldPtr.Interface()
}

// getSliceFieldValues returns the values from a slice field ([]*T or []T).
// structPtr must be a pointer to a struct.
// sliceFieldType is the reflect.Type of the field (must be slice type).
// byteOffset is the field's byte offset within the struct.
// Returns []any containing all slice elements (requires reflect for type conversion).
func getSliceFieldValues(structPtr any, sliceFieldType reflect.Type, byteOffset uintptr) []any {
	sv := reflect.ValueOf(structPtr).Elem()
	p := unsafe.Add(unsafe.Pointer(sv.UnsafeAddr()), byteOffset)
	fieldPtr := reflect.NewAt(sliceFieldType, p).Elem()

	sliceLen := fieldPtr.Len()
	result := make([]any, sliceLen)
	for i := 0; i < sliceLen; i++ {
		result[i] = fieldPtr.Index(i).Interface()
	}
	return result
}

// setPtrFieldValue sets a pointer field (*T) to the given value.
// structPtr must be a pointer to a struct.
// ptrFieldType is the reflect.Type of the field (must be *T).
// byteOffset is the field's byte offset within the struct.
// value is the value to set (may be nil).
func setPtrFieldValue(structPtr any, ptrFieldType reflect.Type, byteOffset uintptr, value any) {
	sv := reflect.ValueOf(structPtr).Elem()
	p := unsafe.Add(unsafe.Pointer(sv.UnsafeAddr()), byteOffset)
	fieldPtr := reflect.NewAt(ptrFieldType, p).Elem()

	if value == nil {
		fieldPtr.Set(reflect.Zero(ptrFieldType))
	} else {
		fieldPtr.Set(reflect.ValueOf(value))
	}
}

// setSliceFieldValues sets a slice field ([]*T or []T) to the given values.
// structPtr must be a pointer to a struct.
// sliceFieldType is the reflect.Type of the field (must be slice type).
// byteOffset is the field's byte offset within the struct.
// values is the slice of values to set (requires reflect for type conversion).
func setSliceFieldValues(structPtr any, sliceFieldType reflect.Type, byteOffset uintptr, values []any) {
	sv := reflect.ValueOf(structPtr).Elem()
	p := unsafe.Add(unsafe.Pointer(sv.UnsafeAddr()), byteOffset)
	fieldPtr := reflect.NewAt(sliceFieldType, p).Elem()

	sliceVal := reflect.MakeSlice(sliceFieldType, len(values), len(values))
	for i, val := range values {
		sliceVal.Index(i).Set(reflect.ValueOf(val))
	}
	fieldPtr.Set(sliceVal)
}
