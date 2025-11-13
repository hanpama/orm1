package key

import "testing"

func TestNewN(t *testing.T) {
	tests := []struct {
		name   string
		create func() Key
		length int
		values []any
	}{
		{"New0", func() Key { return New0() }, 0, []any{}},
		{"New1", func() Key { return New1(1) }, 1, []any{1}},
		{"New2", func() Key { return New2(1, 2) }, 2, []any{1, 2}},
		{"New3", func() Key { return New3(1, 2, 3) }, 3, []any{1, 2, 3}},
		{"New4", func() Key { return New4(1, 2, 3, 4) }, 4, []any{1, 2, 3, 4}},
		{"New5", func() Key { return New5(1, 2, 3, 4, 5) }, 5, []any{1, 2, 3, 4, 5}},
		{"New6", func() Key { return New6(1, 2, 3, 4, 5, 6) }, 6, []any{1, 2, 3, 4, 5, 6}},
		{"New7", func() Key { return New7(1, 2, 3, 4, 5, 6, 7) }, 7, []any{1, 2, 3, 4, 5, 6, 7}},
		{"New8", func() Key { return New8(1, 2, 3, 4, 5, 6, 7, 8) }, 8, []any{1, 2, 3, 4, 5, 6, 7, 8}},
		{"New9", func() Key { return New9(1, 2, 3, 4, 5, 6, 7, 8, 9) }, 9, []any{1, 2, 3, 4, 5, 6, 7, 8, 9}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := tt.create()

			if k.Length() != tt.length {
				t.Errorf("expected length %d, got %d", tt.length, k.Length())
			}

			for i := 0; i < tt.length; i++ {
				if k.At(i) != tt.values[i] {
					t.Errorf("expected At(%d)=%v, got %v", i, tt.values[i], k.At(i))
				}
			}

			k.keyMarker() // Should not panic
		})
	}
}

func TestKeyAt_OutOfBounds_Panics(t *testing.T) {
	tests := []struct {
		name   string
		create func() Key
		index  int
	}{
		{"New0", func() Key { return New0() }, 0},
		{"New1", func() Key { return New1(1) }, 1},
		{"New2", func() Key { return New2(1, 2) }, 2},
		{"New3", func() Key { return New3(1, 2, 3) }, 3},
		{"New4", func() Key { return New4(1, 2, 3, 4) }, 4},
		{"New5", func() Key { return New5(1, 2, 3, 4, 5) }, 5},
		{"New6", func() Key { return New6(1, 2, 3, 4, 5, 6) }, 6},
		{"New7", func() Key { return New7(1, 2, 3, 4, 5, 6, 7) }, 7},
		{"New8", func() Key { return New8(1, 2, 3, 4, 5, 6, 7, 8) }, 8},
		{"New9", func() Key { return New9(1, 2, 3, 4, 5, 6, 7, 8, 9) }, 9},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("expected panic when accessing At(%d)", tt.index)
				}
			}()
			k := tt.create()
			_ = k.At(tt.index)
		})
	}
}

func TestNewKey(t *testing.T) {
	tests := []struct {
		name   string
		vals   []any
		length int
	}{
		{"empty", []any{}, 0},
		{"single", []any{1}, 1},
		{"two", []any{1, 2}, 2},
		{"three", []any{1, 2, 3}, 3},
		{"four", []any{1, 2, 3, 4}, 4},
		{"five", []any{1, 2, 3, 4, 5}, 5},
		{"six", []any{1, 2, 3, 4, 5, 6}, 6},
		{"seven", []any{1, 2, 3, 4, 5, 6, 7}, 7},
		{"eight", []any{1, 2, 3, 4, 5, 6, 7, 8}, 8},
		{"nine", []any{1, 2, 3, 4, 5, 6, 7, 8, 9}, 9},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := NewKey(tt.vals...)
			if k.Length() != tt.length {
				t.Errorf("expected length %d, got %d", tt.length, k.Length())
			}
			for i := 0; i < tt.length; i++ {
				if k.At(i) != tt.vals[i] {
					t.Errorf("expected At(%d)=%v, got %v", i, tt.vals[i], k.At(i))
				}
			}
		})
	}
}

func TestNewKey_TooManyValues_Panics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic when creating key with more than 9 values")
		}
	}()
	_ = NewKey(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
}

func TestKey_AsMapKey(t *testing.T) {
	tests := []struct {
		name     string
		key      Key
		expected string
	}{
		{"k1", New1(42), "value1"},
		{"k2", New2(1, 2), "value2"},
		{"k3_same_as_k1", New1(42), "value1"},
	}

	m := make(map[Key]string)
	m[New1(42)] = "value1"
	m[New2(1, 2)] = "value2"

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := m[tt.key]; got != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, got)
			}
		})
	}
}
