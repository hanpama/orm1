// Package key provides the Key type for representing entity keys.
//
// Key is a comparable interface that can be used as a map key,
// enabling efficient lookups without serialization overhead.
// It supports composite keys with up to 9 column values.
package key

// Key represents an entity's primary or foreign key value(s).
// It is a comparable interface that can be used as a map key.
//
// Examples:
//
//	key.NewKey(1)              // single-column key
//	key.NewKey(orderId, itemId) // composite key
type Key interface {
	keyMarker()
	Length() int
	At(index int) any
}

// key0 represents an empty key (used internally)
type key0 struct{}

func (k key0) keyMarker()       {}
func (k key0) Length() int      { return 0 }
func (k key0) At(index int) any { panic("key0: index out of range") }

// key1 represents a single-column key
type key1 struct{ V0 any }

func (k key1) keyMarker()       {}
func (k key1) Length() int      { return 1 }
func (k key1) At(index int) any {
	if index == 0 {
		return k.V0
	}
	panic("key1: index out of range")
}

// key2 represents a 2-column composite key
type key2 struct{ V0, V1 any }

func (k key2) keyMarker()       {}
func (k key2) Length() int      { return 2 }
func (k key2) At(index int) any {
	switch index {
	case 0:
		return k.V0
	case 1:
		return k.V1
	default:
		panic("key2: index out of range")
	}
}

// key3 represents a 3-column composite key
type key3 struct{ V0, V1, V2 any }

func (k key3) keyMarker()       {}
func (k key3) Length() int      { return 3 }
func (k key3) At(index int) any {
	switch index {
	case 0:
		return k.V0
	case 1:
		return k.V1
	case 2:
		return k.V2
	default:
		panic("key3: index out of range")
	}
}

// key4 represents a 4-column composite key
type key4 struct{ V0, V1, V2, V3 any }

func (k key4) keyMarker()       {}
func (k key4) Length() int      { return 4 }
func (k key4) At(index int) any {
	switch index {
	case 0:
		return k.V0
	case 1:
		return k.V1
	case 2:
		return k.V2
	case 3:
		return k.V3
	default:
		panic("key4: index out of range")
	}
}

// key5 represents a 5-column composite key
type key5 struct{ V0, V1, V2, V3, V4 any }

func (k key5) keyMarker()       {}
func (k key5) Length() int      { return 5 }
func (k key5) At(index int) any {
	switch index {
	case 0:
		return k.V0
	case 1:
		return k.V1
	case 2:
		return k.V2
	case 3:
		return k.V3
	case 4:
		return k.V4
	default:
		panic("key5: index out of range")
	}
}

// key6 represents a 6-column composite key
type key6 struct{ V0, V1, V2, V3, V4, V5 any }

func (k key6) keyMarker()       {}
func (k key6) Length() int      { return 6 }
func (k key6) At(index int) any {
	switch index {
	case 0:
		return k.V0
	case 1:
		return k.V1
	case 2:
		return k.V2
	case 3:
		return k.V3
	case 4:
		return k.V4
	case 5:
		return k.V5
	default:
		panic("key6: index out of range")
	}
}

// key7 represents a 7-column composite key
type key7 struct{ V0, V1, V2, V3, V4, V5, V6 any }

func (k key7) keyMarker()       {}
func (k key7) Length() int      { return 7 }
func (k key7) At(index int) any {
	switch index {
	case 0:
		return k.V0
	case 1:
		return k.V1
	case 2:
		return k.V2
	case 3:
		return k.V3
	case 4:
		return k.V4
	case 5:
		return k.V5
	case 6:
		return k.V6
	default:
		panic("key7: index out of range")
	}
}

// key8 represents an 8-column composite key
type key8 struct{ V0, V1, V2, V3, V4, V5, V6, V7 any }

func (k key8) keyMarker()       {}
func (k key8) Length() int      { return 8 }
func (k key8) At(index int) any {
	switch index {
	case 0:
		return k.V0
	case 1:
		return k.V1
	case 2:
		return k.V2
	case 3:
		return k.V3
	case 4:
		return k.V4
	case 5:
		return k.V5
	case 6:
		return k.V6
	case 7:
		return k.V7
	default:
		panic("key8: index out of range")
	}
}

// key9 represents a 9-column composite key
type key9 struct{ V0, V1, V2, V3, V4, V5, V6, V7, V8 any }

func (k key9) keyMarker()       {}
func (k key9) Length() int      { return 9 }
func (k key9) At(index int) any {
	switch index {
	case 0:
		return k.V0
	case 1:
		return k.V1
	case 2:
		return k.V2
	case 3:
		return k.V3
	case 4:
		return k.V4
	case 5:
		return k.V5
	case 6:
		return k.V6
	case 7:
		return k.V7
	case 8:
		return k.V8
	default:
		panic("key9: index out of range")
	}
}

// NewKey creates a new Key from the given values.
// Supports up to 9 column values.
//
// This is the public API for creating keys with arbitrary values.
func NewKey(vals ...any) Key {
	switch len(vals) {
	case 0:
		return key0{}
	case 1:
		return key1{V0: vals[0]}
	case 2:
		return key2{V0: vals[0], V1: vals[1]}
	case 3:
		return key3{V0: vals[0], V1: vals[1], V2: vals[2]}
	case 4:
		return key4{V0: vals[0], V1: vals[1], V2: vals[2], V3: vals[3]}
	case 5:
		return key5{V0: vals[0], V1: vals[1], V2: vals[2], V3: vals[3], V4: vals[4]}
	case 6:
		return key6{V0: vals[0], V1: vals[1], V2: vals[2], V3: vals[3], V4: vals[4], V5: vals[5]}
	case 7:
		return key7{V0: vals[0], V1: vals[1], V2: vals[2], V3: vals[3], V4: vals[4], V5: vals[5], V6: vals[6]}
	case 8:
		return key8{V0: vals[0], V1: vals[1], V2: vals[2], V3: vals[3], V4: vals[4], V5: vals[5], V6: vals[6], V7: vals[7]}
	case 9:
		return key9{V0: vals[0], V1: vals[1], V2: vals[2], V3: vals[3], V4: vals[4], V5: vals[5], V6: vals[6], V7: vals[7], V8: vals[8]}
	default:
		panic("orm1/key: Key supports up to 9 column values")
	}
}

// New0 creates an empty key (used internally).
func New0() Key {
	return key0{}
}

// New1 creates a single-column key without varargs allocation.
func New1(v0 any) Key {
	return key1{V0: v0}
}

// New2 creates a 2-column composite key without varargs allocation.
func New2(v0, v1 any) Key {
	return key2{V0: v0, V1: v1}
}

// New3 creates a 3-column composite key without varargs allocation.
func New3(v0, v1, v2 any) Key {
	return key3{V0: v0, V1: v1, V2: v2}
}

// New4 creates a 4-column composite key without varargs allocation.
func New4(v0, v1, v2, v3 any) Key {
	return key4{V0: v0, V1: v1, V2: v2, V3: v3}
}

// New5 creates a 5-column composite key without varargs allocation.
func New5(v0, v1, v2, v3, v4 any) Key {
	return key5{V0: v0, V1: v1, V2: v2, V3: v3, V4: v4}
}

// New6 creates a 6-column composite key without varargs allocation.
func New6(v0, v1, v2, v3, v4, v5 any) Key {
	return key6{V0: v0, V1: v1, V2: v2, V3: v3, V4: v4, V5: v5}
}

// New7 creates a 7-column composite key without varargs allocation.
func New7(v0, v1, v2, v3, v4, v5, v6 any) Key {
	return key7{V0: v0, V1: v1, V2: v2, V3: v3, V4: v4, V5: v5, V6: v6}
}

// New8 creates an 8-column composite key without varargs allocation.
func New8(v0, v1, v2, v3, v4, v5, v6, v7 any) Key {
	return key8{V0: v0, V1: v1, V2: v2, V3: v3, V4: v4, V5: v5, V6: v6, V7: v7}
}

// New9 creates a 9-column composite key without varargs allocation.
func New9(v0, v1, v2, v3, v4, v5, v6, v7, v8 any) Key {
	return key9{V0: v0, V1: v1, V2: v2, V3: v3, V4: v4, V5: v5, V6: v6, V7: v7, V8: v8}
}
