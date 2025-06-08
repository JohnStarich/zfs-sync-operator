// Package pointer helps deal with creating or dereferencing pointers.
// Largely this is to test the common functions and enforce patterns.
package pointer

// Of returns a pointer to value
func Of[Value any](value Value) *Value {
	return &value
}
