// Package idgen creates ral and test ID generators with a common interface
package idgen

import (
	"strconv"
	"sync/atomic"
	"testing"

	"k8s.io/apimachinery/pkg/util/uuid"
)

// IDGenerator returns new identifiers. Refer to specific implementations for ID characteristics, like uniqueness or determinism.
type IDGenerator interface {
	MustNewID() string
}

// UUID is an ID generator where every ID is a UUID, a universally unique identifier.
//
// For our purposes, this means:
// 1. Sufficiently random IDs to avoid conflict when generated in the same instant
// 2. Uniqueness across time
type UUID struct{}

// New returns a new [UUID]
func New() *UUID {
	return &UUID{}
}

// MustNewID implements [IDGenerator]
func (u *UUID) MustNewID() string {
	return string(uuid.NewUUID())
}

// TestCounter is a test-only ID generator where every ID is an incremented integer from the previous value.
// This must remain test-only to prevent abuse where uniqueness must be guaranteed. Tests may use this for deterministic test results.
type TestCounter struct {
	value atomic.Int64
}

// NewDeterministicTest returns a test-only [TestCounter].
// This must remain test-only to prevent abuse where uniqueness must be guaranteed. Tests may use this for deterministic test results.
func NewDeterministicTest(tb testing.TB) *TestCounter {
	tb.Helper() // require this never runs outside a test
	return &TestCounter{}
}

// MustNewID implements [IDGenerator]
func (c *TestCounter) MustNewID() string {
	return strconv.Itoa(int(c.value.Add(1)))
}
