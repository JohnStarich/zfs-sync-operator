package idgen

import (
	"strconv"
	"sync/atomic"
	"testing"

	"k8s.io/apimachinery/pkg/util/uuid"
)

type IDGenerator interface {
	MustNewID() string
}

type UUID struct{}

func New() *UUID {
	return &UUID{}
}

func (u *UUID) MustNewID() string {
	return string(uuid.NewUUID())
}

type TestCounter struct {
	value atomic.Int64
}

func NewDeterministicTest(tb testing.TB) *TestCounter {
	tb.Helper() // require this never runs outside a test
	return &TestCounter{}
}

func (c *TestCounter) MustNewID() string {
	return strconv.Itoa(int(c.value.Add(1)))
}
