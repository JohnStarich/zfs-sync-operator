// Package clock implements a real and test clock, useful for swapping in tests for tightly controlled time.
package clock

import (
	"sync/atomic"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Clock can tell time
type Clock interface {
	Now() time.Time
}

// Real is a [Clock] that always returns the current time
type Real struct{}

// Now implements [Clock]
func (r *Real) Now() time.Time { return time.Now() }

// Test is a static time [Clock] for use in tests
type Test struct {
	time atomic.Pointer[time.Time]
}

// NewTest returns a new test clock set to 2000-01-01T00:00:00Z
func NewTest() *Test {
	clock := &Test{}
	t := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	clock.time.Store(&t)
	return clock
}

// Now implements [Clock]
func (c *Test) Now() time.Time {
	return *c.time.Load()
}

// RelativeTime returns the test's clock time in UTC after adding 'add'
func (c *Test) RelativeTime(add time.Duration) time.Time {
	return c.Now().UTC().Add(add)
}

// RoundedRelativeTime returns the test's clock time in UTC after rounding to 'round' and then adding 'add'
func (c *Test) RoundedRelativeTime(round, add time.Duration) time.Time {
	return c.Now().UTC().Round(round).Add(add)
}

// RelativeMetaV1Time is like TestRelativeTime but returns a [metav1.Time] in local time
func (c *Test) RelativeMetaV1Time(add time.Duration) metav1.Time {
	return metav1.Time{Time: c.RelativeTime(add).Local()}
}

// RoundedRelativeMetaV1Time is like TestRoundedRelativeTime but returns a [metav1.Time] in local time
func (c *Test) RoundedRelativeMetaV1Time(round, add time.Duration) metav1.Time {
	return metav1.Time{Time: c.RoundedRelativeTime(round, add).Local()}
}

// Add adds 'd' to the current clock c's time
func (c *Test) Add(d time.Duration) {
	newTime := c.time.Load().Add(d)
	c.time.Store(&newTime)
}
