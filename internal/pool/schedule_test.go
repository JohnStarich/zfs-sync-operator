package pool

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNextClosestIntervalBeforeNow(t *testing.T) {
	t.Parallel()
	now := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		description string
		interval    time.Duration
		previous    time.Time
		expect      time.Time
	}{
		{
			description: "next is exactly now",
			interval:    1 * time.Hour,
			previous:    now.Add(-1 * time.Hour),
			expect:      now,
		},
		{
			description: "zero interval does not crash", // guard against undefined behavior by locking this in
			interval:    0,
			previous:    now.Add(-1 * time.Hour),
			expect:      now,
		},
		{
			description: "next is just before now",
			interval:    2 * time.Second,
			previous:    now.Add(-3 * time.Second),
			expect:      now.Add(-1 * time.Second),
		},
		{
			description: "previous is way before now but next is just before now",
			interval:    2 * time.Second,
			previous:    now.Add(-121 * time.Second),
			expect:      now.Add(-1 * time.Second),
		},
		{
			description: "previous is now so next adds interval",
			interval:    2 * time.Second,
			previous:    now,
			expect:      now.Add(2 * time.Second),
		},
		{
			description: "previous is in the future returns now", // guard against undefined behavior by locking this in
			interval:    2 * time.Second,
			previous:    now.Add(1 * time.Second),
			expect:      now,
		},
	} {
		t.Run(tc.description, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expect, nextClosestIntervalBeforeNow(tc.interval, tc.previous, now))
		})
	}
}
