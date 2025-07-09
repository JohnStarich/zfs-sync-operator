package pool

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNextSnapshotTime(t *testing.T) {
	t.Parallel()
	now := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		description string
		interval    time.Duration
		previous    time.Time
		expect      time.Time
	}{
		{
			description: "within 1 interval of now returns previous+interval",
			interval:    1 * time.Hour,
			previous:    now.Add(-1 * time.Hour),
			expect:      now,
		},
		{
			description: "within 2 intervals of now returns previous+interval",
			interval:    1 * time.Hour,
			previous:    now.Add(-2 * time.Hour),
			expect:      now.Add(-1 * time.Hour),
		},
		{
			description: "greater than 2 intervals before now returns nearest rounded interval before now",
			interval:    2 * time.Second,
			previous:    now.Add(-7 * time.Second),
			expect:      now.Add(-1 * time.Second),
		},
	} {
		t.Run(tc.description, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expect, nextSnapshotTime(tc.interval, tc.previous, now))
		})
	}
}
