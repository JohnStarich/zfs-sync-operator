package pool

import (
	"context"
	"slices"
	"time"

	"github.com/pkg/errors"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// SortScheduledSnapshots sorts snapshots by their scheduled timestamp.
// This only applies to PoolSnapshots created by a Pool's snapshots configuration.
// If any snapshot is encountered with an invalid timestamp, the sort fails.
func SortScheduledSnapshots(snapshots []*PoolSnapshot) error {
	var sortErr error
	slices.SortFunc(snapshots, func(a, b *PoolSnapshot) int {
		annotationA, annotationB := a.Annotations[snapshotTimestampAnnotation], b.Annotations[snapshotTimestampAnnotation]
		timeA, err := time.Parse(time.RFC3339, annotationA)
		if sortErr == nil && err != nil {
			sortErr = errors.WithMessagef(err, "pool snapshot %s", b.Name)
			return 0
		}
		timeB, err := time.Parse(time.RFC3339, annotationB)
		if sortErr == nil && err != nil {
			sortErr = errors.WithMessagef(err, "pool snapshot %s", b.Name)
			return 0
		}
		return timeA.Compare(timeB)
	})
	return sortErr
}

func nextSnapshot(ctx context.Context, now time.Time, interval time.Duration, completedSnapshots []*PoolSnapshot) (time.Time, bool, error) {
	logger := log.FromContext(ctx)
	now = now.UTC()
	if len(completedSnapshots) == 0 {
		nextTime := now.Round(interval).Add(interval)
		logger.Info("No completed snapshots, recommending next snapshot time", "nextTime", nextTime)
		return nextTime, true, nil
	}
	last := completedSnapshots[len(completedSnapshots)-1]
	lastTime, err := time.Parse(time.RFC3339, last.Annotations[snapshotTimestampAnnotation])
	if err != nil {
		return time.Time{}, false, err
	}
	nextTime := nextClosestIntervalBeforeNow(interval, lastTime, now)
	beforeNow := nextTime.Before(now) || nextTime.Equal(now)
	logger.Info("Found completed snapshot, recommending next time after interval", "nextTime", nextTime, "beforeNow", beforeNow)
	return nextTime, beforeNow, nil
}

// nextClosestIntervalBeforeNow returns the closest interval after previous but before now.
// If previous is equal to now, then returns now + interval
func nextClosestIntervalBeforeNow(interval time.Duration, previous, now time.Time) time.Time {
	delta := now.Sub(previous)
	if interval == 0 || delta < 0 {
		// These would otherwise be undefined or fatal error behavior. Worst-case this returns now.
		return now
	}
	next := now.Add(-(delta % interval))
	if delta == 0 {
		next = next.Add(interval)
	}
	return next
}
