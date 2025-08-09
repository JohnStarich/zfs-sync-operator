// Package metrics helps register metrics, annotate them with common labels, and assist in assigning them values
package metrics

import "github.com/prometheus/client_golang/prometheus"

// Common metric labels
const (
	NamespaceLabel = "namespace"
	NameLabel      = "name"
	StateLabel     = "state"
)

// MustRegister ensures collector is registered to regitry and returns the collector.
// Primarily used for inline registration and assignment.
func MustRegister[Collector prometheus.Collector](registry prometheus.Registerer, collector Collector) Collector {
	registry.MustRegister(collector)
	return collector
}

// CountTrue returns 1 if b is true, 0 otherwise
func CountTrue(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
