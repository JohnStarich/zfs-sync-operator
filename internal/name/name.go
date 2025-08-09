// Package name defines constants for the operator to refer to itself
package name

const (
	// Operator is the name of this operator
	Operator = "zfs-sync-operator"

	// Domain is the domain name to associate with the operator
	Domain = Operator + ".johnstarich.com"

	// DomainPrefix is the label prefix to use for an operator property.
	// For example: DomainPrefix + "myproperty" == "mydomain.io/myproperty"
	DomainPrefix = Domain + "/"

	// Metrics is the Prometheus namespace for this operator
	Metrics = "zfs_sync"
)
