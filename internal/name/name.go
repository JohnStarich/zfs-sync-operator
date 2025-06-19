// Package name defines constants for the operator to refer to itself
package name

const (
	// Operator is the name of this operator
	Operator = "zfs-sync-operator"

	// Domain is the domain name to associate with the operator
	Domain = Operator + ".johnstarich.com"

	// LabelPrefix is the label prefix to use for an operator property.
	// For example: LabelPrefix + "myproperty" == "mydomain.io/myproperty"
	LabelPrefix = Domain + "/"
)
