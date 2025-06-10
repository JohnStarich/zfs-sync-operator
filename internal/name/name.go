// Package name defines constants for the operator to refer to itself
package name

const (
	// Operator is the name of this operator
	Operator = "zfs-sync-operator"

	// Domain is the domain name to associate with the operator
	Domain = Operator + ".johnstarich.com"
)

// Label returns an appropriate label key for the given property
func Label(property string) string {
	return Domain + "/" + property
}
