package backup

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// schemeGroupVersion is group version used to register these objects
	schemeGroupVersion = schema.GroupVersion{Group: "zfs-sync-operator.johnstarich.com", Version: "v1alpha1"}

	// schemeBuilder is used to add go types to the GroupVersionKind scheme
	schemeBuilder = &scheme.Builder{GroupVersion: schemeGroupVersion}
)

func MustScheme() *runtime.Scheme {
	scheme, err := schemeBuilder.Build()
	if err != nil {
		panic(err)
	}
	return scheme
}
