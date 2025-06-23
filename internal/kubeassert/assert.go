package kubeassert

import (
	"reflect"

	"github.com/davecgh/go-spew/spew"
	"github.com/pmezard/go-difflib/difflib"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type testingT interface {
	Errorf(format string, args ...any)
}

type testingHelper interface {
	Helper()
}

type resource struct {
	ObjectMeta metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec       any               `json:"spec"`
	Status     any               `json:"status"`
}

// EqualList is like Equal, but asserts for a slice of Kubernetes resources
func EqualList[List ~[]Value, Value client.Object](t testingT, expected, actual List) {
	tryHelper(t)()
	assertOnStatus := false
	var expectedResources []resource
	for _, object := range expected {
		expectedResource := assertableResourceFromObject(object)
		if expectedResource.Status != nil {
			assertOnStatus = true
		}
		expectedResources = append(expectedResources, expectedResource)
	}
	var actualResources []resource
	for _, object := range actual {
		actualResource := assertableResourceFromObject(object)
		if !assertOnStatus {
			actualResource.Status = nil
		}
		actualResources = append(actualResources, actualResource)
	}
	equal(t, expectedResources, actualResources)
}

// Equal asserts Kubernetes resource actual is equal to expected.
// If expected's Status is nil, actual's status is ignored.
func Equal[Value client.Object](t testingT, expected, actual Value) {
	tryHelper(t)()
	expectedResource := assertableResourceFromObject(expected)
	actualResource := assertableResourceFromObject(actual)
	if expectedResource.Status == nil {
		actualResource.Status = nil
	}
	equal(t, expectedResource, actualResource)
}

func tryHelper(t testingT) func() {
	if helper, ok := t.(testingHelper); ok {
		return helper.Helper
	}
	return func() {}
}

func assertableResourceFromObject[Object client.Object](object Object) resource {
	if any(object) == nil {
		return resource{}
	}
	value := reflect.ValueOf(object)
	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}
	var name string
	if object.GetGenerateName() == "" {
		name = object.GetName()
	}
	r := resource{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			GenerateName:    object.GetGenerateName(),
			Namespace:       object.GetNamespace(),
			Labels:          object.GetLabels(),
			Annotations:     object.GetAnnotations(),
			OwnerReferences: assertableOwnerReferences(object.GetOwnerReferences()),
			Finalizers:      object.GetFinalizers(),
		},
	}
	if v := value.FieldByName("Spec"); v.Kind() != reflect.Ptr || !v.IsNil() {
		r.Spec = v.Interface()
	}
	if v := value.FieldByName("Status"); v.Kind() != reflect.Ptr || !v.IsNil() {
		r.Status = v.Interface()
	}
	return r
}

func assertIf(t testingT, condition bool, format string, args ...any) bool {
	tryHelper(t)()
	if !condition {
		t.Errorf(format, args...)
		return false
	}
	return true
}

func equal[Value any](t testingT, expected, actual Value) bool {
	tryHelper(t)()
	expectedStr := dump(expected)
	actualStr := dump(actual)
	diff, err := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
		A:        difflib.SplitLines(expectedStr),
		FromFile: "Expected",
		B:        difflib.SplitLines(actualStr),
		ToFile:   "Actual",
		Context:  1,
	})
	if err != nil {
		t.Errorf(err.Error())
		return false
	}
	return assertIf(t, reflect.DeepEqual(expected, actual), "Not equal:\nexpected: %v\nactual  : %v\nDiff:\n%v", expectedStr, actualStr, diff)
}

func dump(object any) string {
	return (&spew.ConfigState{
		Indent:                  " ",
		DisablePointerAddresses: true,
		DisableCapacities:       true,
		SortKeys:                true,
	}).Sdump(object)
}

func assertableOwnerReferences(ownerRefs []metav1.OwnerReference) []metav1.OwnerReference {
	var assertableRefs []metav1.OwnerReference
	for _, ref := range ownerRefs {
		ref.UID = ""
		assertableRefs = append(assertableRefs, ref)
	}
	return assertableRefs
}
