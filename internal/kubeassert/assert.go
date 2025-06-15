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

func tryHelper(t testingT) func() {
	if helper, ok := t.(testingHelper); ok {
		return helper.Helper
	}
	return func() {}
}

// EqualList asserts Kubernetes resource list actual is equal to expected
func EqualList[List ~[]Value, Value client.Object](t testingT, expected, actual List) {
	tryHelper(t)()
	for i := range min(len(expected), len(actual)) {
		Equal(t, expected[i], actual[i])
	}
	if len(expected) != len(actual) {
		if len(expected) < len(actual) {
			t.Errorf("Extra actual resources: %#v", actual[len(expected):])
		} else {
			t.Errorf("Extra expected resources: %#v", expected[len(actual):])
		}
	}
}

type resource struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              any `json:"spec"`
	Status            any `json:"status"`
}

// Equal asserts Kubernetes resource actual is equal to expected
func Equal[Value client.Object](t testingT, expected, actual Value) {
	tryHelper(t)()
	if !equal(t, isNil(expected), isNil(actual)) {
		t.Errorf("Expected and actual's ==nil result should be the same")
		return
	}

	expectedResource := assertableResourceFromObject(expected)
	actualResource := assertableResourceFromObject(actual)
	equal(t, expectedResource, actualResource)
}

func assertableResourceFromObject[Object client.Object](object Object) resource {
	value := reflect.ValueOf(object)
	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}
	return resource{
		ObjectMeta: metav1.ObjectMeta{
			Name:            object.GetName(),
			Namespace:       object.GetNamespace(),
			Labels:          object.GetLabels(),
			Annotations:     object.GetAnnotations(),
			OwnerReferences: object.GetOwnerReferences(),
			Finalizers:      object.GetFinalizers(),
		},
		Spec:   value.FieldByName("Spec").Interface(),
		Status: value.FieldByName("Status").Interface(),
	}
}

func isNil(value any) bool {
	return value == nil
}

func assertIf(t testingT, condition bool, format string, args ...any) bool {
	tryHelper(t)()
	if !condition {
		t.Errorf(spew.Sprintf(format, args...))
		return false
	}
	return true
}

func equal[Value any](t testingT, expected, actual Value) bool {
	tryHelper(t)()
	diff, err := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
		A:        difflib.SplitLines(dump(expected)),
		FromFile: "Expected",
		B:        difflib.SplitLines(dump(actual)),
		ToFile:   "Actual",
		Context:  1,
	})
	if err != nil {
		t.Errorf(err.Error())
		return false
	}
	return assertIf(t, reflect.DeepEqual(expected, actual), "Not equal:\n%v", diff)
}

func dump(object any) string {
	return (&spew.ConfigState{
		Indent:                  " ",
		DisablePointerAddresses: true,
		DisableCapacities:       true,
		SortKeys:                true,
		DisableMethods:          true,
	}).Sdump(object)
}
