package wireguard

import (
	"reflect"

	"gvisor.dev/gvisor/pkg/tcpip"
)

func encodableStats(stats tcpip.Stats) (any, error) {
	return encodableStatCounters(reflect.ValueOf(stats)), nil
}

func encodableStatCounters(value reflect.Value) any {
	switch value.Kind() {
	case reflect.Pointer:
		elem := value.Elem()
		if elem.Kind() == reflect.Struct && value.CanInterface() {
			if value, ok := value.Interface().(*tcpip.StatCounter); ok {
				return value.Value()
			}
			return elem.Type().String()
		}
		return encodableStatCounters(elem)
	case reflect.Struct:
		mapValue := make(map[string]any)
		for _, field := range reflect.VisibleFields(value.Type()) {
			fieldValue := value.FieldByIndex(field.Index)
			newFieldValue := encodableStatCounters(fieldValue)
			if newFieldValue != nil && !reflect.ValueOf(newFieldValue).IsZero() {
				mapValue[field.Name] = newFieldValue
			}
		}
		if len(mapValue) == 0 {
			return nil
		}
		return mapValue
	default:
		return value.Interface()
	}
}
