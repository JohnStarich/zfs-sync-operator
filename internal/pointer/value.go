package pointer

func Of[Value any](value Value) *Value {
	return &value
}
