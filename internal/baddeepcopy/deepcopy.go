package baddeepcopy

import (
	"encoding/json"
)

func DeepCopy[Value any](value Value) Value {
	copiedValue, err := deepCopy(value)
	if err != nil {
		panic(err)
	}
	return copiedValue
}

func deepCopy[Value any](value Value) (Value, error) {
	var copiedValue Value
	data, err := json.Marshal(value)
	if err == nil {
		err = json.Unmarshal(data, &copiedValue)
	}
	return copiedValue, err
}
