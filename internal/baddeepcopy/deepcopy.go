// Package baddeepcopy contains a horribly slow and inefficient, but generic, implementation of DeepCopy.
package baddeepcopy

import (
	"encoding/json"
)

// DeepCopy creates a deep copy of value via JSON marshaling.
// Yes, it's slow. Prove the performance matters with a benchmark before changing it.
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
