package pool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSafelyFormatCommand(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		description string
		args        []string
		expect      string
	}{
		{
			description: "basic name only",
			args:        []string{"name"},
			expect:      `name`,
		},
		{
			description: "complex name",
			args:        []string{"name with spaces/slashes"},
			expect:      `name\ with\ spaces/slashes`,
		},
		{
			description: "multiple args",
			args:        []string{"arg with space", "and/slashes"},
			expect:      `arg\ with\ space and/slashes`,
		},
	} {
		t.Run(tc.description, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expect, safelyFormatCommand(tc.args[0], tc.args[1:]...))
		})
	}
}

func TestShellQuote(t *testing.T) {
	t.Parallel()
	const multiByteRune = '💙'
	assert.Equal(t, 4, len([]byte(string(multiByteRune))))
	for _, tc := range []struct {
		str    string
		expect string
	}{
		{
			str:    `basic`,
			expect: `basic`,
		},
		{
			str:    `with space`,
			expect: `with\ space`,
		},
		{
			str:    `with/slash`,
			expect: `with/slash`,
		},
		{
			str:    `sneaky$variable`,
			expect: `sneaky\$variable`,
		},
		{
			str:    string(multiByteRune),
			expect: `\` + string(multiByteRune),
		},
	} {
		t.Run(tc.str, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expect, shellQuote(tc.str))
		})
	}
}
