package testlog

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnquoteStringsInline(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		in          string
		mustContain string
		expect      string
	}{
		{in: `foo=bar`, expect: `foo=bar`},
		{in: `foo="bar"`, expect: `foo=bar`},
		{in: `foo="bar\nbaz"`, expect: `foo=bar
baz`},
		{in: `foo="bar" baz="biff"`, mustContain: "bi", expect: `foo="bar" baz=biff`},
		{in: `foo="bar" baz="biff\nboo"`, mustContain: "\n", expect: `foo="bar" baz=biff
boo`},
	} {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expect, unquoteStringsInline(tc.in, tc.mustContain))
		})
	}
}
