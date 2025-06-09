package pool

import (
	"strings"
	"unicode"
)

func safelyFormatCommand(name string, args ...string) string {
	var b strings.Builder
	b.WriteString(shellQuote(name))
	for _, arg := range args {
		b.WriteRune(' ')
		b.WriteString(shellQuote(arg))
	}
	return b.String()
}

func shellQuote(str string) string {
	var builder strings.Builder
	for _, r := range str {
		if shouldQuote(r) {
			const escapeBackslash = '\\'
			builder.WriteRune(escapeBackslash)
		}
		builder.WriteRune(r)
	}
	return builder.String()
}

// shouldQuote conservatively quotes most runes, save some basic readability of ASCII compatible runes
func shouldQuote(r rune) bool {
	if r > unicode.MaxASCII {
		return true
	}
	return !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '/' && r != '-'
}
