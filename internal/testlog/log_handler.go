package testlog

import (
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func NewLogHandler(tb testing.TB, level slog.Level) *slog.TextHandler {
	var writer io.Writer = NewWriter(tb)
	writer = &unrollQuotedMultiLineStringsWriter{writer: writer}
	moduleDirPrefix := filepath.Join(currentFile(tb), "..", "..", "..") + string(filepath.Separator)
	return slog.NewTextHandler(writer, &slog.HandlerOptions{
		AddSource: true,
		Level:     level,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if len(groups) > 0 {
				return a
			}
			switch a.Key {
			case slog.TimeKey, slog.LevelKey:
				return slog.Attr{}
			case slog.SourceKey:
				switch source := a.Value.Any().(type) {
				case *slog.Source:
					if !strings.HasPrefix(source.File, moduleDirPrefix) {
						return slog.Attr{}
					}
					a.Key = "s"
					source.File = strings.TrimPrefix(source.File, moduleDirPrefix)
				}
				return a
			case "err":
				switch err := a.Value.Any().(type) {
				case error:
					a.Value = slog.StringValue(fmt.Sprintf("%+v", err))
				}
				return a
			default:
				return a
			}
		},
	})
}

func currentFile(tb testing.TB) string {
	_, file, _, ok := runtime.Caller(0)
	require.True(tb, ok)
	absoluteFilePath, err := filepath.Abs(file)
	require.NoError(tb, err)
	return absoluteFilePath
}

type unrollQuotedMultiLineStringsWriter struct {
	writer io.Writer
}

func (w *unrollQuotedMultiLineStringsWriter) Write(b []byte) (int, error) {
	b = []byte(unquoteStringsInline(string(b), "\n"))
	return w.writer.Write(b)
}

func unquoteStringsInline(s, mustContain string) string {
	const separator = "="
	quotedCandidates := strings.SplitAfter(s, separator)
	for i, candidate := range quotedCandidates {
		if quoted, err := strconv.QuotedPrefix(candidate); err == nil {
			unquoted, err := strconv.Unquote(quoted)
			if err == nil && strings.Contains(unquoted, mustContain) {
				quotedCandidates[i] = unquoted + candidate[len(quoted):]
			}
		}
	}
	return strings.Join(quotedCandidates, "")
}
