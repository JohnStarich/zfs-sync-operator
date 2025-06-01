package testlog

import (
	"log/slog"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func NewLogHandler(tb testing.TB) *slog.TextHandler {
	testWriter := NewWriter(tb)
	moduleDirPrefix := filepath.Join(currentFile(tb), "..", "..", "..") + string(filepath.Separator)
	return slog.NewTextHandler(testWriter, &slog.HandlerOptions{
		AddSource: true,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if len(groups) > 0 {
				return a
			}
			switch a.Key {
			case "time", "level":
				return slog.Attr{}
			case "source":
				switch source := a.Value.Any().(type) {
				case *slog.Source:
					if !strings.HasPrefix(source.File, moduleDirPrefix) {
						return slog.Attr{}
					}
					source.File = strings.TrimPrefix(source.File, moduleDirPrefix)
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
