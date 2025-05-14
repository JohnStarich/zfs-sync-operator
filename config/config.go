// Package config embeds static configuration data for use in envtest.
package config

import (
	"embed"
	"io/fs"
)

//go:embed crd/*
var configFS embed.FS

// FS returns a read-only set of files for statically defined configurations in this directory
func FS() fs.FS {
	return configFS
}
