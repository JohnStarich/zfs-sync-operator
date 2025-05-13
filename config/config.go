package config

import (
	"embed"
	"io/fs"
)

//go:embed crd/*
var configFS embed.FS

func FS() fs.FS {
	return configFS
}
