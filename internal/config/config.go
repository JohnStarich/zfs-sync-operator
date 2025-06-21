// Package config embeds static configuration data for use in envtest.
package config

import (
	"embed"
	"io/fs"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/yaml"
)

//go:embed crd/*
var configFS embed.FS

func CustomResourceDefinitions() ([]*apiextensionsv1.CustomResourceDefinition, error) {
	var crds []*apiextensionsv1.CustomResourceDefinition
	err := fs.WalkDir(configFS, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		contents, err := fs.ReadFile(configFS, path)
		if err != nil {
			return err
		}
		var crd apiextensionsv1.CustomResourceDefinition
		err = yaml.Unmarshal(contents, &crd)
		if err != nil {
			return err
		}
		crds = append(crds, &crd)
		return nil
	})
	return crds, err
}
