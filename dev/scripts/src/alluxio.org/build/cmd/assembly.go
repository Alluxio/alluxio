/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package cmd

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/palantir/stacktrace"
	"gopkg.in/yaml.v3"
)

type AssemblyJar struct {
	GeneratedJarPath string                       `yaml:"generatedJarPath"` // relative path of generated jar, before formatting with alluxio version string
	TarballJarPath   string                       `yaml:"tarballJarPath"`   // relative path of copied jar into the tarball, before formatting with alluxio version string
	FileReplacements map[string]map[string]string `yaml:"fileReplacements"` // location of file to execute replacement -> find -> replace
}

type AssemblyJars map[string]*AssemblyJar

func loadAssemblyJars(assemblyYml string) (AssemblyJars, error) {
	wd, err := os.Getwd()
	if err != nil {
		return nil, stacktrace.Propagate(err, "error getting current working directory")
	}
	assemblyYmlPath := filepath.Join(wd, assemblyYml)
	log.Printf("Reading assembly jars from %v", assemblyYmlPath)
	content, err := ioutil.ReadFile(assemblyYmlPath)
	if err != nil {
		return nil, stacktrace.Propagate(err, "error reading file at %v", assemblyYmlPath)
	}
	var assemblyJars AssemblyJars
	if err := yaml.Unmarshal(content, &assemblyJars); err != nil {
		return nil, stacktrace.Propagate(err, "error unmarshalling assembly jars from:\n%v", string(content))
	}
	return assemblyJars, nil
}
