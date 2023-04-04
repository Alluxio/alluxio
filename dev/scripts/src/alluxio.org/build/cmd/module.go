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
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/palantir/stacktrace"
	"gopkg.in/yaml.v3"
)

// LibModule represents a module built as part of the maven build for the project but is not strictly necessary for Alluxio to run
type LibModule struct {
	GeneratedJarPath string   `yaml:"generatedJarPath"` // relative path from repo root of plugin jar built by maven; requires replacement of version string
	BundleWith       []string `yaml:"bundleWith"`       // list of bundle names to associate with
}

func (m *LibModule) copyFileForTarball(repoBuildDir, dstDir, alluxioVersion string) error {
	// GeneratedJarPath is where the jar will be built after the maven command completes
	// Copy the generated jar to the tarball along the same relative path
	src := filepath.Join(repoBuildDir, strings.ReplaceAll(m.GeneratedJarPath, versionPlaceholder, alluxioVersion))
	dst := filepath.Join(dstDir, strings.ReplaceAll(m.GeneratedJarPath, versionPlaceholder, alluxioVersion))
	if err := copyFileForTarball(src, dst); err != nil {
		return stacktrace.Propagate(err, "error copying module")
	}
	return nil
}

// PluginModule represents an additional module that can be built and added into the tarball
type PluginModule struct {
	ModuleType       string   `yaml:"moduleType"`       // module type, ex. hdfs
	EnumName         string   `yaml:"enumName"`         // for modules with multiple versions, ensure the enum file lists the version
	MavenArgs        string   `yaml:"mavenArgs"`        // plugin module specific arguments to build via maven
	GeneratedJarPath string   `yaml:"generatedJarPath"` // relative path from repo root of plugin jar built by maven; requires replacement of version string
	TarballJarPath   string   `yaml:"tarballJarPath"`   // relative path from tarball root to copy plugin jar into tarball; requires replacement of version string
	BundleWith       []string `yaml:"bundleWith"`       // list of bundle names to associate with
}

func (m *PluginModule) copyFileForTarball(repoBuildDir, dstDir, alluxioVersion string) error {
	// GeneratedJarPath is where the jar will be built after the maven command completes.
	// The generated jar is expected be renamed to this path after being built in the build directory
	//   and should be copied to the tarball along the same updated relative path
	src := filepath.Join(repoBuildDir, strings.ReplaceAll(m.TarballJarPath, versionPlaceholder, alluxioVersion))
	dst := filepath.Join(dstDir, strings.ReplaceAll(m.TarballJarPath, versionPlaceholder, alluxioVersion))
	if err := copyFileForTarball(src, dst); err != nil {
		return stacktrace.Propagate(err, "error copying module")
	}
	return nil
}

type ModulesFile struct {
	LibModules    map[string]*LibModule    `yaml:"libModules"`
	PluginModules map[string]*PluginModule `yaml:"pluginModules"`
}

type modulesInfo struct {
	ModulesFile

	libBundles    map[string][]string // bundle name -> lib names
	pluginBundles map[string][]string // bundle name -> plugin names
}

func loadModules(modulesYml string) (*modulesInfo, error) {
	wd, err := os.Getwd()
	if err != nil {
		return nil, stacktrace.Propagate(err, "error getting current working directory")
	}
	pluginModulesPath := filepath.Join(wd, modulesYml)
	log.Printf("Reading modules from %v", pluginModulesPath)
	content, err := ioutil.ReadFile(pluginModulesPath)
	if err != nil {
		return nil, stacktrace.Propagate(err, "error reading file at %v", pluginModulesPath)
	}
	modules := ModulesFile{}
	if err := yaml.Unmarshal(content, &modules); err != nil {
		return nil, stacktrace.Propagate(err, "error unmarshalling modules from:\n%v", string(content))
	}
	libBundles, pluginBundles := map[string][]string{}, map[string][]string{}
	for n, l := range modules.LibModules {
		for _, b := range l.BundleWith {
			log.Printf("Loading lib module for %v", n)
			libBundles[b] = append(libBundles[b], n)
		}
	}
	for n, p := range modules.PluginModules {
		for _, b := range p.BundleWith {
			log.Printf("Loading plugin module for %v", n)
			pluginBundles[b] = append(pluginBundles[b], n)
		}
	}
	return &modulesInfo{
		ModulesFile:   modules,
		libBundles:    libBundles,
		pluginBundles: pluginBundles,
	}, nil
}

func PluginsF(args []string) error {
	ret := &buildOpts{}
	cmd := flag.NewFlagSet(Modules, flag.ExitOnError)
	cmd.StringVar(&ret.modulesFile, "modulesFile", defaultModulesFilePath, `path to modules.yml file`)
	if err := cmd.Parse(args); err != nil {
		return stacktrace.Propagate(err, "error parsing flags")
	}
	modules, err := loadModules(ret.modulesFile)
	if err != nil {
		return stacktrace.Propagate(err, "error loading plugin modules from file at %v", ret.modulesFile)
	}
	fmt.Println()
	{
		bundlesContent, err := yaml.Marshal(modules.libBundles)
		if err != nil {
			return stacktrace.Propagate(err, "error serializing %v", modules.libBundles)
		}
		var names []string
		for n := range modules.LibModules {
			names = append(names, n)
		}
		sort.Strings(names)
		namesContent, err := yaml.Marshal(names)
		if err != nil {
			return stacktrace.Propagate(err, "error serializing %v", names)
		}

		fmt.Println("Lib module names:")
		fmt.Println(string(namesContent))
		fmt.Println("Lib module bundles:")
		fmt.Println(string(bundlesContent))
	}
	{
		bundlesContent, err := yaml.Marshal(modules.pluginBundles)
		if err != nil {
			return stacktrace.Propagate(err, "error serializing %v", modules.pluginBundles)
		}
		var names []string
		for n := range modules.PluginModules {
			names = append(names, n)
		}
		sort.Strings(names)
		namesContent, err := yaml.Marshal(names)
		if err != nil {
			return stacktrace.Propagate(err, "error serializing %v", names)
		}

		fmt.Println("Plugin module names:")
		fmt.Println(string(namesContent))
		fmt.Println("Plugin module bundles:")
		fmt.Println(string(bundlesContent))
	}
	return nil
}
