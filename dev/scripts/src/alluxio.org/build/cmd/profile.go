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
	"github.com/palantir/stacktrace"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
)

type Profile struct {
	MvnArgs       string `yaml:"mvnArgs"`
	LibModules    string `yaml:"libModules"`
	PluginModules string `yaml:"pluginModules"`

	Tarball    TarballOpts `yaml:"tarball"`
	TargetName string      `yaml:"targetName"`
}

type TarballOpts struct {
	SkipCopyWebUi bool `yaml:"skipCopyWebUi"`

	AssemblyJars  []string          `yaml:"assemblyJars"`
	ClientJarName string            `yaml:"clientJarName"` // skip copying client jar if empty
	EmptyDirList  []string          `yaml:"emptyDirList"`
	FileList      []string          `yaml:"fileList"`
	Symlinks      map[string]string `yaml:"symlinks"`
}

type ProfilesYaml struct {
	DefaultName string              `yaml:"defaultName"`
	Profiles    map[string]*Profile `yaml:"profiles"`
}

func (t *TarballOpts) clientJarPath(alluxioVersion string) string {
	return filepath.Join("client", strings.ReplaceAll(t.ClientJarName, versionPlaceholder, alluxioVersion))
}

func (p *Profile) updateFromFlags(targetName, mvnArgs, libModules, pluginModules string) {
	// update profile
	if targetName != "" {
		p.TargetName = targetName
	}
	if mvnArgs != "" {
		p.MvnArgs = mvnArgs
	}
	if libModules != "" {
		p.LibModules = libModules
	}
	if pluginModules != "" {
		p.PluginModules = pluginModules
	}
}

func loadProfiles(profilesYml string) (*ProfilesYaml, error) {
	wd, err := os.Getwd()
	if err != nil {
		return nil, stacktrace.Propagate(err, "error getting current working directory")
	}
	profilesPath := filepath.Join(wd, profilesYml)
	log.Printf("Reading profiles from %v", profilesPath)
	content, err := ioutil.ReadFile(profilesPath)
	if err != nil {
		return nil, stacktrace.Propagate(err, "error reading file at %v", profilesPath)
	}
	var profsYml *ProfilesYaml
	if err := yaml.Unmarshal(content, &profsYml); err != nil {
		return nil, stacktrace.Propagate(err, "error unmarshalling profiles from:\n%v", string(content))
	}
	return profsYml, nil
}

func ProfilesF(args []string) error {
	ret := &buildOpts{}
	cmd := flag.NewFlagSet(Profiles, flag.ExitOnError)
	cmd.StringVar(&ret.profilesFile, "profilesFile", defaultProfilesFilePath, `path to profiles.yml file`)
	if err := cmd.Parse(args); err != nil {
		return stacktrace.Propagate(err, "error parsing flags")
	}
	profs, err := loadProfiles(ret.profilesFile)
	if err != nil {
		return stacktrace.Propagate(err, "error loading profiles from file at %v", ret.profilesFile)
	}
	content, err := yaml.Marshal(profs)
	if err != nil {
		return stacktrace.Propagate(err, "error serializing profiles")
	}
	fmt.Println()
	fmt.Println(string(content))
	return nil
}
