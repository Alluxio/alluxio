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
	"log"
	"strings"

	"github.com/palantir/stacktrace"

	"alluxio.org/common/repo"
)

const (
	Docker          = "docker"
	Modules         = "modules"
	Presets         = "presets"
	Profiles        = "profiles"
	Tarball         = "tarball"
	UfsVersionCheck = "ufsVersionCheck"
	Version         = "version"
)

var SubCmdNames = []string{
	Docker,
	Modules,
	Presets,
	Profiles,
	Tarball,
	UfsVersionCheck,
	Version,
}

const (
	defaultAssemblyFilePath = "src/alluxio.org/build/assembly.yml"
	defaultModulesFilePath  = "src/alluxio.org/build/modules.yml"
	defaultProfilesFilePath = "src/alluxio.org/build/profiles.yml"

	versionPlaceholder = `${VERSION}` // used as a placeholder string in tarball names and config files
)

type buildOpts struct {
	artifactOutput      string
	assemblyJarFile     string
	dryRun              bool
	modulesFile         string
	outputDir           string
	profilesFile        string
	skipRepoCopy        bool
	suppressMavenOutput bool

	assemblyJars  AssemblyJars
	mavenArgs     []string
	libModules    map[string]*LibModule
	pluginModules map[string]*PluginModule
	targetName    string
	tarball       TarballOpts
}

func parseTarballFlags(cmd *flag.FlagSet, args []string) (*buildOpts, error) {
	opts := &buildOpts{}

	// common flags
	cmd.StringVar(&opts.artifactOutput, "artifact", "", "If set, writes object representing the tarball to YAML output file")
	cmd.StringVar(&opts.assemblyJarFile, "assemblyFile", defaultAssemblyFilePath, "Path to assembly.yml file")
	cmd.StringVar(&opts.outputDir, "outputDir", repo.FindRepoRoot(), "Set output dir for generated tarball")
	cmd.BoolVar(&opts.dryRun, "dryRun", false, "If set, writes placeholder files instead of running maven commands to mock the final state of the build directory to be packaged as a tarball")
	cmd.StringVar(&opts.modulesFile, "modulesFile", defaultModulesFilePath, "Path to modules.yml file")
	cmd.StringVar(&opts.profilesFile, "profilesFile", defaultProfilesFilePath, "Path to profiles.yml file")
	cmd.BoolVar(&opts.skipRepoCopy, "skipRepoCopy", false, "Set true to build tarball from local repository instead of making a copy and running git clean")
	cmd.BoolVar(&opts.suppressMavenOutput, "suppressMavenOutput", false, "Set true to avoid printing maven command stdout to console")

	// profile specific flags
	// all default values are set to empty strings to be able to check if the user provided any input, which would override the profile's corresponding predefined value
	var flagProfile, flagTargetName, flagMvnArgs, flagLibModules, flagPluginModules string
	cmd.StringVar(&flagProfile, "profile", "", "Tarball profile to build; list available profiles with the profiles command")
	cmd.StringVar(&flagMvnArgs, "mvnArgs", "", `Comma-separated list of additional Maven arguments to build with, e.g. -mvnArgs "-Pspark,-Dhadoop.version=2.2.0"`)
	cmd.StringVar(&flagLibModules, "libModules", "",
		fmt.Sprintf("Either a lib modules bundle name or a comma-separated list of lib modules to compile into the tarball; list available lib modules and lib module bundles with the plugins command"))
	cmd.StringVar(&flagPluginModules, "pluginModules", "",
		fmt.Sprintf("Either a plugin modules bundle name or a comma-separated list of plugin modules to compile into the tarball; list available plugin modules and plugin module bundles with the plugins command"))
	cmd.StringVar(&flagTargetName, "target", "", "Name for the generated tarball; use '${VERSION}' as a placeholder for the version string")

	// parse and set finalized values into buildOpts
	if err := cmd.Parse(args); err != nil {
		return nil, stacktrace.Propagate(err, "error parsing flags")
	}
	// select profile to define defaults for profile specific flags, then overwrite value if corresponding flag is set
	profsYaml, err := loadProfiles(opts.profilesFile)
	if err != nil {
		return nil, stacktrace.Propagate(err, "error loading profiles")
	}
	// use profiles.yml default if flag wasn't set
	if flagProfile == "" {
		flagProfile = profsYaml.DefaultName
	}
	prof, ok := profsYaml.Profiles[flagProfile]
	if !ok {
		var names []string
		for n := range profsYaml.Profiles {
			names = append(names, n)
		}
		return nil, stacktrace.NewError("unknown profile value %v among possible profiles %v", flagProfile, names)
	}
	prof.updateFromFlags(flagTargetName, flagMvnArgs, flagLibModules, flagPluginModules)

	// process flag strings and store in opts
	if err := opts.processProfileValues(prof); err != nil {
		return nil, stacktrace.Propagate(err, "error processing profile values to update opts")
	}
	// parse assembly jars
	assemblyJars, err := loadAssemblyJars(opts.assemblyJarFile)
	if err != nil {
		return nil, stacktrace.Propagate(err, "error parsing assembly jars")
	}
	opts.assemblyJars = assemblyJars

	return opts, nil
}

func (opts *buildOpts) processProfileValues(prof *Profile) error {
	alluxioVersion, err := alluxioVersionFromPom()
	if err != nil {
		return stacktrace.Propagate(err, "error parsing version string")
	}
	opts.targetName = strings.ReplaceAll(prof.TargetName, versionPlaceholder, alluxioVersion)
	opts.mavenArgs = strings.Split(prof.MvnArgs, ",")
	opts.tarball = prof.Tarball

	// determine modules
	modules, err := loadModules(opts.modulesFile)
	if err != nil {
		return stacktrace.Propagate(err, "error loading modules")
	}

	opts.libModules = map[string]*LibModule{}
	// check if it is a bundle name
	if names, ok := modules.libBundles[prof.LibModules]; ok {
		log.Printf("Using lib modules defined for bundle %v", prof.LibModules)
		for _, n := range names {
			log.Printf("Including lib module %v", n)
			opts.libModules[n] = modules.LibModules[n]
		}
	} else {
		// assume comma-separated list of module names
		for _, n := range strings.Split(prof.LibModules, ",") {
			p, ok := modules.LibModules[n]
			if !ok {
				return stacktrace.NewError("no lib module named %v", n)
			}
			log.Printf("Including lib module %v", n)
			opts.libModules[n] = p
		}
	}
	opts.pluginModules = map[string]*PluginModule{}
	// check if it is a bundle name
	if names, ok := modules.pluginBundles[prof.PluginModules]; ok {
		log.Printf("Using plugin modules defined for bundle %v", prof.PluginModules)
		for _, n := range names {
			log.Printf("Including plugin module %v", n)
			opts.pluginModules[n] = modules.PluginModules[n]
		}
	} else {
		// assume comma-separated list of module names
		for _, n := range strings.Split(prof.PluginModules, ",") {
			p, ok := modules.PluginModules[n]
			if !ok {
				return stacktrace.NewError("no plugin module named %v", n)
			}
			log.Printf("Including plugin module %v", n)
			opts.pluginModules[n] = p
		}
	}

	return nil
}
