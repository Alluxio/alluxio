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
	"strings"
)

const (
	// The version of the hadoop client that the Alluxio client will be built for
	defaultHadoopClient = "hadoop-3.3"

	// enums for identifying gropus of lib jars
	libJarsAll  = "all"
	libJarsFuse = "fuse"
)

var (
	debugFlag              bool
	hadoopDistributionFlag string
	includedLibJarsFlag    string
	mvnArgsFlag            string
	targetFlag             string
	ufsModulesFlag         string
)

type FlagsOpts struct {
	TargetName string
	UfsModules string
	LibJars    string
}

// flags used by single/release/fuse to generate tarball
func addCommonFlags(cmd *flag.FlagSet, opts *FlagsOpts) {
	cmd.BoolVar(&debugFlag, "debug", false, "whether to run this tool in debug mode to generate additional console output")
	cmd.StringVar(&hadoopDistributionFlag, "hadoop-distribution", defaultHadoopClient, "the hadoop distribution to build this Alluxio distribution tarball")
	cmd.StringVar(&mvnArgsFlag, "mvn-args", "", `a comma-separated list of additional Maven arguments to build with, e.g. -mvn-args "-Pspark,-Dhadoop.version=2.2.0"`)
	cmd.StringVar(&targetFlag, "target", opts.TargetName,
		fmt.Sprintf("an optional target name for the generated tarball. The default is alluxio-%v.tar.gz for alluxio tarballs and alluxio-fuse-%v.tar.gz for alluxio fuse tarballs."+
			"The string %q will be substituted with the built version. "+
			`Note that trailing ".tar.gz" will be stripped to determine the name for the Root directory of the generated tarball`, versionMarker, versionMarker, versionMarker))
	cmd.StringVar(&ufsModulesFlag, "ufs-modules", opts.UfsModules,
		fmt.Sprintf("a comma-separated list of ufs modules to compile into the distribution tarball(s). Specify 'all' to build all ufs modules. Supported ufs modules: [%v]", strings.Join(validModules(ufsModules), ",")))
	cmd.StringVar(&includedLibJarsFlag, "lib-jars", opts.LibJars,
		"a comma-separated list of jars under lib/ to include in addition to all underfs-hdfs modules. "+
			"e.g. underfs-s3a,underfs-gcs. "+
			"All jars under lib/ will be included by default using value 'all'. "+
			"Core jars (using by Alluxio Fuse tarball) will be included using value 'core'.")
}

// parses 'all' to include all known ufs modules/lib jars or validates given ufs modules/lib jars are valid
func handleUfsModulesAndLibJars() error {
	if strings.ToLower(ufsModulesFlag) == "all" {
		ufsModulesFlag = strings.Join(validModules(ufsModules), ",")
	} else {
		for _, module := range strings.Split(ufsModulesFlag, ",") {
			if _, ok := ufsModules[module]; !ok {
				return fmt.Errorf("ufs module %v not recognized", module)
			}
		}
	}
	switch strings.ToLower(includedLibJarsFlag) {
	case libJarsAll:
		var allLibJars []string
		for jar := range libJars {
			allLibJars = append(allLibJars, jar)
		}
		includedLibJarsFlag = strings.Join(allLibJars, ",")
	case libJarsFuse:
		var fuseJars []string
		for jar := range fuseLibJars {
			fuseJars = append(fuseJars, jar)
		}
		includedLibJarsFlag = strings.Join(fuseJars, ",")
	default:
		for _, jar := range strings.Split(includedLibJarsFlag, ",") {
			if _, ok := libJars[jar]; !ok {
				return fmt.Errorf("lib jar %v not recognized", jar)
			}
		}
	}
	return nil
}
