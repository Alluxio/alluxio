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

func Fuse(args []string) error {
	fuseCmd := flag.NewFlagSet("fuse", flag.ExitOnError)
	// flags
	addCommonFlags(fuseCmd)
	// TODO(lu) update description
	fuseCmd.StringVar(&targetFlag, "target", fmt.Sprintf("alluxio-fuse-%v.tar.gz", versionMarker),
		fmt.Sprintf("an optional target name for the generated tarball. The default is alluxio-fuse-%v.tar.gz. The string %q will be substituted with the built version. "+
			`Note that trailing ".tar.gz" will be stripped to determine the name for the Root directory of the generated tarball`, versionMarker, versionMarker))
	fuseCmd.StringVar(&ufsModulesFlag, "ufs-modules", strings.Join(defaultModules(fuseHadoopModules), ","),
		fmt.Sprintf("a comma-separated list of ufs modules to compile into the distribution tarball(s). Specify 'all' to build all ufs modules. Supported ufs modules: [%v]", strings.Join(validModules(ufsModules), ",")))
	fuseCmd.StringVar(&includedLibJarsFlag, "lib-jars", "core",
		"a comma-separated list of jars under lib/ to include in addition to all underfs-hdfs modules. All core UFS jars under lib/ will be included by default."+
			" e.g. underfs-s3a,underfs-gcs")

	fuseCmd.Parse(args[2:]) // error handling by flag.ExitOnError

	if err := handleUfsModulesAndLibJars(); err != nil {
		return err
	}
	if err := generateTarball(&GenerateTarballOpts{
		SkipUI:   false,
		SkipHelm: false,
		Fuse:     true,
	}); err != nil {
		return err
	}
	return nil
}
