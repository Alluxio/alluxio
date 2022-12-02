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
	addCommonFlags(fuseCmd, &FlagsOpts{
		TargetName: fmt.Sprintf("alluxio-fuse-%v.tar.gz", versionMarker),
		UfsModules: strings.Join(fuseUfsModuleNames, ","),
		LibJars:    libJarsCore,
	})
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
