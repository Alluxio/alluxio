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
)

func Release(args []string) error {
	releaseCmd := flag.NewFlagSet("release", flag.ExitOnError)
	// flags
	generateFlags(releaseCmd)
	releaseCmd.Parse(args[2:]) // error handling by flag.ExitOnError

	if err := handleUfsModulesAndLibJars(); err != nil {
		return err
	}
	if err := generateTarballs(); err != nil {
		return err
	}
	return nil
}

func generateTarballs() error {
	fmt.Printf("Generating tarball %v\n", fmt.Sprintf("alluxio-%v-bin.tar.gz", versionMarker))
	// Do not skip UI and Helm
	if err := generateTarball(false, false); err != nil {
		return err
	}
	return nil
}
