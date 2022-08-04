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

package main

import (
	"fmt"
	"os"

	"alluxio.org/build-distribution/cmd"
)

const (
	checkUfsVersions       = "checkUfsVersions"
	generateTarball        = "single"
	generateReleaseTarball = "release"
)

func main() {
	if err := runSubcmd(os.Args); err != nil {
		fmt.Println(err)
	}
}

func runSubcmd(args []string) error {
	subcmdNames := []string{checkUfsVersions, generateTarball, generateReleaseTarball}
	if len(args) < 2 {
		return fmt.Errorf("expected a subcommand in arguments. use one of %v", subcmdNames)
	}
	subcmd := args[1]
	switch subcmd {
	case generateTarball:
		return cmd.Single(args)
	case generateReleaseTarball:
		return cmd.Release(args)
	case checkUfsVersions:
		return cmd.CheckUfsVersions()
	default:
		return fmt.Errorf("unknown subcommand %q. use one of %v", args[1], subcmdNames)
	}
}
