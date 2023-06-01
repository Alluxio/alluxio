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

	"github.com/palantir/stacktrace"

	"alluxio.org/build/cmd"
)

func main() {
	if err := run(os.Args); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) < 2 {
		return stacktrace.NewError("expected a subcommand argument. select one of the following: %v", cmd.SubCmdNames)
	}
	switch subCmd := args[1]; subCmd {
	case cmd.Docker:
		return cmd.DockerF(args[2:])
	case cmd.Modules:
		return cmd.PluginsF(args[2:])
	case cmd.Profiles:
		return cmd.ProfilesF(args[2:])
	case cmd.Tarball:
		return cmd.TarballF(args[2:])
	case cmd.UfsVersionCheck:
		return cmd.UfsVersionCheckF(args[2:])
	case cmd.Version:
		return cmd.VersionF()
	default:
		return stacktrace.NewError("unknown subcommand %v. select one of the following: %v", subCmd, cmd.SubCmdNames)
	}
}
