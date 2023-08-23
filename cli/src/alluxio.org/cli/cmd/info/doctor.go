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

package info

import (
	"fmt"

	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"

	"alluxio.org/cli/cmd/names"
	"alluxio.org/cli/env"
)

var Doctor = &DoctorCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "doctor",
		JavaClassName: names.FileSystemAdminShellJavaClass,
		Parameters:    []string{"doctor"},
	},
}

type DoctorCommand struct {
	*env.BaseJavaCommand
}

func (c *DoctorCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *DoctorCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   fmt.Sprintf("%v [type]", Doctor.CommandName),
		Short: "Runs doctor configuration or storage command",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	return cmd
}

func (c *DoctorCommand) Run(args []string) error {
	var javaArgs []string
	if args[0] == "configuration" || args[0] == "storage" {
		javaArgs = append(javaArgs, args[0])
	} else if args[0] != "all" {
		return stacktrace.NewError("Invalid doctor type. Valid types: [all, configuration, storage]")
	}
	return c.Base().Run(javaArgs)
}
