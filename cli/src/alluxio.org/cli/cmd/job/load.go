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

package job

import (
	"github.com/spf13/cobra"

	"alluxio.org/cli/cmd/names"
	"alluxio.org/cli/env"
)

var Load = &LoadCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "load",
		JavaClassName: names.FileSystemShellJavaClass,
	},
}

type LoadCommand struct {
	*env.BaseJavaCommand
	path string
}

func (c *LoadCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *LoadCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   Load.CommandName,
		Short: "Submit load job to Alluxio master, update job options if already exists",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	const path = "path"
	cmd.Flags().StringVar(&c.path, path, "", "Determine the path of the load job to submit")
	cmd.MarkFlagRequired(path)
	return cmd
}

func (c *LoadCommand) Run(args []string) error {
	javaArgs := []string{"load", c.path, "--submit"}
	return c.Base().Run(javaArgs)
}
