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

package metadata

import (
	"alluxio.org/cli/cmd/names"
	"github.com/spf13/cobra"

	"alluxio.org/cli/env"
)

var Load = &LoadCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "load",
		JavaClassName: names.FileSystemShellJavaClass,
		Parameters:    []string{"loadMetadata"},
	},
}

type LoadCommand struct {
	*env.BaseJavaCommand

	path string

	force     bool
	recursive bool
}

func (c *LoadCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *LoadCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   c.CommandName,
		Short: "Loads metadata for the given Alluxio path from the under file system",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	const path = "path"
	cmd.Flags().StringVar(&c.path, path, "", "Path to load metadata from")
	cmd.MarkFlagRequired(path)
	cmd.Flags().BoolVarP(&c.force, "force", "F", false, "Update the metadata of existing subfiles forcibly")
	cmd.Flags().BoolVarP(&c.recursive, "recursive", "R", false, "Load metadata from subdirectories recursively")
	return cmd
}

func (c *LoadCommand) Run(args []string) error {
	var javaArgs []string
	if c.force {
		javaArgs = append(javaArgs, "-F")
	}
	if c.recursive {
		javaArgs = append(javaArgs, "-R")
	}
	javaArgs = append(javaArgs, c.path)
	return c.Base().Run(javaArgs)
}
