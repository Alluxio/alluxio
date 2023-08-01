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

package fs

import (
	"github.com/spf13/cobra"

	"alluxio.org/cli/env"
)

func Rm(className string) env.Command {
	return &RmCommand{
		BaseJavaCommand: &env.BaseJavaCommand{
			CommandName:   "rm",
			JavaClassName: className,
			Parameters:    []string{"rm"},
		},
	}
}

type RmCommand struct {
	*env.BaseJavaCommand

	alluxioOnly  bool
	deleteMount  bool
	isRecursive  bool
	skipUfsCheck bool
}

func (c *RmCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *RmCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   "rm [path]",
		Short: "Remove the specified file",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	cmd.Flags().BoolVar(&c.alluxioOnly, "alluxio-only", false, "True to only remove data and metadata from Alluxio cache")
	cmd.Flags().BoolVarP(&c.deleteMount, "delete-mount", "m", false, "True to remove mount points within the specified directory subtree, which would otherwise cause failures")
	cmd.Flags().BoolVarP(&c.isRecursive, "recursive", "R", false, "True to recursively remove files within the specified directory subtree")
	cmd.Flags().BoolVarP(&c.skipUfsCheck, "skip-ufs-check", "U", false, "True to skip checking if corresponding UFS contents are in sync")
	return cmd
}

func (c *RmCommand) Run(args []string) error {
	var javaArgs []string
	if c.alluxioOnly {
		javaArgs = append(javaArgs, "--alluxioOnly")
	}
	if c.deleteMount {
		javaArgs = append(javaArgs, "--deleteMountPoint")
	}
	if c.isRecursive {
		javaArgs = append(javaArgs, "-R")
	}
	if c.skipUfsCheck {
		javaArgs = append(javaArgs, "-U")
	}
	javaArgs = append(javaArgs, args...)
	return c.Base().Run(javaArgs)
}
