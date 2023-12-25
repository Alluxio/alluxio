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
		Long: `The rm command removes a file from Alluxio space and the under storage system.
The file will be unavailable immediately after this command returns, but the actual data may be deleted a while later.`,
		Example: `# Remove a file from Alluxio and the under storage system
$ ./bin/alluxio fs rm /tmp/unused-file

# Remove a file from Alluxio filesystem only
$ ./bin/alluxio fs rm --alluxio-only --skip-ufs-check /tmp/unused-file2
# Note it is recommended to use both --alluxio-only and --skip-ufs-check together in this situation`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	cmd.Flags().BoolVar(&c.alluxioOnly, "alluxio-only", false, "True to only remove data and metadata from Alluxio cache")
	cmd.Flags().BoolVarP(&c.isRecursive, "recursive", "R", false, "True to recursively remove files within the specified directory subtree")
	cmd.Flags().BoolVarP(&c.skipUfsCheck, "skip-ufs-check", "U", false, "True to skip checking if corresponding UFS contents are in sync")
	return cmd
}

func (c *RmCommand) Run(args []string) error {
	var javaArgs []string
	if c.alluxioOnly {
		javaArgs = append(javaArgs, "--alluxioOnly")
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
