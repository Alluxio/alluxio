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

func Checksum(className string) env.Command {
	return &ChecksumCommand{
		BaseJavaCommand: &env.BaseJavaCommand{
			CommandName:   "checksum",
			JavaClassName: className,
			Parameters:    []string{"checksum"},
		},
	}
}

type ChecksumCommand struct {
	*env.BaseJavaCommand
}

func (c *ChecksumCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *ChecksumCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   "checksum [path]",
		Short: "Calculates the md5 checksum of a specified file",
		Long: `The checksum command outputs the md5 value of a file in Alluxio.
This can be used to verify the contents of a file stored in Alluxio.`,
		Example: `# Compare the checksum values
# value from Alluxio filesystem
$ ./bin/alluxio fs checksum /LICENSE
md5sum: bf0513403ff54711966f39b058e059a3
# value from local filesystem
md5 LICENSE
MD5 (LICENSE) = bf0513403ff54711966f39b058e059a3`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	return cmd
}

func (c *ChecksumCommand) Run(args []string) error {
	return c.Base().Run(args)
}
