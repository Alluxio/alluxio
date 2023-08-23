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

package cache

import (
	"fmt"

	"github.com/spf13/cobra"

	"alluxio.org/cli/env"
)

var Format = &FormatCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "format",
		JavaClassName: "alluxio.cli.Format",
		ShellJavaOpts: fmt.Sprintf(env.JavaOptFormat, env.ConfAlluxioLoggerType, "Console"),
	},
}

type FormatCommand struct {
	*env.BaseJavaCommand
}

func (c *FormatCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *FormatCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   Format.CommandName,
		Short: "Format Alluxio worker nodes.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	return cmd
}

func (c *FormatCommand) Run(args []string) error {
	javaArgs := []string{"worker"}
	return c.Base().Run(javaArgs)
}
