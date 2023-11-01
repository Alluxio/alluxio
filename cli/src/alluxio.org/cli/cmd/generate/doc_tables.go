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

package generate

import (
	"fmt"

	"github.com/spf13/cobra"

	"alluxio.org/cli/env"
)

var DocTables = &DocTablesCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "doc-tables",
		JavaClassName: "alluxio.cli.DocGenerator",
		ShellJavaOpts: []string{fmt.Sprintf(env.JavaOptFormat, env.ConfAlluxioLoggerType, "Console")},
	},
}

type DocTablesCommand struct {
	*env.BaseJavaCommand
}

func (c *DocTablesCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *DocTablesCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   DocTables.CommandName,
		Short: "Generate configuration and metric tables used in documentation",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	return cmd
}

func (c *DocTablesCommand) Run(args []string) error {
	return c.Base().Run(args)
}
