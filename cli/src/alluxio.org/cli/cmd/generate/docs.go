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

var Docs = &DocsCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "docs",
		JavaClassName: "alluxio.cli.DocGenerator",
		ShellJavaOpts: fmt.Sprintf(env.JavaOptFormat, env.ConfAlluxioLoggerType, "Console"),
	},
}

type DocsCommand struct {
	*env.BaseJavaCommand
}

func (c *DocsCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *DocsCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   Docs.CommandName,
		Short: "Generate docs automatically.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	return cmd
}

func (c *DocsCommand) Run(args []string) error {
	return c.Base().Run(args)
}
