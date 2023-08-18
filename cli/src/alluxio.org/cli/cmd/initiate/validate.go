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

package initiate

import (
	"fmt"

	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"

	"alluxio.org/cli/env"
)

var Validate = &ValidateCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "validate",
		ShellJavaOpts: fmt.Sprintf(env.JavaOptFormat, env.ConfAlluxioLoggerType, "Console"),
	},
}

type ValidateCommand struct {
	*env.BaseJavaCommand
	validateType string
}

func (c *ValidateCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *ValidateCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   c.CommandName,
		Short: "Validate Alluxio conf or environment and exit",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	cmd.Flags().StringVar(&c.validateType, "type", "",
		"Decide the type to validate. Valid inputs: [conf, env]")
	return cmd
}

func (c *ValidateCommand) Run(args []string) error {
	if c.validateType == "conf" {
		c.JavaClassName = "alluxio.cli.ValidateConf"
	} else if c.validateType == "env" {
		c.JavaClassName = "alluxio.cli.ValidateEnv"
	} else {
		return stacktrace.NewError("Invalid validate type. Valid inputs: [conf, env]")
	}
	return c.Base().Run(args)
}
