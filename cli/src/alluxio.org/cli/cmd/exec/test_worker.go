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

package exec

import (
	"fmt"

	"github.com/spf13/cobra"

	"alluxio.org/cli/env"
)

var TestRunCluster = &TestClusterCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "checkCluster",
		JavaClassName: "alluxio.cli.CheckCluster",
		ShellJavaOpts: []string{fmt.Sprintf(env.JavaOptFormat, env.ConfAlluxioLoggerType, "Console")},
	},
}

type TestClusterCommand struct {
	*env.BaseJavaCommand
}

func (c *TestClusterCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *TestClusterCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   "checkCluster",
		Args:  cobra.NoArgs,
		Short: "Test whether the workers have already run successfully.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	return cmd
}

func (c *TestClusterCommand) Run(args []string) error {
	var javaArgs []string
	return c.Base().Run(javaArgs)
}
