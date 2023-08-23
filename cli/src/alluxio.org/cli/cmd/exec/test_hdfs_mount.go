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

var TestHdfsMount = &TestHdfsMountCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "hdfsMountTest",
		JavaClassName: "alluxio.cli.ValidateHdfsMount",
		ShellJavaOpts: fmt.Sprintf(env.JavaOptFormat, env.ConfAlluxioLoggerType, "Console"),
	},
}

type TestHdfsMountCommand struct {
	*env.BaseJavaCommand
	path     string
	readonly bool
	shared   bool
	option   string
}

func (c *TestHdfsMountCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *TestHdfsMountCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   "hdfsMountTest",
		Args:  cobra.NoArgs,
		Short: "Tests runs a set of validations against the given hdfs path.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	const path = "path"
	cmd.Flags().StringVar(&c.path, path, "", "specifies the HDFS path you want to validate.")
	cmd.MarkFlagRequired(path)
	cmd.Flags().BoolVar(&c.readonly, "readonly", false, "mount point is readonly in Alluxio.")
	cmd.Flags().BoolVar(&c.shared, "shared", false, "mount point is shared.")
	cmd.Flags().StringVar(&c.option, "option", "", "options associated with this mount point.")
	return cmd
}

func (c *TestHdfsMountCommand) Run(args []string) error {
	var javaArgs []string
	javaArgs = append(javaArgs, c.path)
	if c.readonly {
		javaArgs = append(javaArgs, "--readonly")
	}
	if c.shared {
		javaArgs = append(javaArgs, "--shared")
	}
	if c.option != "" {
		javaArgs = append(javaArgs, "--option", c.option)
	}

	return c.Base().Run(javaArgs)
}
