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
	"github.com/spf13/cobra"

	"alluxio.org/cli/env"
)

var TestUfs = &TestUfsCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "ufsTest",
		JavaClassName: "alluxio.cli.UnderFileSystemContractTest",
	},
}

type TestUfsCommand struct {
	*env.BaseJavaCommand
	path string
	test []string
}

func (c *TestUfsCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *TestUfsCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   "ufsTest",
		Args:  cobra.NoArgs,
		Short: "Test the integration between Alluxio and the given UFS to validate UFS semantics",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	const path = "path"
	cmd.Flags().StringVar(&c.path, path, "", "the full UFS path to run tests against.")
	cmd.MarkFlagRequired(path)
	cmd.Flags().StringSliceVar(&c.test, "test", nil, "Test name, this option can be passed multiple times to indicate multipleZ tests")

	return cmd
}

func (c *TestUfsCommand) Run(args []string) error {
	javaArgs := []string{"--path", c.path}
	for _, singleTest := range c.test {
		javaArgs = append(javaArgs, "--test", singleTest)
	}

	return c.Base().Run(javaArgs)
}
