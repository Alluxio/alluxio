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
	"alluxio.org/log"
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
		Use: "ufsTest",
		Short: "Test the integration between Alluxio and the given UFS.\n" +
			"UFS tests validate the semantics Alluxio expects of the UFS.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	cmd.Flags().StringVar(&c.path, "path", "",
		"the full UFS path to run tests against.")
	cmd.PersistentFlags().StringSliceVar(&c.test, "test", nil,
		"Test name, this option can be passed multiple times to indicate multiply tests")
	err := cmd.MarkFlagRequired("path")
	if err != nil {
		log.Logger.Errorln("Required flag --path not specified.")
	}
	return cmd
}

func (c *TestUfsCommand) Run(args []string) error {
	var javaArgs []string
	if c.path != "" {
		javaArgs = append(javaArgs, "--path", c.path)
	}
	for _, singleTest := range c.test {
		javaArgs = append(javaArgs, "--test", singleTest)
	}
	javaArgs = append(javaArgs, args...)

	return c.Base().Run(javaArgs)
}
