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
	"strconv"

	"github.com/spf13/cobra"

	"alluxio.org/cli/env"
)

var TestHms = &TestHmsCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "hiveMetastoreTest",
		JavaClassName: "alluxio.cli.HmsTests",
	},
}

type TestHmsCommand struct {
	*env.BaseJavaCommand
	metastore     string
	database      string
	tables        string
	socketTimeout int
}

func (c *TestHmsCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *TestHmsCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   "hiveMetastoreTest",
		Args:  cobra.NoArgs,
		Short: "Test the configuration, connectivity, and permission of an existing hive metastore.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	cmd.Flags().StringVarP(&c.metastore, "metastore", "m", "",
		"Uri(s) to connect to hive metastore.")
	cmd.Flags().StringVarP(&c.database, "database", "d", "default",
		"Database to run tests against.")
	cmd.Flags().StringVarP(&c.tables, "table", "t", "",
		"Tables to run tests against.\n"+
			"Multiple tables should be separated with comma.")
	cmd.Flags().IntVarP(&c.socketTimeout, "socketTimeout", "s", -1,
		"Socket timeout of hive metastore client in minutes.\n"+
			"Consider increasing this if you have tables with a lot of metadata.")
	cmd.MarkFlagRequired("metastore")
	return cmd
}

func (c *TestHmsCommand) Run(args []string) error {
	javaArgs := []string{"-m", c.metastore}
	if c.database != "" {
		javaArgs = append(javaArgs, "-d", c.database)
	}
	if c.tables != "" {
		javaArgs = append(javaArgs, "-t", c.tables)
	}
	javaArgs = append(javaArgs, "-st", strconv.Itoa(c.socketTimeout))

	return c.Base().Run(javaArgs)
}
