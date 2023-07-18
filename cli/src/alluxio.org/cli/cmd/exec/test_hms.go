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
	"alluxio.org/cli/env"
	"alluxio.org/log"
	"bytes"
	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"
)

var TestHms = &TestHmsCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "testHms",
		JavaClassName: "alluxio.cli.HmsTests",
	},
}

type TestHmsCommand struct {
	*env.BaseJavaCommand
	metastore     string
	database      string
	tables        string
	socketTimeout string
}

func (c *TestHmsCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *TestHmsCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   "testHms -m <arg> [-d <arg>] [-h] [-s <arg>] [-t <arg>]",
		Short: "Test the configuration, connectivity, and permission of an existing hive metastore.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	cmd.Flags().StringVarP(&c.metastore, "metastore", "m", "",
		"[required] Uri(s) to connect to hive metastore.")
	cmd.Flags().StringVarP(&c.database, "database", "d", "",
		"Database to run tests against.")
	cmd.Flags().StringVarP(&c.tables, "table", "t", "",
		"Tables to run tests against.\n"+
			"Multiple tables should be separated with comma.")
	cmd.Flags().StringVarP(&c.socketTimeout, "socketTimeout", "s", "",
		"Socket timeout of hive metastore client in minutes.\n"+
			"Consider increasing this if you have tables with a lot of metadata.")
	return cmd
}

func (c *TestHmsCommand) Run(args []string) error {
	var javaArgs []string
	if c.metastore != "" {
		javaArgs = append(javaArgs, "-m", c.metastore)
	}
	if c.database != "" {
		javaArgs = append(javaArgs, "-d", c.database)
	}
	if c.tables != "" {
		javaArgs = append(javaArgs, "-t", c.tables)
	}
	if c.socketTimeout != "" {
		javaArgs = append(javaArgs, "-st", c.socketTimeout)
	}
	javaArgs = append(javaArgs, args...)

	return c.Base().Run(javaArgs)
}

func (c *TestHmsCommand) FetchValue(key string) (string, error) {
	cmd := c.RunJavaClassCmd([]string{key})

	errBuf := &bytes.Buffer{}
	cmd.Stderr = errBuf

	log.Logger.Debugln(cmd.String())
	out, err := cmd.Output()
	if err != nil {
		return "", stacktrace.Propagate(err, "error getting conf for %v\nstderr: %v", key, errBuf.String())
	}
	return string(out), nil
}
