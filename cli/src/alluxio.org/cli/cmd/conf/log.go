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

package conf

import (
	"fmt"
	"github.com/spf13/cobra"

	"alluxio.org/cli/env"
)

var Log = &LogCommand{
	BaseCommand: &env.BaseCommand{
		CommandName:   "log",
		JavaClassName: "alluxio.cli.LogLevel",
	},
}

type LogCommand struct {
	*env.BaseCommand

	LogName string
	Level   string
	Target  string
}

func (c *LogCommand) Base() *env.BaseCommand {
	return c.BaseCommand
}

func (c *LogCommand) ToCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   fmt.Sprintf("%v --name <name> [--level <level>] [--target <target>]", Log.CommandName),
		Short: "Get or set the log level for the specified logger",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	}
	cmd.Flags().StringVar(&c.LogName, "name", "", "Logger name (ex. alluxio.master.file.DefaultFileSystemMaster)")
	if err := cmd.MarkFlagRequired("name"); err != nil {
		panic(err)
	}
	cmd.Flags().StringVar(&c.Level, "level", "", "If specified, sets the specified logger at the given level")
	cmd.Flags().StringVar(&c.Target, "target", "", "A list of comma delimited targets among <master|workers|job_master|job_workers|host:webPort[:role]>. Defaults to master,workers,job_master,job_workers")
	return cmd
}

func (c *LogCommand) Run(_ []string) error {
	javaArgs := []string{"--logName", c.LogName}
	if c.Level != "" {
		javaArgs = append(javaArgs, "--level", c.Level)
	}
	if c.Target != "" {
		javaArgs = append(javaArgs, "--target", c.Target)
	}

	return c.Base().Run(javaArgs)
}
