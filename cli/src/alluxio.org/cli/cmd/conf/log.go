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
	"strings"

	"github.com/spf13/cobra"

	"alluxio.org/cli/env"
)

var Log = &LogCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "log",
		JavaClassName: "alluxio.cli.LogLevel",
	},
}

type LogCommand struct {
	*env.BaseJavaCommand

	LogName string
	Level   string
	Targets []string
}

func (c *LogCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *LogCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   Log.CommandName,
		Short: "Get or set the log level for the specified logger",
		Long: `The log command returns the current value of or updates the log level of a particular class on specific instances.
Users are able to change Alluxio server-side log levels at runtime.

The --target flag specifies which processes to apply the log level change to.
The target could be of the form <master|workers|job_master|job_workers|host:webPort[:role]> and multiple targets can be listed as comma-separated entries.
The role can be one of master,worker,job_master,job_worker.
Using the role option is useful when an Alluxio process is configured to use a non-standard web port (e.g. if an Alluxio master does not use 19999 as its web port).
The default target value is the primary master, primary job master, all workers and job workers.

> Note: This command requires the Alluxio cluster to be running.`,
		Example: `# Set DEBUG level for DefaultFileSystemMaster class on master processes
$ ./bin/alluxio conf log --logName alluxio.master.file.DefaultFileSystemMaster --target=master --level=DEBUG

# Set WARN level for PagedDoraWorker class on the worker process on host myHostName
$ ./bin/alluxio conf log --logName alluxio.worker.dora.PagedDoraWorker.java --target=myHostName:worker --level=WARN
`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	const name = "name"
	cmd.Flags().StringVar(&c.LogName, name, "", "Logger name (ex. alluxio.master.file.DefaultFileSystemMaster)")
	cmd.MarkFlagRequired(name)
	cmd.Flags().StringVar(&c.Level, "level", "", "If specified, sets the specified logger at the given level")
	cmd.Flags().StringSliceVar(&c.Targets, "target", nil, "A target name among <master|workers|job_master|job_workers|host:webPort[:role]>. Defaults to master,workers,job_master,job_workers")
	return cmd
}

func (c *LogCommand) Run(_ []string) error {
	javaArgs := []string{"--logName", c.LogName}
	if c.Level != "" {
		javaArgs = append(javaArgs, "--level", c.Level)
	}
	if len(c.Targets) > 0 {
		javaArgs = append(javaArgs, "--target", strings.Join(c.Targets, ","))
	}

	return c.Base().Run(javaArgs)
}
