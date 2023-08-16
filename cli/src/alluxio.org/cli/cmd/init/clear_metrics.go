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

package init

import (
	"strconv"

	"alluxio.org/cli/cmd/names"
	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"

	"alluxio.org/cli/env"
)

var ClearMetrics = &ClearMetricsCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "clearMetrics",
		JavaClassName: names.FileSystemAdminShellJavaClass,
		Parameters:    []string{"metrics", "clear"},
	},
}

type ClearMetricsCommand struct {
	*env.BaseJavaCommand
	master      bool
	worker      string
	parallelism int
}

func (c *ClearMetricsCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *ClearMetricsCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   ClearMetrics.CommandName,
		Short: "",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	cmd.Flags().BoolVar(&c.master, "master", false,
		"Clear the metrics of Alluxio leading master")
	cmd.Flags().StringVar(&c.worker, "worker", "",
		"Clear metrics of specified workers, pass in the worker hostnames separated by comma")
	cmd.Flags().IntVar(&c.parallelism, "parallelism", 8,
		"Number of concurrent worker metrics clear operations")
	return cmd
}

func (c *ClearMetricsCommand) Run(args []string) error {
	var javaArgs []string
	if c.parallelism <= 0 {
		return stacktrace.NewError("Flag --parallelism should be a positive number.")
	}
	if c.master {
		javaArgs = append(javaArgs, "--master")
	}
	if c.worker != "" {
		javaArgs = append(javaArgs, "--worker", c.worker)
	}
	javaArgs = append(javaArgs, "--parallelism", strconv.Itoa(c.parallelism))
	return c.Base().Run(args)
}
