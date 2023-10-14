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

package cache

import (
	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"

	"alluxio.org/cli/cmd/names"
	"alluxio.org/cli/env"
)

var Free = &FreeCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "free",
		JavaClassName: names.FileSystemShellJavaClass,
	},
}

type FreeCommand struct {
	*env.BaseJavaCommand
	worker string
	path   string
}

func (c *FreeCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *FreeCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   Free.CommandName,
		Short: "Synchronously free cached files along a path or held by a specific worker",
		Example: `# Free a file by its path
$ ./bin/alluxio cache free --path /path/to/file

# Free files on a worker
$ ./bin/alluxio cache free --worker <workerHostName>`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	cmd.Flags().StringVar(&c.worker, "worker", "", "The worker to free")
	cmd.Flags().StringVar(&c.path, "path", "", "The file or directory to free")
	cmd.MarkFlagsMutuallyExclusive("worker", "path")
	return cmd
}

func (c *FreeCommand) Run(args []string) error {
	var javaArgs []string
	if c.worker == "" {
		if c.path != "" {
			// free directory
			javaArgs = append(javaArgs, "free", c.path)
		} else {
			return stacktrace.NewError("neither worker nor path to free specified")
		}
	} else {
		if c.path == "" {
			// free workers
			javaArgs = append(javaArgs, "freeWorker", c.worker)
		} else {
			return stacktrace.NewError("both worker and path to free specified")
		}
	}
	return c.Base().Run(javaArgs)
}
