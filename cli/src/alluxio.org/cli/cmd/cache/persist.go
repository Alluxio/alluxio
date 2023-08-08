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
	"strconv"

	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"

	"alluxio.org/cli/cmd"
	"alluxio.org/cli/env"
)

var Persist = &PersistCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "persist",
		JavaClassName: cmd.FileSystemShellJavaClass,
	},
}

type PersistCommand struct {
	*env.BaseJavaCommand
	parallelism int
	timeout     int
	wait        int
	path        []string
}

func (c *PersistCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *PersistCommand) ToCommand() *cobra.Command {
	command := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   Persist.CommandName,
		Short: "Persists files or directories currently stored only in Alluxio to the UnderFileSystem.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	command.Flags().IntVarP(&c.parallelism, "parallelism", "p", 4,
		"Number of concurrent persist operations")
	command.Flags().IntVarP(&c.timeout, "timeout", "t", 1200000,
		"Time in milliseconds for a single file persist to time out")
	command.Flags().IntVarP(&c.wait, "wait", "w", 0,
		"The time to wait before persisting")
	command.Flags().StringSliceVar(&c.path, "path", nil, "The path(s) to persist")
	return command
}

func (c *PersistCommand) Run(args []string) error {
	if c.parallelism <= 0 {
		return stacktrace.NewError("Flag -p should be a positive integer")
	}
	if c.timeout < 0 {
		return stacktrace.NewError("Flag -t should be a non-negative integer")
	}
	if c.wait < 0 {
		return stacktrace.NewError("Flag -w should be a non-negative integer")
	}
	if len(c.path) == 0 {
		return stacktrace.NewError("Path not specified, at least one path needed")
	}
	javaArgs := []string{
		"persist",
		"-p", strconv.Itoa(c.parallelism),
		"-t", strconv.Itoa(c.timeout),
		"-w", strconv.Itoa(c.wait),
	}
	javaArgs = append(javaArgs, c.path...)
	return c.Base().Run(javaArgs)
}
