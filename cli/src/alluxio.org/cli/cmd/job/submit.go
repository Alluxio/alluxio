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

package job

import (
	"strconv"

	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"

	"alluxio.org/cli/cmd"
	"alluxio.org/cli/env"
	"alluxio.org/log"
)

var Submit = &SubmitCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "submit",
		JavaClassName: cmd.FileSystemShellJavaClass,
	},
}

type SubmitCommand struct {
	*env.BaseJavaCommand
	operationType string
	src           string
	dst           string
	activeJobs    int
	batchSize     int
}

func (c *SubmitCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *SubmitCommand) ToCommand() *cobra.Command {
	command := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   Submit.CommandName,
		Short: "Moves or copies a file or directory in parallel at file level.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	command.Flags().StringVar(&c.operationType, "type", "", "Determine type, options: [cp, mv]")
	command.Flags().StringVar(&c.src, "src", "", "The path to move/copy from")
	command.Flags().StringVar(&c.dst, "dst", "", "The path to move/copy to")
	command.Flags().IntVar(&c.activeJobs, "active-jobs", 3000,
		"Number of active jobs that can run at the same time, later jobs must wait")
	command.Flags().IntVar(&c.batchSize, "batch-size", 1, "Number of files per request")
	command.MarkFlagRequired("type")
	command.MarkFlagRequired("src")
	command.MarkFlagRequired("dst")
	return command
}

func (c *SubmitCommand) Run(args []string) error {
	var javaArgs []string
	switch c.operationType {
	case "cp":
		javaArgs = append(javaArgs, "distributedCp")
		if c.activeJobs <= 0 {
			return stacktrace.NewError("Flag --active-jobs should be a positive integer")
		}
		javaArgs = append(javaArgs, "--active-jobs", strconv.Itoa(c.activeJobs))
		if c.batchSize <= 0 {
			return stacktrace.NewError("Flag --batch-size should be a positive integer")
		}
		javaArgs = append(javaArgs, "--batch-size", strconv.Itoa(c.batchSize))

	case "mv":
		javaArgs = append(javaArgs, "distributedMv")
		if c.activeJobs != 3000 {
			log.Logger.Warningf("Flag --active-jobs is set, but won't be used in this operation type")
		}
		if c.batchSize != 1 {
			log.Logger.Warningf("Flag --batch-size is set, but won't be used in this operation type")
		}

	default:
		return stacktrace.NewError("Invalid operation type. Must be one of [cp mv].")
	}

	javaArgs = append(javaArgs, c.src)
	javaArgs = append(javaArgs, c.dst)
	return c.Base().Run(javaArgs)
}
