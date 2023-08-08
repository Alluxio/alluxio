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

	"alluxio.org/log"
	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"

	"alluxio.org/cli/env"
)

var Submit = &SubmitCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "submit",
		JavaClassName: "alluxio.cli.fs.FileSystemShell",
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
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   Cancel.CommandName,
		Short: "Moves, or copies a file or directory in parallel at file level.",
		Args:  cobra.MinimumNArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	cmd.Flags().StringVar(&c.operationType, "type", "", "Determine type, options: [cp, mv].")
	cmd.Flags().StringVar(&c.src, "src", "", "The path to move/copy from.")
	cmd.Flags().StringVar(&c.dst, "dst", "", "The path to move/copy to.")
	cmd.Flags().IntVar(&c.activeJobs, "active-jobs", 3000,
		"Number of active jobs that can run at the same time. Later jobs must wait."+
			"The default upper limit is 3000.")
	cmd.Flags().IntVar(&c.batchSize, "batch-size", 1,
		"Number of files per request. The default batch size is 1.")
	cmd.MarkFlagRequired("type")
	cmd.MarkFlagRequired("src")
	cmd.MarkFlagRequired("dst")
	return cmd
}

func (c *SubmitCommand) Run(args []string) error {
	var javaArgs []string
	switch c.operationType {
	case "cp":
		javaArgs = append(javaArgs, "distributedCp")
		javaArgs = append(javaArgs, c.src)
		javaArgs = append(javaArgs, c.dst)
		if c.activeJobs <= 0 {
			stacktrace.NewError("Flag --active-jobs should be a positive integer")
		}
		javaArgs = append(javaArgs, "--active-jobs", strconv.Itoa(c.activeJobs))
		if c.batchSize <= 0 {
			stacktrace.NewError("Flag --batch-size should be a positive integer")
		}
		javaArgs = append(javaArgs, "--batch-size", strconv.Itoa(c.batchSize))

	case "mv":
		javaArgs = append(javaArgs, "distributedMv")
		javaArgs = append(javaArgs, c.src)
		javaArgs = append(javaArgs, c.dst)
		if c.activeJobs != 3000 {
			log.Logger.Warningf("Flag --active-jobs is set, but won't be used in this operation type")
		}
		if c.batchSize != 1 {
			log.Logger.Warningf("Flag --batch-size is set, but won't be used in this operation type")
		}

	default:
		stacktrace.NewError("Invalid operation type. Must be one of [cp mv].")
	}
	return c.Base().Run(args)
}
