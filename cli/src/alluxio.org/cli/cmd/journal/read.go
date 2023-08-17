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

package journal

import (
	"fmt"
	"strconv"

	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"

	"alluxio.org/cli/env"
)

var Read = &ReadCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:        "read",
		JavaClassName:      "alluxio.master.journal.tool.JournalTool",
		UseServerClasspath: true,
		ShellJavaOpts:      fmt.Sprintf(env.JavaOptFormat, env.ConfAlluxioLoggerType, "Console"),
	},
}

type ReadCommand struct {
	*env.BaseJavaCommand

	end       int
	inputDir  string
	master    string
	outputDir string
	start     int
}

func (c *ReadCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *ReadCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   Read.CommandName,
		Short: "Read an Alluxio journal file to a human-readable version",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(nil)
		},
	})
	cmd.Flags().IntVar(&c.end, "end", -1, "end log sequence number (exclusive)")
	cmd.Flags().StringVar(&c.inputDir, "input-dir", "", "input directory on-disk to read the journal content from")
	cmd.Flags().StringVar(&c.master, "master", "FileSystemMaster", "name of the master class")
	cmd.Flags().StringVar(&c.outputDir, "output-dir", "", "output directory to write journal content to")
	cmd.Flags().IntVar(&c.start, "start", 0, "start log sequence number (inclusive)")
	return cmd
}

func (c *ReadCommand) Run(_ []string) error {
	var javaArgs []string
	if c.start < 0 {
		return stacktrace.NewError("start log sequence number must be non-negative but was %v", c.start)
	}
	if c.end < -1 {
		return stacktrace.NewError("end log sequence number must be non-negative but was %v", c.end)
	}
	if c.end != -1 {
		javaArgs = append(javaArgs, "-end", strconv.Itoa(c.end))
	}
	if c.inputDir != "" {
		javaArgs = append(javaArgs, "-inputDir", c.inputDir)
	}
	if c.master != "FileSystemMaster" {
		javaArgs = append(javaArgs, "-master", c.master)
	}
	if c.outputDir != "" {
		javaArgs = append(javaArgs, "-outputDir", c.outputDir)
	}
	if c.start > 0 {
		javaArgs = append(javaArgs, "-start", strconv.Itoa(c.start))
	}
	return c.Base().Run(javaArgs)
}
