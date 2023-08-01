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

package fs

import (
	"github.com/spf13/cobra"

	"alluxio.org/cli/env"
)

func Du(className string) env.Command {
	return &DuCommand{
		BaseJavaCommand: &env.BaseJavaCommand{
			CommandName:   "du",
			JavaClassName: className,
			Parameters:    []string{"du"},
		},
	}
}

type DuCommand struct {
	*env.BaseJavaCommand

	groupByWorker   bool
	isHumanReadable bool
	showMemoryInfo  bool
	summarize       bool
}

func (c *DuCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *DuCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   "du [path]",
		Short: "Print specified file's content",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	// special case to overwrite the -h shorthand for --help flag for --human-readable
	cmd.PersistentFlags().BoolP("help", "", false, "help for this command")

	cmd.Flags().BoolVarP(&c.groupByWorker, "group-worker", "g", false, "Display distribution of data store in Alluxio grouped by worker")
	cmd.Flags().BoolVarP(&c.isHumanReadable, "human-readable", "h", false, "Print sizes in human readable format")
	cmd.Flags().BoolVarP(&c.showMemoryInfo, "memory", "m", false, "Show in memory size and percentage")
	cmd.Flags().BoolVarP(&c.summarize, "summary", "s", false, "Summarize listed files by aggregating their sizes")
	return cmd
}

func (c *DuCommand) Run(args []string) error {
	var javaArgs []string
	if c.groupByWorker {
		javaArgs = append(javaArgs, "-g")
	}
	if c.isHumanReadable {
		javaArgs = append(javaArgs, "-h")
	}
	if c.showMemoryInfo {
		javaArgs = append(javaArgs, "-m")
	}
	if c.summarize {
		javaArgs = append(javaArgs, "-s")
	}
	javaArgs = append(javaArgs, args...)
	return c.Base().Run(javaArgs)
}
