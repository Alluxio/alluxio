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
	"fmt"
	"github.com/palantir/stacktrace"
	"strconv"

	"github.com/spf13/cobra"

	"alluxio.org/cli/env"
)

var TestJournalCrash = &TestJournalCrashCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "testJournalCrash",
		JavaClassName: "alluxio.cli.JournalCrashTest",
		ShellJavaOpts: fmt.Sprintf(env.JavaOptFormat, env.ConfAlluxioLoggerType, "Console"),
	},
}

type TestJournalCrashCommand struct {
	*env.BaseJavaCommand
	creates   string
	deletes   string
	maxAlive  string
	renames   string
	testDir   string
	totalTime string
}

func (c *TestJournalCrashCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *TestJournalCrashCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   "testJournalCrash",
		Short: "Test the Master Journal System in a crash scenario.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	cmd.Flags().StringVar(&c.creates, "creates", "2",
		"Number of Client Threads to request create operations.")
	cmd.Flags().StringVar(&c.deletes, "deletes", "2",
		"Number of Client Threads to request create/delete operations.")
	cmd.Flags().StringVar(&c.maxAlive, "maxAlive", "5",
		"The maximum time a master should ever be alive during the test, in seconds.")
	cmd.Flags().StringVar(&c.renames, "renames", "2",
		"Number of Client Threads to request create/rename operations.")
	cmd.Flags().StringVar(&c.testDir, "testDir", "/default_tests_files",
		"Test Directory on Alluxio.")
	cmd.Flags().StringVar(&c.totalTime, "totalTime", "20",
		"The total time to run this test, in seconds. This value should be greater than [maxAlive].")
	return cmd
}

func (c *TestJournalCrashCommand) Run(args []string) error {
	var javaArgs []string
	if c.creates != "" {
		_, err := strconv.Atoi(c.creates)
		if err != nil {
			return stacktrace.Propagate(err, "Flag --creates should be a number.")
		}
		javaArgs = append(javaArgs, "-creates", c.creates)
	}
	if c.deletes != "" {
		_, err := strconv.Atoi(c.deletes)
		if err != nil {
			return stacktrace.Propagate(err, "Flag --deletes should be a number.")
		}
		javaArgs = append(javaArgs, "-deletes", c.deletes)
	}
	if c.maxAlive != "" {
		_, err := strconv.Atoi(c.maxAlive)
		if err != nil {
			return stacktrace.Propagate(err, "Flag --maxAlive should be a number.")
		}
		javaArgs = append(javaArgs, "-maxAlive", c.maxAlive)
	}
	if c.renames != "" {
		_, err := strconv.Atoi(c.renames)
		if err != nil {
			return stacktrace.Propagate(err, "Flag --renames should be a number.")
		}
		javaArgs = append(javaArgs, "-renames", c.renames)
	}
	if c.testDir != "" {
		javaArgs = append(javaArgs, "-testDir", c.testDir)
	}
	if c.totalTime != "" {
		_, err := strconv.Atoi(c.totalTime)
		if err != nil {
			return stacktrace.Propagate(err, "Flag --totalTime should be a number.")
		}
		javaArgs = append(javaArgs, "-totalTime", c.totalTime)
	}
	javaArgs = append(javaArgs, args...)

	return c.Base().Run(javaArgs)
}
