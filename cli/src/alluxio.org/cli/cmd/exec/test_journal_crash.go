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
		CommandName:   "journalCrashTest",
		JavaClassName: "alluxio.cli.JournalCrashTest",
		ShellJavaOpts: fmt.Sprintf(env.JavaOptFormat, env.ConfAlluxioLoggerType, "Console"),
	},
}

type TestJournalCrashCommand struct {
	*env.BaseJavaCommand
	creates   int
	deletes   int
	maxAlive  int
	renames   int
	testDir   string
	totalTime int
}

func (c *TestJournalCrashCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *TestJournalCrashCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   "journalCrashTest",
		Short: "Test the Master Journal System in a crash scenario.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	cmd.Flags().IntVar(&c.creates, "creates", 2,
		"Number of Client Threads to request create operations.")
	cmd.Flags().IntVar(&c.deletes, "deletes", 2,
		"Number of Client Threads to request create/delete operations.")
	cmd.Flags().IntVar(&c.maxAlive, "maxAlive", 5,
		"The maximum time a master should ever be alive during the test, in seconds.")
	cmd.Flags().IntVar(&c.renames, "renames", 2,
		"Number of Client Threads to request create/rename operations.")
	cmd.Flags().StringVar(&c.testDir, "testDir", "/default_tests_files",
		"Test Directory on Alluxio.")
	cmd.Flags().IntVar(&c.totalTime, "totalTime", 20,
		"The total time to run this test, in seconds. This value should be greater than flag --maxAlive.")
	return cmd
}

func (c *TestJournalCrashCommand) Run(args []string) error {
	if c.creates <= 0 {
		stacktrace.Propagate(nil, "Flag --creates should be a positive integer")
	}
	if c.deletes <= 0 {
		stacktrace.Propagate(nil, "Flag --deletes should be a positive integer")
	}
	if c.maxAlive <= 0 {
		stacktrace.Propagate(nil, "Flag --maxAlive should be a positive integer")
	}
	if c.renames <= 0 {
		stacktrace.Propagate(nil, "Flag --renames should be a positive integer")
	}
	if c.totalTime <= 0 {
		stacktrace.Propagate(nil, "Flag --totalTime should be a positive integer")
	}
	if c.totalTime < c.maxAlive {
		stacktrace.Propagate(nil, "Flag --totalTime should be greater than flag --maxAlive.")
	}
	javaArgs := []string{
		"-creates", strconv.Itoa(c.creates),
		"-deletes", strconv.Itoa(c.deletes),
		"-maxAlive", strconv.Itoa(c.maxAlive),
		"-renames", strconv.Itoa(c.renames),
		"-totalTime", strconv.Itoa(c.totalTime),
	}
	if c.testDir != "" {
		javaArgs = append(javaArgs, "-testDir", c.testDir)
	}

	return c.Base().Run(javaArgs)
}
