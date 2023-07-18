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
	"alluxio.org/cli/env"
	"alluxio.org/log"
	"bytes"
	"fmt"
	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"
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
		Use: "testJournalCrash [--creates <arg>] [--deletes <arg>] [--maxAlive <arg>]" +
			"[--renames <arg>] [--testDir <arg>] [--totalTime <arg>]",
		Short: "Test the Master Journal System in a crash scenario.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	cmd.Flags().StringVar(&c.creates, "creates", "",
		"Number of Client Threads to request create operations.")
	cmd.Flags().StringVar(&c.deletes, "deletes", "",
		"Number of Client Threads to request create/delete operations.")
	cmd.Flags().StringVar(&c.maxAlive, "maxAlive", "",
		"The maximum time a master should ever be alive during the test, in seconds.")
	cmd.Flags().StringVar(&c.renames, "renames", "",
		"Number of Client Threads to request create/rename operations.")
	cmd.Flags().StringVar(&c.testDir, "testDir", "",
		"Test Directory on Alluxio.")
	cmd.Flags().StringVar(&c.totalTime, "totalTime", "",
		"The total time to run this test, in seconds. This value should be greater than [maxAlive].")
	return cmd
}

func (c *TestJournalCrashCommand) Run(args []string) error {
	var javaArgs []string
	if c.creates != "" {
		javaArgs = append(javaArgs, "-creates", c.creates)
	}
	if c.deletes != "" {
		javaArgs = append(javaArgs, "-deletes", c.deletes)
	}
	if c.maxAlive != "" {
		javaArgs = append(javaArgs, "-maxAlive", c.maxAlive)
	}
	if c.renames != "" {
		javaArgs = append(javaArgs, "-renames", c.renames)
	}
	if c.testDir != "" {
		javaArgs = append(javaArgs, "-testDir", c.testDir)
	}
	if c.totalTime != "" {
		javaArgs = append(javaArgs, "-totalTime", c.totalTime)
	}
	javaArgs = append(javaArgs, args...)

	return c.Base().Run(javaArgs)
}

func (c *TestJournalCrashCommand) FetchValue(key string) (string, error) {
	cmd := c.RunJavaClassCmd([]string{key})

	errBuf := &bytes.Buffer{}
	cmd.Stderr = errBuf

	log.Logger.Debugln(cmd.String())
	out, err := cmd.Output()
	if err != nil {
		return "", stacktrace.Propagate(err, "error getting conf for %v\nstderr: %v", key, errBuf.String())
	}
	return string(out), nil
}
