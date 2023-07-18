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

var TestRun = &TestRunCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "testRun",
		JavaClassName: "alluxio.cli.TestRunner",
		ShellJavaOpts: fmt.Sprintf(env.JavaOptFormat, env.ConfAlluxioLoggerType, "Console"),
	},
}

type TestRunCommand struct {
	*env.BaseJavaCommand
	directory string
	operation string
	readType  string
	workers   string
	writeType string
}

func (c *TestRunCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *TestRunCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   "testRun [options]",
		Short: "Run all end-to-end tests, or a specific test, on an Alluxio cluster.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	cmd.Flags().StringVar(&c.directory, "directory", "",
		"Alluxio path for the tests working directory. Default: /")
	cmd.Flags().StringVar(&c.operation, "operation", "",
		"The operation to test, either BASIC or BASIC_NON_BYTE_BUFFER. \n"+
			"By default both operations are tested.")
	cmd.Flags().StringVar(&c.readType, "readType", "",
		"The read type to use, one of NO_CACHE, CACHE, CACHE_PROMOTE. \n"+
			"By default all readTypes are tested.")
	cmd.Flags().StringVar(&c.workers, "workers", "",
		"Alluxio worker addresses to run tests on. \n"+
			"If not specified, random ones will be used.")
	cmd.Flags().StringVar(&c.writeType, "writeType", "",
		"The write type to use, one of MUST_CACHE, CACHE_THROUGH, THROUGH, ASYNC_THROUGH. \n"+
			"By default all writeTypes are tested.")
	return cmd
}

func (c *TestRunCommand) Run(args []string) error {
	var javaArgs []string
	if c.directory != "" {
		javaArgs = append(javaArgs, "--directory", c.directory)
	}
	if c.operation != "" {
		javaArgs = append(javaArgs, "--operation", c.operation)
	}
	if c.readType != "" {
		javaArgs = append(javaArgs, "--readType", c.readType)
	}
	if c.workers != "" {
		javaArgs = append(javaArgs, "--workers", c.workers)
	}
	if c.writeType != "" {
		javaArgs = append(javaArgs, "--writeType", c.writeType)
	}
	javaArgs = append(javaArgs, args...)

	return c.Base().Run(javaArgs)
}

func (c *TestRunCommand) FetchValue(key string) (string, error) {
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
