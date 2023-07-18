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

var TestHdfsMount = &TestHdfsMountCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "testHdfsMount",
		JavaClassName: "alluxio.cli.ValidateHdfsMount",
		ShellJavaOpts: fmt.Sprintf(env.JavaOptFormat, env.ConfAlluxioLoggerType, "Console"),
	},
}

type TestHdfsMountCommand struct {
	*env.BaseJavaCommand
	path     string
	readonly bool
	shared   bool
	option   string
}

func (c *TestHdfsMountCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *TestHdfsMountCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   "testHdfsMount [--readonly] [--shared] [--option <key=val>] [--path <hdfsURI>]",
		Short: "Tests runs a set of validations against the given hdfs path.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	cmd.Flags().StringVar(&c.path, "path", "",
		"[required] specifies the HDFS path you want to validate.")
	cmd.Flags().BoolVar(&c.readonly, "readonly", false,
		"mount point is readonly in Alluxio.")
	cmd.Flags().BoolVar(&c.shared, "shared", false,
		"mount point is shared.")
	cmd.Flags().StringVar(&c.option, "option", "",
		"options associated with this mount point.")
	return cmd
}

func (c *TestHdfsMountCommand) Run(args []string) error {
	var javaArgs []string
	if c.readonly != false {
		javaArgs = append(javaArgs, "--readonly")
	}
	if c.shared != false {
		javaArgs = append(javaArgs, "--shared")
	}
	if c.option != "" {
		javaArgs = append(javaArgs, "--option", c.option)
	}
	if c.path != "" {
		javaArgs = append(javaArgs, c.path)
	}
	javaArgs = append(javaArgs, args...)

	return c.Base().Run(javaArgs)
}

func (c *TestHdfsMountCommand) FetchValue(key string) (string, error) {
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
