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
	"bytes"
	"fmt"

	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"

	"alluxio.org/cli/env"
	"alluxio.org/log"
)

var Format = &FormatCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:        "format",
		JavaClassName:      "alluxio.cli.Format",
		Parameters:         []string{"master"},
		UseServerClasspath: true,
		ShellJavaOpts:      fmt.Sprintf(env.JavaOptFormat, env.ConfAlluxioLoggerType, "Console"),
	},
}

type FormatCommand struct {
	*env.BaseJavaCommand
}

func (c *FormatCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *FormatCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   Format.CommandName,
		Short: "Format the local Alluxio master journal",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	return cmd
}

func (c *FormatCommand) Run(_ []string) error {
	return c.Base().Run(nil)
}

func (c *FormatCommand) Format() error {
	cmd := c.RunJavaClassCmd(nil)
	errBuf := &bytes.Buffer{}
	cmd.Stderr = errBuf

	log.Logger.Debugln(cmd.String())
	if err := cmd.Run(); err != nil {
		return stacktrace.Propagate(err, "error running %v\nstderr: %v", c.CommandName, errBuf.String())
	}
	return nil
}
