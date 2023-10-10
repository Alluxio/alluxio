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

package info

import (
	"bytes"
	"fmt"
	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"
	"io"
	"os"
	"strings"

	"alluxio.org/cli/cmd/names"
	"alluxio.org/cli/cmd/util"
	"alluxio.org/cli/env"
)

var Report = &ReportCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "report",
		JavaClassName: names.FileSystemAdminShellJavaClass,
		Parameters:    []string{"report"},
	},
}

type ReportCommand struct {
	*env.BaseJavaCommand
	raw bool
}

func (c *ReportCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *ReportCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   fmt.Sprintf("%v [arg]", c.CommandName),
		Short: "Reports Alluxio running cluster information",
		Long: `Reports Alluxio running cluster information
[arg] can be one of the following values:
  jobservice: job service metrics information
  metrics:    metrics information
  summary:    cluster summary
  ufs:        under storage system information

Defaults to summary if no arg is provided
`,
		Args: cobra.RangeArgs(0, 1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	cmd.Flags().BoolVar(&c.raw, "raw", false,
		"Output raw JSON data instead of human-readable format for bytes, datetime, and duration.")
	return cmd
}

func (c *ReportCommand) Run(args []string) error {
	reportArg := "summary"
	if len(args) == 1 {
		options := map[string]struct{}{
			"jobservice": {},
			"metrics":    {},
			"summary":    {},
			"ufs":        {},
		}
		if _, ok := options[args[0]]; !ok {
			var cmds []string
			for c := range options {
				cmds = append(cmds, c)
			}
			return stacktrace.NewError("first argument must be one of %v", strings.Join(cmds, ", "))
		}
		reportArg = args[0]
	}

	buf := &bytes.Buffer{}
	if err := c.RunWithIO([]string{reportArg}, nil, buf, os.Stderr); err != nil {
		io.Copy(os.Stdout, buf)
		return err
	}

	if err := util.PrintProcessedJsonBytes(buf.Bytes(), c.raw); err != nil {
		return stacktrace.Propagate(err, "error formatting output to print")
	}
	return nil
}
