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

package conf

import (
	"bytes"
	"fmt"

	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"

	"alluxio.org/cli/env"
	"alluxio.org/log"
)

var Get = &GetCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "get",
		JavaClassName: "alluxio.cli.GetConf",
		ShellJavaOpts: fmt.Sprintf(env.JavaOptFormat, env.ConfAlluxioConfValidationEnabled, false),
	},
}

type GetCommand struct {
	*env.BaseJavaCommand

	ShowMaster bool
	ShowSource bool
	Unit       string
}

func (c *GetCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *GetCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   fmt.Sprintf("%v [key]", Get.CommandName),
		Short: "Look up a configuration value by its property key or print all configuration if no key is provided",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	cmd.Flags().BoolVar(&c.ShowMaster, "master", false, "Show configuration properties used by the master")
	cmd.Flags().BoolVar(&c.ShowSource, "source", false, "Show source of the configuration property instead of the value")
	cmd.Flags().StringVar(&c.Unit, "unit", "",
		`Unit of the value to return, converted to correspond to the given unit.
E.g., with "--unit KB", a configuration value of "4096B" will return 4
Possible options include B, KB, MB, GB, TP, PB, MS, S, M, H, D`)
	return cmd
}

func (c *GetCommand) Run(args []string) error {
	var javaArgs []string
	if c.ShowMaster {
		javaArgs = append(javaArgs, "--master")
	}
	if c.ShowSource {
		javaArgs = append(javaArgs, "--source")
	}
	if c.Unit != "" {
		javaArgs = append(javaArgs, "--unit", c.Unit)
	}
	javaArgs = append(javaArgs, args...)

	return c.Base().Run(javaArgs)
}

func (c *GetCommand) FetchValue(key string) (string, error) {
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
