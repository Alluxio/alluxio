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

var GetConf = &GetConfCommand{
	BaseCommand: &env.BaseCommand{
		Name:          "get",
		JavaClassName: "alluxio.cli.GetConf",
		ShellJavaOpts: fmt.Sprintf(env.JavaOptFormat, env.ConfAlluxioConfValidationEnabled, false),
	},
}

type GetConfCommand struct {
	*env.BaseCommand
}

func (c *GetConfCommand) Base() *env.BaseCommand {
	return c.BaseCommand
}

func (c *GetConfCommand) ToCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   fmt.Sprintf("%v [key]", GetConf.Name),
		Short: "Look up a configuration value by its key or print all configuration if no key is provided",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	}
	cmd.Flags().BoolVar(&c.DebugMode, "attachDebug", false, "True to attach debug opts")
	cmd.Flags().StringVar(&c.InlineJavaOpts, "javaOpts", "", `Java options to apply, ex. "-Dkey=value"`)
	// TODO: add optional args defined in the corresponding java class, such as --master, --source, --unit
	return cmd
}

func (c *GetConfCommand) FetchValue(key string) (string, error) {
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
