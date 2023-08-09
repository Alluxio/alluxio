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

package cache

import (
	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"

	"alluxio.org/cli/cmd"
	"alluxio.org/cli/env"
	"alluxio.org/log"
)

var Ttl = &TtlCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "ttl",
		JavaClassName: cmd.FileSystemShellJavaClass,
	},
}

type TtlCommand struct {
	*env.BaseJavaCommand
	set      string
	unset    string
	duration string
	action   string
}

func (c *TtlCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *TtlCommand) ToCommand() *cobra.Command {
	command := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   Format.CommandName,
		Short: "Format Alluxio worker nodes.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	command.Flags().StringVar(&c.set, "set", "", "the path of set ttl operation")
	command.Flags().StringVar(&c.duration, "duration", "", "time to live")
	command.Flags().StringVar(&c.action, "action", "delete",
		"Action to take after TTL expiry, delete or free the target")
	command.Flags().StringVar(&c.unset, "unset", "", "the path of unset ttl operation")
	command.MarkFlagsRequiredTogether("set", "duration")
	command.MarkFlagsMutuallyExclusive("set", "unset")
	command.MarkFlagsMutuallyExclusive("duration", "unset")
	command.MarkFlagsMutuallyExclusive("action", "unset")
	return command
}

func (c *TtlCommand) Run(args []string) error {
	var javaArgs []string
	if c.set != "" {
		// set ttl
		javaArgs = append(javaArgs, "setTtl", c.set, c.duration)
		if c.action == "delete" || c.action == "free" {
			log.Logger.Infof("Running action " + c.action)
			javaArgs = append(javaArgs, "--action", c.action)
		} else {
			return stacktrace.NewError(
				"invalid action, must use one of [delete, free]")
		}
	} else if c.unset != "" {
		// unset ttl
		javaArgs = append(javaArgs, "unsetTtl", c.unset)
	} else {
		return stacktrace.NewError(
			"operation and path not specified, must use one of the following flags: [set, unset]")
	}
	return c.Base().Run(javaArgs)
}
