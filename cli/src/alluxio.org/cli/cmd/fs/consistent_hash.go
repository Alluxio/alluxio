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

package fs

import (
	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"

	"alluxio.org/cli/env"
)

func ConsistentHash(className string) env.Command {
	return &ConsistentHashCommand{
		BaseJavaCommand: &env.BaseJavaCommand{
			CommandName:   "consistent-hash",
			JavaClassName: className,
			Parameters:    []string{"consistent-hash"},
		},
	}
}

type ConsistentHashCommand struct {
	*env.BaseJavaCommand

	create  bool
	compare bool
	clean   bool
}

func (c *ConsistentHashCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *ConsistentHashCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   "consistent-hash [--create]|[--compare <1stCheckFilePath> <2ndCheckFilePath>]|[--clean]",
		Short: "This command is for checking whether the consistent hash ring is changed or not",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	const create, compare, clean = "create", "compare", "clean"
	cmd.Flags().BoolVar(&c.create, create, false, "Generate check file")
	cmd.Flags().BoolVar(&c.compare, compare, false, "Compare check files to see if the hash ring has changed")
	cmd.Flags().BoolVar(&c.clean, clean, false, "Delete generated check data")
	cmd.MarkFlagsMutuallyExclusive(create, compare, clean)
	return cmd
}

func (c *ConsistentHashCommand) Run(args []string) error {
	javaArgs := []string{}
	if c.create {
		javaArgs = append(javaArgs, "--create")
	}
	if c.compare {
		if len(args) != 2 {
			return stacktrace.NewError("expect 2 arguments with --compare but got %v", len(args))
		}
		javaArgs = append(javaArgs, "--compare")
	}
	if c.clean {
		javaArgs = append(javaArgs, "--clean")
	}

	javaArgs = append(javaArgs, args...)
	return c.Base().Run(javaArgs)
}
