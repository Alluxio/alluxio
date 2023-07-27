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
	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"

	"alluxio.org/cli/env"
)

var Class = &ClassCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName: "class",
	},
}

type ClassCommand struct {
	*env.BaseJavaCommand
	mainClass string
	jarFile   string
	module    string
}

func (c *ClassCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *ClassCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   "class",
		Short: "Run the main method of an Alluxio class.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	cmd.Flags().StringVar(&c.jarFile, "jar", "",
		"Determine a JAR file to run.")
	cmd.Flags().StringVar(&c.module, "m", "",
		"Determine a module to run.")
	cmd.MarkFlagsMutuallyExclusive("jar", "m")
	return cmd
}

func (c *ClassCommand) Run(args []string) error {
	var javaArgs []string
	if c.jarFile != "" {
		javaArgs = append(javaArgs, "-jar", c.jarFile)
	} else if c.module != "" {
		javaArgs = append(javaArgs, "-m", c.module)
	} else if len(args) != 0 {
		c.JavaClassName = args[0]
	} else {
		return stacktrace.Propagate(nil, "None of JAR, module, nor a java class is specified")
	}

	if len(args) > 1 {
		javaArgs = append(javaArgs, args[1:]...)
	}
	return c.Base().Run(javaArgs)
}
