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
    "github.com/spf13/cobra"

    "alluxio.org/cli/env"
)

func Location(className string) env.Command {
    return &LocationCommand{
        BaseJavaCommand: &env.BaseJavaCommand{
            CommandName:   "location",
            JavaClassName: className,
            Parameters:    []string{"location"},
        },
    }
}

type LocationCommand struct {
    *env.BaseJavaCommand
}

func (c *LocationCommand) Base() *env.BaseJavaCommand {
    return c.BaseJavaCommand
}

func (c *LocationCommand) ToCommand() *cobra.Command {
    cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
        Use:   "location [path]",
        Short: "Displays the list of hosts storing the specified file.",
        Args:  cobra.ExactArgs(1),
        RunE: func(cmd *cobra.Command, args []string) error {
            return c.Run(args)
        },
    })
    return cmd
}

func (c *LocationCommand) Run(args []string) error {
    return c.Base().Run(args)
}
