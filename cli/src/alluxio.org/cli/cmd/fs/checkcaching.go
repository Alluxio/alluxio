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
    "alluxio.org/cli/env"
    "github.com/spf13/cobra"
    "strconv"
)

func CheckCaching(className string) env.Command {
    return &CheckCachingCommand{
        BaseJavaCommand: &env.BaseJavaCommand{
            CommandName:   "checkCaching",
            JavaClassName: className,
            Parameters:    []string{"checkCaching"},
        },
    }
}

type CheckCachingCommand struct {
    *env.BaseJavaCommand

    sample int
    limit  int
}

func (c *CheckCachingCommand) Base() *env.BaseJavaCommand {
    return c.BaseJavaCommand
}

func (c *CheckCachingCommand) ToCommand() *cobra.Command {
    cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
        Use:   "checkCaching [path]",
        Short: "Checks if files under a path have been cached in alluxio.",
        Args:  cobra.ExactArgs(1),
        RunE: func(cmd *cobra.Command, args []string) error {
            return c.Run(args)
        },
    })
    cmd.Flags().IntVar(&c.sample, "sample", 0, "Sample ratio, 10 means sample 1 in every 10 files.")
    cmd.Flags().IntVar(&c.limit, "limit", 0, "limit, default 1000")
    return cmd
}

func (c *CheckCachingCommand) Run(args []string) error {
    javaArgs := []string{"checkCaching"}
    if c.sample != 0 {
        javaArgs = append(javaArgs, "--sample", strconv.Itoa(c.sample))
    }
    if c.limit != 0 {
        javaArgs = append(javaArgs, "--limit", strconv.Itoa(c.limit))
    }
    javaArgs = append(javaArgs, args...)
    return c.Base().Run(args)
}
