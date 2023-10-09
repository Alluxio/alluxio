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
    "strconv"

    "github.com/spf13/cobra"

    "alluxio.org/cli/env"
)

func CheckCached(className string) env.Command {
    return &CheckCachedCommand{
        BaseJavaCommand: &env.BaseJavaCommand{
            CommandName:   "check-cached",
            JavaClassName: className,
            Parameters:    []string{"check-cached"},
        },
    }
}

type CheckCachedCommand struct {
    *env.BaseJavaCommand

    sample int
    limit  int
}

func (c *CheckCachedCommand) Base() *env.BaseJavaCommand {
    return c.BaseJavaCommand
}

func (c *CheckCachedCommand) ToCommand() *cobra.Command {
    cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
        Use:   "check-cached [path]",
        Short: "Checks if files under a path have been cached in alluxio.",
        Args:  cobra.ExactArgs(1),
        RunE: func(cmd *cobra.Command, args []string) error {
            return c.Run(args)
        },
    })
    cmd.Flags().IntVar(&c.sample, "sample", 1, "Sample ratio, 10 means sample 1 in every 10 files.")
    cmd.Flags().IntVar(&c.limit, "limit", 1000, "Limit number of files to check")
    return cmd
}

func (c *CheckCachedCommand) Run(args []string) error {
    var javaArgs []string
    if c.sample != 0 {
        javaArgs = append(javaArgs, "--sample", strconv.Itoa(c.sample))
    }
    if c.limit != 0 {
        javaArgs = append(javaArgs, "--limit", strconv.Itoa(c.limit))
    }
    javaArgs = append(javaArgs, args...)
    return c.Base().Run(javaArgs)
}
