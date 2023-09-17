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
    "github.com/palantir/stacktrace"
    "github.com/spf13/cobra"
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

    createCheckFile   bool
    compareCheckFiles bool
    cleanCheckData    bool
}

func (c *ConsistentHashCommand) Base() *env.BaseJavaCommand {
    return c.BaseJavaCommand
}

func (c *ConsistentHashCommand) ToCommand() *cobra.Command {
    cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
        Use:   "consistent-hash [--create]|[--compare <1stCheckFilePath> <2ndCheckFilePath>]|[--clean] ",
        Short: "This command is for checking whether the consistent hash ring is changed or not",
        RunE: func(cmd *cobra.Command, args []string) error {
            return c.Run(args)
        },
    })
    cmd.Flags().BoolVar(&c.createCheckFile, "create", false, "Generate check file.")
    cmd.Flags().BoolVar(&c.compareCheckFiles, "compare", false, "Compare check files to see if the hash ring has changed and if data lost.")
    cmd.Flags().BoolVar(&c.cleanCheckData, "clean", false, "Clean all check data.")
    cmd.MarkFlagsMutuallyExclusive("create", "compare", "clean")
    return cmd
}

func (c *ConsistentHashCommand) Run(args []string) error {
    javaArgs := []string{}
    if c.createCheckFile {
        javaArgs = append(javaArgs, "--create")
    }
    if c.compareCheckFiles {
        if len(args) != 2 {
            return stacktrace.NewError("expect 2 arguments with --compare-check-files but got %v", len(args))
        }
        javaArgs = append(javaArgs, "--compare")
    }
    if c.cleanCheckData {
        javaArgs = append(javaArgs, "--clean")
    }

    javaArgs = append(javaArgs, args...)
    return c.Base().Run(javaArgs)
}
