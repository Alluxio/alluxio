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
)

func ConsistentHash(className string) env.Command {
    return &ConsistentHashCommand{
        BaseJavaCommand: &env.BaseJavaCommand{
            CommandName:   "consistentHash",
            JavaClassName: className,
            Parameters:    []string{"consistentHash"},
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
        Use:   "consistentHash [--createCheckFile]|[--compareCheckFiles <1stCheckFilePath> <2ndCheckFilePath>]|[--cleanCheckData] ",
        Short: "This command is for checking whether the consistent hash ring is changed or not. ",
        RunE: func(cmd *cobra.Command, args []string) error {
            return c.Run(args)
        },
    })
    cmd.Flags().BoolVarP(&c.createCheckFile, "createCheckFile", "c", false, "Generate check file.")
    cmd.Flags().BoolVarP(&c.compareCheckFiles, "compareCheckFiles", "C", false, "Compare check files to see if the hash ring has changed and if data lost.")
    cmd.Flags().BoolVarP(&c.cleanCheckData, "cleanCheckData", "d", false, "Clean all check data.")
    return cmd
}

func (c *ConsistentHashCommand) Run(args []string) error {
    javaArgs := []string{}
    if c.createCheckFile {
        javaArgs = append(javaArgs, "--createCheckFile")
    }
    if c.compareCheckFiles {
        javaArgs = append(javaArgs, "--compareCheckFiles")
    }
    if c.cleanCheckData {
        javaArgs = append(javaArgs, "--cleanCheckData")
    }

    javaArgs = append(javaArgs, args...)
    return c.Base().Run(javaArgs)
}
