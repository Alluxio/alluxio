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

func Stat(className string) env.Command {
	return &StatCommand{
		BaseJavaCommand: &env.BaseJavaCommand{
			CommandName:   "stat",
			JavaClassName: className,
			Parameters:    []string{"stat"},
		},
	}
}

type StatCommand struct {
	*env.BaseJavaCommand

	Path   string
	FileId string
	Format string
}

func (c *StatCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *StatCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   "stat",
		Short: "Displays info for the specified file or directory",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(nil)
		},
	})
	const path, fileId = "path", "file-id"
	cmd.Flags().StringVar(&c.Path, path, "", "Path to file or directory")
	cmd.Flags().StringVar(&c.FileId, fileId, "", "File id of file")
	cmd.Flags().StringVarP(&c.Format, "format", "f", "", `Display info in the given format:
  "%N": name of the file
  "%z": size of file in bytes
  "%u": owner
  "%g": group name of owner
  "%i": file id of the file
  "%y": modification time in UTC in 'yyyy-MM-dd HH:mm:ss' format
  "%Y": modification time as Unix timestamp in milliseconds
  "%b": Number of blocks allocated for file
`)
	cmd.MarkFlagsMutuallyExclusive(path, fileId)
	return cmd
}

func (c *StatCommand) Run(_ []string) error {
	if c.Path == "" && c.FileId == "" {
		return stacktrace.NewError("must set one of --path or --file-id flags")
	}

	var javaArgs []string
	if c.Format != "" {
		javaArgs = append(javaArgs, "-f", c.Format)
	}
	if c.Path != "" {
		javaArgs = append(javaArgs, c.Path)
	} else if c.FileId != "" {
		javaArgs = append(javaArgs, "--file-id", c.FileId)
	}
	return c.Base().Run(javaArgs)
}
