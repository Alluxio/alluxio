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

	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"

	"alluxio.org/cli/env"
)

func Cp(className string) env.Command {
	return &CopyCommand{
		BaseJavaCommand: &env.BaseJavaCommand{
			CommandName:   "cp",
			JavaClassName: className,
		},
	}
}

type CopyCommand struct {
	*env.BaseJavaCommand

	bufferSize  string
	isRecursive bool
	preserve    bool
	threads     int
}

func (c *CopyCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *CopyCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   "cp [srcPath] [dstPath]",
		Short: "Copy a file or directory",
		Long: `Copies a file or directory in the Alluxio filesystem or between local and Alluxio filesystems.
The file:// scheme indicates a local filesystem path and the alluxio:// scheme or no scheme indicates an Alluxio filesystem path.`,
		Example: `# Copy within the Alluxio filesystem
$ ./bin/alluxio fs cp /file1 /file2

# Copy a local file to the Alluxio filesystem
$ ./bin/alluxio fs cp file:///file1 /file2

# Copy a file in Alluxio to local
$ ./bin/alluxio fs cp alluxio:///file1 file:///file2

# Recursively copy a directory within the Alluxio filesystem
$ ./bin/alluxio fs cp -R /dir1 /dir2
`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	cmd.Flags().StringVar(&c.bufferSize, "buffer-size", "", "Read buffer size when coping to or from local, with defaults of 64MB and 8MB respectively")
	cmd.Flags().BoolVarP(&c.isRecursive, "recursive", "R", false, "True to copy the directory subtree to the destination directory")
	cmd.Flags().BoolVarP(&c.preserve, "preserve", "p", false, "Preserve file permission attributes when copying files; all ownership, permissions, and ACLs will be preserved")
	cmd.Flags().IntVar(&c.threads, "thread", 0, "Number of threads used to copy files in parallel, defaults to 2 * CPU cores")
	return cmd
}

func (c *CopyCommand) Run(args []string) error {
	if c.threads < 0 {
		return stacktrace.NewError("thread value must be positive but was %v", c.threads)
	}

	javaArgs := []string{"cp"}
	if c.bufferSize != "" {
		javaArgs = append(javaArgs, "--buffersize", c.bufferSize)
	}
	if c.isRecursive {
		javaArgs = append(javaArgs, "--recursive")
	}
	if c.preserve {
		javaArgs = append(javaArgs, "--preserve")
	}
	if c.threads != 0 {
		javaArgs = append(javaArgs, "--thread", strconv.Itoa(c.threads))
	}
	javaArgs = append(javaArgs, args...)
	return c.Base().Run(javaArgs)
}
