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
	"fmt"
	"strings"

	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"

	"alluxio.org/cli/env"
)

func Ls(className string) env.Command {
	return &LsCommand{
		BaseJavaCommand: &env.BaseJavaCommand{
			CommandName:   "ls",
			JavaClassName: className,
			Parameters:    []string{"ls"},
		},
	}
}

type LsCommand struct {
	*env.BaseJavaCommand

	listDirAsFile     bool
	forceLoadMetadata bool
	isHumanReadable   bool
	omitMountInfo     bool
	pinnedFileOnly    bool
	isRecursive       bool
	isReverse         bool
	sortBy            string
	timestamp         string
}

func (c *LsCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

var (
	sortOptions = []string{
		"creationTime",
		"inMemoryPercentage",
		"lastAccessTime",
		"lastModificationTime",
		"name",
		"path",
		"size",
	}
	timestampOptions = []string{
		"createdTime",
		"lastAccessTime",
		"lastModifiedTime",
	}
)

func (c *LsCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   "ls [path]",
		Short: "Prints information for files and directories at the given path",
		Long:  `Displays information for all files and directories directly under the specified paths, including permission, owner, group, size (bytes for files or the number of children for directories), persistence state, last modified time, the percentage of content already in Alluxio, and the path`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	// special case to overwrite the -h shorthand for --help flag for --human-readable
	cmd.PersistentFlags().BoolP("help", "", false, "help for this command")

	cmd.Flags().BoolVarP(&c.listDirAsFile, "list-dir-as-file", "d", false, "List directories as files")
	cmd.Flags().BoolVarP(&c.forceLoadMetadata, "load-metadata", "f", false, "Force load metadata for immediate children in a directory")
	cmd.Flags().BoolVarP(&c.isHumanReadable, "human-readable", "h", false, "Print sizes in human readable format")
	cmd.Flags().BoolVarP(&c.omitMountInfo, "omit-mount-info", "m", false, "Omit mount point related information such as the UFS path")
	cmd.Flags().BoolVarP(&c.pinnedFileOnly, "pinned-files", "p", false, "Only show pinned files")
	cmd.Flags().BoolVarP(&c.isRecursive, "recursive", "R", false, "List subdirectories recursively")
	cmd.Flags().BoolVarP(&c.isReverse, "reverse", "r", false, "Reverse sorted order")
	cmd.Flags().StringVar(&c.sortBy, "sort", "", fmt.Sprintf("Sort entries by column, one of {%v}", strings.Join(sortOptions, "|")))
	cmd.Flags().StringVar(&c.timestamp, "timestamp", "", fmt.Sprintf("Display specified timestamp of entry, one of {%v}", strings.Join(timestampOptions, "|")))
	return cmd
}

func (c *LsCommand) Run(args []string) error {
	var javaArgs []string
	if c.listDirAsFile {
		javaArgs = append(javaArgs, "-d")
	}
	if c.forceLoadMetadata {
		javaArgs = append(javaArgs, "-f")
	}
	if c.isHumanReadable {
		javaArgs = append(javaArgs, "-h")
	}
	if c.omitMountInfo {
		javaArgs = append(javaArgs, "-m")
	}
	if c.pinnedFileOnly {
		javaArgs = append(javaArgs, "-p")
	}
	if c.isRecursive {
		javaArgs = append(javaArgs, "-R")
	}
	if c.isReverse {
		javaArgs = append(javaArgs, "-r")
	}
	if c.sortBy != "" {
		if err := checkAllowed(c.sortBy, sortOptions...); err != nil {
			return stacktrace.Propagate(err, "error validating sort options")
		}
		javaArgs = append(javaArgs, "--sort", c.sortBy)
	}
	if c.timestamp != "" {
		if err := checkAllowed(c.timestamp, timestampOptions...); err != nil {
			return stacktrace.Propagate(err, "error validating timestamp options")
		}
		javaArgs = append(javaArgs, "--timestamp", c.timestamp)
	}
	javaArgs = append(javaArgs, args...)
	return c.Base().Run(javaArgs)
}

func checkAllowed(val string, allowed ...string) error {
	for _, i := range allowed {
		if val == i {
			return nil
		}
	}
	return stacktrace.NewError("value %v must be one of %v", val, strings.Join(allowed, ","))
}
