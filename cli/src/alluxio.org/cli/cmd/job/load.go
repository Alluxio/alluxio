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

package job

import (
	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"

	"alluxio.org/cli/cmd/names"
	"alluxio.org/cli/env"
)

var Load = &LoadCommand{
	BaseJobCommand: &BaseJobCommand{
		BaseJavaCommand: &env.BaseJavaCommand{
			CommandName:   "load",
			JavaClassName: names.FileSystemShellJavaClass,
		},
	},
}

type LoadCommand struct {
	*BaseJobCommand
	path string

	bandwidth      string
	verify         bool
	partialListing bool
	metadataOnly   bool
}

func (c *LoadCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *LoadCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   Load.CommandName,
		Short: "Submit or manage load jobs",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	const path = "path"
	cmd.Flags().StringVar(&c.path, path, "", "[all] Source path of load operation")
	cmd.MarkFlagRequired(path)
	c.AttachOperationFlags(cmd)

	cmd.Flags().StringVar(&c.bandwidth, "bandwidth", "", "[submit] Single worker read bandwidth limit")
	cmd.Flags().BoolVar(&c.verify, "verify", false, "[submit] Run verification when load finishes and load new files if any")
	cmd.Flags().BoolVar(&c.partialListing, "partial-listing", false, "[submit] Use partial directory listing, initializing load before reading the entire directory but cannot report on certain progress details")
	cmd.Flags().BoolVar(&c.metadataOnly, "metadata-only", false, "[submit] Only load file metadata")
	return cmd
}

func (c *LoadCommand) Run(_ []string) error {
	opWithArgs, err := c.OperationWithArgs()
	if err != nil {
		return stacktrace.Propagate(err, "error parsing operation")
	}
	javaArgs := []string{"load", c.path}
	javaArgs = append(javaArgs, opWithArgs...)
	if c.bandwidth != "" {
		javaArgs = append(javaArgs, "--bandwidth", c.bandwidth)
	}
	if c.partialListing {
		javaArgs = append(javaArgs, "--partial-listing")
	}
	if c.metadataOnly {
		javaArgs = append(javaArgs, "--metadata-only")
	}
	return c.Base().Run(javaArgs)
}
