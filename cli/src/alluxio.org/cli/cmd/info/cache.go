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

package info

import (
	"strings"

	"github.com/spf13/cobra"

	"alluxio.org/cli/cmd/names"
	"alluxio.org/cli/env"
)

var Cache = &CacheCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "cache",
		JavaClassName: names.FileSystemAdminShellJavaClass,
		Parameters:    []string{"report", "capacity"},
	},
}

type CacheCommand struct {
	*env.BaseJavaCommand

	liveWorkers bool
	lostWorkers bool
	workersList []string // list of worker hostnames or ip addresses
}

func (c *CacheCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *CacheCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   c.CommandName,
		Short: "Reports worker capacity information",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(nil)
		},
	})
	const live, lost, worker = "live", "lost", "worker"
	cmd.Flags().BoolVar(&c.liveWorkers, live, false, "Only show live workers for capacity report")
	cmd.Flags().BoolVar(&c.lostWorkers, lost, false, "Only show lost workers for capacity report")
	cmd.Flags().StringSliceVar(&c.workersList, worker, nil, "Only show specified workers for capacity report, labeled by hostname or IP address")
	cmd.MarkFlagsMutuallyExclusive(live, lost, worker)
	return cmd
}

func (c *CacheCommand) Run(_ []string) error {
	// TODO: output all in a serializable format and filter/trim as specified by flags
	var args []string
	if c.liveWorkers {
		args = append(args, "-live")
	} else if c.lostWorkers {
		args = append(args, "-lost")
	} else if len(c.workersList) > 0 {
		args = append(args, "-workers", strings.Join(c.workersList, ","))
	}
	return c.Base().Run(args)
}
