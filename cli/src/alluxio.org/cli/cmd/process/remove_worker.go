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

package process

import (
	"github.com/spf13/cobra"

	"alluxio.org/cli/cmd/names"
	"alluxio.org/cli/env"
)

var RemoveWorker = &RemoveWorkerCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "remove-worker",
		JavaClassName: names.FileSystemAdminShellJavaClass,
		Parameters:    []string{"nodes", "remove"},
	},
}

type RemoveWorkerCommand struct {
	*env.BaseJavaCommand
	workerId string
}

func (c *RemoveWorkerCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *RemoveWorkerCommand) ToCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "remove-worker",
		Short: "Remove a worker from ETCD membership",
		Long: `Remove given worker from the cluster, so that clients and other workers will not consider the removed worker for services.
The worker must have been stopped before it can be safely removed from the cluster.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	}
	const name = "name"
	cmd.Flags().StringVarP(&c.workerId, name, "n", "", "Worker id")
	cmd.MarkFlagRequired(name)
	return cmd
}

func (c *RemoveWorkerCommand) Run(_ []string) error {
	return c.Base().Run([]string{"-n", c.workerId})
}
