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

package env

import "github.com/spf13/cobra"

const StopProcessName = "stop"

type StopProcessCommand struct {
	SoftKill bool
}

func (c *StopProcessCommand) ToCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   StopProcessName,
		Short: "Stops a process locally or a group of similar processes across the cluster",
		Long: `Stops a single process locally or a group of similar processes across the cluster.
For stopping a group, it is assumed the local host has passwordless SSH access to other nodes in the cluster.
The command will parse the hostnames to run on by reading the conf/masters and conf/workers files, depending on the process type.`,
	}
	cmd.PersistentFlags().BoolVarP(&c.SoftKill, "soft", "s", false, "Soft kill only, don't forcibly kill the process")

	for _, p := range ProcessRegistry {
		p := p
		cmd.AddCommand(p.StopCmd(&cobra.Command{
			Args: cobra.NoArgs,
			RunE: func(cmd *cobra.Command, args []string) error {
				return p.Stop(c)
			},
		}))
	}
	return cmd
}
