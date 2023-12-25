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

import (
	"github.com/spf13/cobra"
)

const StartProcessName = "start"

type StartProcessCommand struct {
	AsyncStart      bool
	SkipKillOnStart bool
}

func (c *StartProcessCommand) ToCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   StartProcessName,
		Short: "Starts a process locally or a group of similar processes across the cluster",
		Long: `Starts a single process locally or a group of similar processes across the cluster.
For starting a group, it is assumed the local host has passwordless SSH access to other nodes in the cluster.
The command will parse the hostnames to run on by reading the conf/masters and conf/workers files, depending on the process type.`,
	}
	cmd.PersistentFlags().BoolVarP(&c.SkipKillOnStart, "skip-kill-prev", "N", false, "Avoid killing previous running processes when starting")
	cmd.PersistentFlags().BoolVarP(&c.AsyncStart, "async", "a", false, "Asynchronously start processes without monitoring for start completion")

	for _, p := range ProcessRegistry {
		p := p
		cmd.AddCommand(p.StartCmd(&cobra.Command{
			Args: cobra.NoArgs,
			RunE: func(cmd *cobra.Command, args []string) error {
				if !c.SkipKillOnStart {
					_ = p.Stop(&StopProcessCommand{})
				}
				return p.Start(c)
			},
		}))
	}
	return cmd
}
