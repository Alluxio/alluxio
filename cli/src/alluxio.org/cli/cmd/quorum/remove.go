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

package quorum

import (
	"github.com/spf13/cobra"

	"alluxio.org/cli/cmd"
	"alluxio.org/cli/env"
)

var Remove = &RemoveCommand{
	QuorumCommand: &QuorumCommand{
		BaseJavaCommand: &env.BaseJavaCommand{
			CommandName:   "remove",
			JavaClassName: cmd.FileSystemAdminShellJavaClass,
			Parameters:    []string{"journal", "quorum", "remove"},
		},
		AllowedDomains: []string{DomainJobMaster, DomainMaster},
	},
}

type RemoveCommand struct {
	*QuorumCommand

	serverAddress string
}

func (c *RemoveCommand) ToCommand() *cobra.Command {
	cmd := c.QuorumCommand.InitQuorumCmd(c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   Remove.CommandName,
		Short: "Removes the specified server from the quorum",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(nil)
		},
	}))
	const address = "address"
	cmd.Flags().StringVar(&c.serverAddress, address, "", "Address of server in the format <hostname>:<port>")
	if err := cmd.MarkFlagRequired(address); err != nil {
		panic(err)
	}
	return cmd
}

func (c *RemoveCommand) Run(_ []string) error {
	return c.QuorumCommand.Run([]string{"-address", c.serverAddress})
}
