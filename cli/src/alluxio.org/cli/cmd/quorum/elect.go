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
	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"

	"alluxio.org/cli/cmd/names"
	"alluxio.org/cli/env"
)

var Elect = &ElectCommand{
	QuorumCommand: &QuorumCommand{
		BaseJavaCommand: &env.BaseJavaCommand{
			CommandName:   "elect",
			JavaClassName: names.FileSystemAdminShellJavaClass,
			Parameters:    []string{"journal", "quorum", "elect"},
		},
		AllowedDomains: []string{DomainMaster},
	},
}

type ElectCommand struct {
	*QuorumCommand

	serverAddress string
}

func (c *ElectCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   Elect.CommandName,
		Short: "Transfers leadership of the quorum to the specified server",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(nil)
		},
	})

	const address = "address"
	cmd.Flags().StringVar(&c.serverAddress, address, "", "Address of new leader server in the format <hostname>:<port>")
	if err := cmd.MarkFlagRequired(address); err != nil {
		panic(err)
	}
	return cmd
}

func (c *ElectCommand) Run(_ []string) error {
	if err := checkDomain(c.QuorumCommand.Domain, c.QuorumCommand.AllowedDomains...); err != nil {
		return stacktrace.Propagate(err, "error checking domain %v", c.QuorumCommand.Domain)
	}
	// TODO: ignore the domain flag for now since elect command can only operate on MASTER
	//  java command needs to be updated such that it can accept the domain flag
	return c.Base().Run([]string{"-address", c.serverAddress})
}
