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

	"alluxio.org/cli/env"
)

var Elect = &ElectCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "elect",
		JavaClassName: "alluxio.cli.fsadmin.FileSystemAdminShell",
		Parameters:    []string{"journal", "quorum"},
	},
}

type ElectCommand struct {
	*env.BaseJavaCommand

	ServerAddress string
}

func (c *ElectCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *ElectCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   Elect.CommandName,
		Short: "Transfers leadership of the quorum to the specified server",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(nil)
		},
	})
	// TODO: to be consistent with other quorum commands, should this have a domain flag?
	const address = "address"
	cmd.Flags().StringVar(&c.ServerAddress, address, "", "Address of new leader server in the format <hostname>:<port>")
	if err := cmd.MarkFlagRequired(address); err != nil {
		panic(err)
	}
	return cmd
}

func (c *ElectCommand) Run(_ []string) error {
	return c.Base().Run([]string{"elect", "-address", c.ServerAddress})
}
