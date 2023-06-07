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

	"alluxio.org/cli/env"
)

var Remove = &RemoveCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "remove",
		JavaClassName: "alluxio.cli.fsadmin.FileSystemAdminShell",
		Parameters:    []string{"journal", "quorum"},
	},
}

type RemoveCommand struct {
	*env.BaseJavaCommand

	Domain        string
	ServerAddress string
}

func (c *RemoveCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *RemoveCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   Remove.CommandName,
		Short: "Removes the specified server from the quorum",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(nil)
		},
	})
	const address, domain = "address", "domain"
	cmd.Flags().StringVar(&c.Domain, domain, "", "")
	if err := cmd.MarkFlagRequired(domain); err != nil {
		panic(err)
	}
	cmd.Flags().StringVar(&c.ServerAddress, address, "", "Address of server in the format <hostname>:<port>")
	if err := cmd.MarkFlagRequired(address); err != nil {
		panic(err)
	}
	return cmd
}

func (c *RemoveCommand) Run(_ []string) error {
	if err := checkDomain(c.Domain); err != nil {
		return stacktrace.Propagate(err, "error checking domain %v", c.Domain)
	}
	return c.Base().Run([]string{"remove", "-address", c.ServerAddress, "-domain", c.Domain})
}
