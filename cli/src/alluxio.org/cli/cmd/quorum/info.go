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

	"alluxio.org/cli/cmd/names"
	"alluxio.org/cli/env"
)

var Info = &InfoCommand{
	QuorumCommand: &QuorumCommand{
		BaseJavaCommand: &env.BaseJavaCommand{
			CommandName:   "info",
			JavaClassName: names.FileSystemAdminShellJavaClass,
			Parameters:    []string{"journal", "quorum", "info"},
		},
		AllowedDomains: []string{DomainJobMaster, DomainMaster},
	},
}

type InfoCommand struct {
	*QuorumCommand
}

func (c *InfoCommand) ToCommand() *cobra.Command {
	cmd := c.QuorumCommand.InitQuorumCmd(c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   Info.CommandName,
		Short: "Shows quorum information",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(nil)
		},
	}))
	return cmd
}

func (c *InfoCommand) Run(_ []string) error {
	return c.QuorumCommand.Run(nil)
}
