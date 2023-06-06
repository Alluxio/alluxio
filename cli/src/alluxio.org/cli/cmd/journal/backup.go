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

package journal

import (
	"github.com/spf13/cobra"

	"alluxio.org/cli/env"
)

var Backup = &BackupCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "backup",
		JavaClassName: "alluxio.cli.fsadmin.FileSystemAdminShell",
		Parameters:    []string{"backup"},
	},
}

type BackupCommand struct {
	*env.BaseJavaCommand

	backupPath       string
	local            bool
	allowLeader      bool
	bypassDelegation bool
}

func (c *BackupCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *BackupCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   Backup.CommandName,
		Short: "Backup the local Alluxio master journal",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(nil)
		},
	})
	cmd.Flags().StringVar(&c.backupPath, "path", "", "Directory to write backup file to")
	cmd.MarkFlagRequired("path")
	cmd.Flags().BoolVar(&c.local, "local", false, "If true, writes the backup file to the local filesystem instead of root UFS")
	cmd.Flags().BoolVar(&c.allowLeader, "allow-leader", false, "If true, allows the leader to take the backup when there are no standby masters")
	cmd.Flags().BoolVar(&c.bypassDelegation, "bypass-delegation", false, "If true, takes the backup on the leader even if standby masters are available")
	return cmd
}

func (c *BackupCommand) Run(_ []string) error {
	javaArgs := []string{c.backupPath}
	if c.local {
		javaArgs = append(javaArgs, "--local")
	}
	if c.allowLeader {
		javaArgs = append(javaArgs, "--allow-leader")
	}
	if c.bypassDelegation {
		javaArgs = append(javaArgs, "--bypass-delegation")
	}
	return c.Base().Run(javaArgs)
}
