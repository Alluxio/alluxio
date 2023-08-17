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

package initiate

import (
	"github.com/spf13/cobra"
)

var CopyDir = &CopyDirCommand{}

type CopyDirCommand struct{}

func (c *CopyDirCommand) ToCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "copyDir [path]",
		Args:  cobra.ExactArgs(1),
		Short: "Copy a path to all master/worker nodes",
		RunE: func(cmd *cobra.Command, args []string) error {
			// TODO: copy_dir command
			return nil
		},
	}
	return cmd
}
