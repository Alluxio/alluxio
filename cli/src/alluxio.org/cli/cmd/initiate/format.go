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

var Format = &FormatCommand{}

type FormatCommand struct {
	only bool
}

func (c *FormatCommand) ToCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "format",
		Args:  cobra.NoArgs,
		Short: "Format Alluxio master and all workers",
		RunE: func(cmd *cobra.Command, args []string) error {
			// TODO: format command
			return nil
		},
	}
	cmd.Flags().BoolVarP(&c.only, "s", "s", false,
		"if -s specified, only format if underfs is local and doesn't already exist")
	return cmd
}
