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

package generate

import (
	"alluxio.org/cli/env"
	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"
)

var Service = &env.Service{
	Name:        "generate",
	Description: "Generate files used in documentation",
	Commands: []env.Command{
		&DocsCommand{},
		DocTables,
		UserCliDoc,
	},
}

type DocsCommand struct{}

func (c *DocsCommand) ToCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "docs",
		Short: "Generate all documentation files",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			for _, c := range []env.Command{
				UserCliDoc,
				DocTables,
			} {
				if err := c.ToCommand().RunE(cmd, args); err != nil {
					return stacktrace.Propagate(err, "error running %v", c.ToCommand().Use)
				}
			}
			return nil
		},
	}
}
