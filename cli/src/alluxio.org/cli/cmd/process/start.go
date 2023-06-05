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

package process

import (
	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"

	"alluxio.org/cli/env"
)

var Start = &StartCommand{}

type StartCommand struct {
	AsyncStart      bool
	SkipKillOnStart bool
}

func (c *StartCommand) ToCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "Start the process",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			p, err := ParseSelectedProcessType()
			if err != nil {
				return stacktrace.Propagate(err, "error determining process to stop")
			}

			if !c.SkipKillOnStart {
				_ = p.Stop(&env.StopOpts{})
			}
			return p.Start(&env.StartOpts{
				AsyncStart: c.AsyncStart,
			})
		},
	}
	cmd.Flags().BoolVarP(&c.SkipKillOnStart, "skipKillOnStart", "N", false, "Avoid killing previous running processes when starting")
	cmd.Flags().BoolVarP(&c.AsyncStart, "startAsync", "a", false, "Asynchronously start processes without monitoring for start completion")

	for _, p := range env.ProcessRegistry {
		p.SetStartFlags(cmd)
	}
	return cmd
}
