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

var Stop = &StopCommand{}

type StopCommand struct {
	SoftKill bool
}

func (c *StopCommand) ToCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stop",
		Short: "Stop the process",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			p, err := ParseSelectedProcessType()
			if err != nil {
				return stacktrace.Propagate(err, "error determining process to stop")
			}
			return p.Stop(&env.StopOpts{
				SoftKill: c.SoftKill,
			})
		},
	}
	cmd.Flags().BoolVarP(&c.SoftKill, "softKill", "s", false, "Soft kill only, don't forcibly kill the process")

	for _, p := range env.ProcessRegistry {
		p.SetStopFlags(cmd)
	}
	return cmd
}
