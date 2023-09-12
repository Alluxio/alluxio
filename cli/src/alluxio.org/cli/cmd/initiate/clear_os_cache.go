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
	"io/ioutil"
	"os/exec"

	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"
)

var ClearOSCache = &ClearOSCacheCommand{}

type ClearOSCacheCommand struct{}

func (c *ClearOSCacheCommand) ToCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "clearOSCache",
		Args:  cobra.NoArgs,
		Short: "Clear OS buffer cache of the machine",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := exec.Command("sync").Run(); err != nil {
				return stacktrace.Propagate(err, "error running sync")
			}
			if err := ioutil.WriteFile("/proc/sys/vm/drop_caches", []byte("3"), 0644); err != nil {
				return stacktrace.Propagate(err, "error running drop_caches")
			}
			return nil
		},
	}
	return cmd
}
