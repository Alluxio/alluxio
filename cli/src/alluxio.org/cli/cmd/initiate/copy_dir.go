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
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"

	"alluxio.org/cli/processes"
	"alluxio.org/log"
)

var CopyDir = &CopyDirCommand{}

type CopyDirCommand struct{}

func (c *CopyDirCommand) ToCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "copyDir [path]",
		Args:  cobra.ExactArgs(1),
		Short: "Copy a path to all master/worker nodes.",
		RunE: func(cmd *cobra.Command, args []string) error {
			// get list of masters or workers, or both
			hosts, err := processes.GetHostnames([]string{processes.HostGroupMasters, processes.HostGroupWorkers})
			if err != nil {
				return stacktrace.Propagate(err, "cannot read host names")
			}

			// get name of current host, avoid rsync itself
			myHost, err := os.Hostname()
			if err != nil {
				return stacktrace.Propagate(err, "cannot read local host name")
			}

			// determine the absolute directory to copy
			absolutePath, err := filepath.Abs(args[0])
			if err != nil {
				return stacktrace.Propagate(err, "cannot find absolute path")
			}

			var errs []error
			for _, host := range hosts {
				if host == myHost {
					continue
				}
				log.Logger.Infof("RSYNCing directory %s to %s", args[0], host)

				// src path needs to end with a slash, but dst path needs to end without the slash
				dstDir := strings.TrimRight(absolutePath, "/")
				srcDir := dstDir + "/"

				cmd := exec.Command("rsync", "-az", srcDir,
					fmt.Sprintf("%s:%s", host, dstDir))
				log.Logger.Infof("Command: %s", cmd)
				if err := cmd.Run(); err != nil {
					log.Logger.Error("Error: %s", err)
					errs = append(errs, err)
				}
			}

			if len(errs) > 0 {
				var errMsgs []string
				for _, err := range errs {
					errMsgs = append(errMsgs, err.Error())
				}
				return stacktrace.NewError("At least one error encountered:\n%v", strings.Join(errMsgs, "\n"))
			}

			log.Logger.Infof("Copying path %s successful on nodes: %s", args[0], hosts)
			return nil
		},
	}
	return cmd
}
