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

package cmd

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

func LaunchProcess(args []string) error {
	var cmd *exec.Cmd
	switch process := args[1]; process {
	case "master":
		cmd = exec.Command("/opt/alluxio/bin/launch-process", "master")
	case "worker":
		cmd = exec.Command("/opt/alluxio/bin/launch-process", "worker")
	case "fuse":
		cmd = exec.Command("/opt/alluxio/dora/integration/fuse/bin/alluxio-fuse", "mount", strings.Join(os.Args[2:], " "), "-f")
	case "mount":
		cmd = exec.Command("/opt/alluxio/bin/alluxio-fuse", "mount", strings.Join(os.Args[2:], " "), "-f")
	case "proxy":
		cmd = exec.Command("/opt/alluxio/bin/launch-process", "proxy")
	default:
		return errors.New(fmt.Sprintf("process name %v not found.", process))
	}
	if err := cmd.Run(); err != nil {
		return err
	}
	return nil
}
