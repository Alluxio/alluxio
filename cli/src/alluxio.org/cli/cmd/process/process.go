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
	"fmt"
	"strings"

	"github.com/palantir/stacktrace"

	"alluxio.org/cli/env"
)

var Service = &env.Service{
	Name:        "process",
	Description: "Start, stop, and other operations related to the cluster processes",
	Commands: []env.Command{
		Start,
		Stop,
	},
}

func ParseSelectedProcessType() (env.Process, error) {
	var selectedProcess env.Process
	for _, p := range env.ProcessRegistry {
		if p.Base().IsSelected() {
			if selectedProcess != nil {
				return nil, stacktrace.NewError("Cannot select multiple processes, but got %v and %v", selectedProcess.Base().Name, p.Base().Name)
			}
			selectedProcess = p
		}
	}
	if selectedProcess == nil {
		var processFlags []string
		for n := range env.ProcessRegistry {
			processFlags = append(processFlags, fmt.Sprintf("--%v", n))
		}
		return nil, stacktrace.NewError("Command must specify one of the following processes:\n%v", strings.Join(processFlags, "\n"))
	}
	return selectedProcess, nil
}
