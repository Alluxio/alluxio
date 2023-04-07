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

package operations

import (
	"fmt"
	"strings"

	units "github.com/docker/go-units"
)

func (a AlluxioFileUtils) CachedState() (cached int64, err error) {
	var (
		command = []string{"alluxio", "fsadmin", "report"}
		stdout  string
		stderr  string
	)

	found := false
	stdout, stderr, err = a.exec(command, false)
	if err != nil {
		err = fmt.Errorf("execute command %v with err: %v stdout %s and stderr %s", command, err, stdout, stderr)
		return
	}
	str := strings.Split(stdout, "\n")

	for _, s := range str {
		if strings.Contains(s, "Used Capacity:") {
			values := strings.Fields(s)
			if len(values) == 0 {
				return cached, fmt.Errorf("Failed to parse %s", s)
			}
			cached, err = units.RAMInBytes(values[len(values)-1])
			if err != nil {
				return
			}
			found = true
		}
	}

	if !found {
		err = fmt.Errorf("Failed to find the cache in output %v", stdout)
	}

	return
}

func (a AlluxioFileUtils) CleanCache(path string) (err error) {
	var (
		command = []string{"alluxio", "fs", "free", "-f", path}
		stdout  string
		stderr  string
	)

	stdout, stderr, err = a.exec(command, false)
	if err != nil {
		err = fmt.Errorf("execute command %v with err: %v stdout %s and stderr %s", command, err, stdout, stderr)
		return
	}

	return
}
