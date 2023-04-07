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
	"strconv"
	"strings"
	"time"

	"github.com/Alluxio/alluxio/pkg/utils/kubeclient"
	"github.com/go-logr/logr"
)

type AlluxioFileUtils struct {
	podName   string
	namespace string
	container string
	log       logr.Logger
}

func NewAlluxioFileUtils(podName string, containerName string, namespace string, log logr.Logger) AlluxioFileUtils {

	return AlluxioFileUtils{
		podName:   podName,
		namespace: namespace,
		container: containerName,
		log:       log,
	}
}

// Check if the alluxioPath exists
func (a AlluxioFileUtils) IsExist(alluxioPath string) (found bool, err error) {
	var (
		command = []string{"alluxio", "fs", "ls", alluxioPath}
		stdout  string
		stderr  string
	)

	stdout, stderr, err = a.exec(command, true)
	if err != nil {
		if strings.Contains(stdout, "does not exist") {
			err = nil
		} else {
			err = fmt.Errorf("execute command %v with err: %v stdout %s and stderr %s", command, err, stdout, stderr)
			return
		}
	} else {
		found = true
	}

	return
}

// Load the metadata
func (a AlluxioFileUtils) LoadMetaData(alluxioPath string) (err error) {
	var (
		// command = []string{"alluxio", "fs", "-Dalluxio.user.file.metadata.sync.interval=0", "ls", "-R", alluxioPath}
		// command = []string{"alluxio", "fs", "-Dalluxio.user.file.metadata.sync.interval=0", "count", alluxioPath}
		command = []string{"alluxio", "fs", "ls", "-R", alluxioPath}
		stdout  string
		stderr  string
	)

	start := time.Now()
	stdout, stderr, err = a.exec(command, false)
	duration := time.Since(start)
	a.log.Info("Load MetaData took times to run", "period", duration)
	if err != nil {
		err = fmt.Errorf("execute command %v with err: %v stdout %s and stderr %s", command, err, stdout, stderr)
		return
	}

	return
}

func (a AlluxioFileUtils) Mkdir(alluxioPath string) (err error) {
	var (
		command = []string{"alluxio", "fs", "mkdir", alluxioPath}
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

func (a AlluxioFileUtils) Mount(alluxioPath string,
	ufsPath string,
	options map[string]string,
	readOnly bool,
	shared bool) (err error) {

	// exist, err := a.IsExist(alluxioPath)
	// if err != nil {
	// 	return err
	// }

	// if !exist {
	// 	err = a.Mkdir(alluxioPath)
	// 	if err != nil {
	// 		return err
	// 	}
	// }

	var (
		command = []string{"alluxio", "fs", "mount"}
		stderr  string
		stdout  string
	)

	if readOnly {
		command = append(command, "--readonly")
	}

	if shared {
		command = append(command, "--shared")
	}

	for key, value := range options {
		command = append(command, "--option", fmt.Sprintf("%s=%s", key, value))
	}

	command = append(command, alluxioPath, ufsPath)

	stdout, stderr, err = a.exec(command, false)
	if err != nil {
		err = fmt.Errorf("execute command %v with err: %v stdout %s and stderr %s", command, err, stdout, stderr)
		return
	}

	return
}

func (a AlluxioFileUtils) IsMounted(alluxioPath string) (mounted bool, err error) {
	var (
		command = []string{"alluxio", "fs", "mount"}
		stdout  string
		stderr  string
	)

	stdout, stderr, err = a.exec(command, true)
	if err != nil {
		return mounted, fmt.Errorf("execute command %v with err: %v stdout %s and stderr %s", command, err, stdout, stderr)
	}

	results := strings.Split(stdout, "\n")

	for _, line := range results {
		fields := strings.Fields(line)
		a.log.Info("parse output of isMounted", "alluxioPath", alluxioPath, "fields", fields)
		if fields[2] == alluxioPath {
			mounted = true
			return mounted, nil
		}
	}

	// pattern := fmt.Sprintf(" on %s ", alluxioPath)
	// if strings.Contains(stdout, pattern) {
	// 	mounted = true
	// }

	return mounted, err
}

// Check if it's ready
func (a AlluxioFileUtils) Ready() (ready bool) {
	var (
		command = []string{"alluxio", "fsadmin", "report"}
	)

	_, _, err := a.exec(command, true)
	if err == nil {
		ready = true
	}

	return ready
}

func (a AlluxioFileUtils) Du(alluxioPath string) (ufs uint64, cached uint64, err error) {
	var (
		command = []string{"alluxio", "fs", "du", "-s", alluxioPath}
		stdout  string
		stderr  string
	)

	stdout, stderr, err = a.exec(command, false)
	if err != nil {
		err = fmt.Errorf("execute command %v with err: %v stdout %s and stderr %s", command, err, stdout, stderr)
		return
	}
	str := strings.Split(stdout, "\n")

	if len(str) != 2 {
		err = fmt.Errorf("Failed to parse %s in Du method", str)
		return
	}

	data := strings.Fields(str[1])
	if len(data) != 4 {
		err = fmt.Errorf("Failed to parse %s in Du method", data)
		return
	}

	ufs, err = strconv.ParseUint(data[0], 10, 64)
	if err != nil {
		return
	}

	cached, err = strconv.ParseUint(data[1], 10, 64)
	if err != nil {
		return
	}

	return
}

// The count of the Alluxio Filesystem
func (a AlluxioFileUtils) Count(alluxioPath string) (fileCount uint64, folderCount uint64, total uint64, err error) {
	var (
		command = []string{"alluxio", "fs", "count", alluxioPath}
		stdout  string
		stderr  string
	)

	stdout, stderr, err = a.exec(command, false)
	if err != nil {
		err = fmt.Errorf("execute command %v with err: %v stdout %s and stderr %s", command, err, stdout, stderr)
		return
	}

	// [File Count Folder Count Total Bytes 1152 4 154262709011]
	str := strings.Split(stdout, "\n")

	if len(str) != 2 {
		err = fmt.Errorf("Failed to parse %s in Count method", str)
		return
	}

	data := strings.Fields(str[1])
	if len(data) != 3 {
		err = fmt.Errorf("Failed to parse %s in Count method", data)
		return
	}

	fileCount, err = strconv.ParseUint(data[0], 10, 64)
	if err != nil {
		return
	}

	folderCount, err = strconv.ParseUint(data[1], 10, 64)
	if err != nil {
		return
	}

	total, err = strconv.ParseUint(data[2], 10, 64)
	if err != nil {
		return
	}

	return
}

func (a AlluxioFileUtils) exec(command []string, verbose bool) (stdout string, stderr string, err error) {
	stdout, stderr, err = kubeclient.ExecCommandInContainer(a.podName, a.container, a.namespace, command)
	if err != nil {
		a.log.Info("Stdout", "Command", command, "Stdout", stdout)
		a.log.Error(err, "Failed", "Command", command, "FailedReason", stderr)
		return
	}
	if verbose {
		a.log.Info("Stdout", "Command", command, "Stdout", stdout)
	}

	return
}
