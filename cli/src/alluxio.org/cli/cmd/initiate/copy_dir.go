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
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"
	"golang.org/x/crypto/ssh"

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
			// prepare client config, ssh port info
			config, port, err := processes.PrepareCommand()
			if err != nil {
				return stacktrace.Propagate(err, "prepare command failed")
			}

			// get list of masters or workers, or both
			hosts, err := processes.GetHostnames([]string{processes.HostGroupMasters, processes.HostGroupWorkers})
			if err != nil {
				return stacktrace.Propagate(err, "cannot read host names")
			}

			myHost, err := os.Hostname()
			if err != nil {
				return stacktrace.Propagate(err, "cannot read local host name")
			}

			absolutePath, err := filepath.Abs(args[0])
			if err != nil {
				return stacktrace.Propagate(err, "Invalid path to copy")
			}

			var wg sync.WaitGroup
			errs := make(chan error, len(hosts))
			for _, host := range hosts {
				if host != myHost {
					wg.Add(1)
					go func(host string) {
						defer wg.Done()
						err := copyDirectory(absolutePath, host, config, port, errs)
						errs <- err
					}(host)
				}
			}

			go func() {
				wg.Wait()
				close(errs)
			}()

			var errMsgs []string
			for err := range errs {
				errMsgs = append(errMsgs, err.Error())
			}
			if len(errMsgs) > 0 {
				return stacktrace.NewError("At least one error encountered:\n%v", strings.Join(errMsgs, "\n"))
			}
			log.Logger.Infof("CopyDir successful on nodes: %s", hosts)
			return nil
		},
	}
	return cmd
}

func copyDirectory(path, host string, config *ssh.ClientConfig, port int, errs chan<- error) error {
	log.Logger.Infof("Path: %s", path)
	conn, err := ssh.Dial("tcp", host+":"+strconv.Itoa(port), config)
	if err != nil {
		return err
	}
	defer func(conn *ssh.Client) {
		if err := conn.Close(); err != nil {
			log.Logger.Warnf("connection to %s closed, error: %s", host, err)
		} else {
			log.Logger.Debugf("connection to %s closed", host)
		}
	}(conn)

	session, err := conn.NewSession()
	if err != nil {
		return stacktrace.Propagate(err, "cannot create session at %v.", host)
	}
	defer func(session *ssh.Session) {
		if err := session.Close(); err != nil && err != io.EOF {
			log.Logger.Warnf("session at %s closed, error: %s", host, err)
		} else {
			log.Logger.Debugf("session at %s closed", host)
		}
	}(session)

	session.Stdout = os.Stdout
	session.Stderr = os.Stderr

	entries, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		filePath := filepath.Join(path, entry.Name())
		if entry.IsDir() {
			if err := copyDirectory(filePath, host, config, port, errs); err != nil {
				return err
			}
		} else {
			go func() {
				err := copyFile(path, session)
				errs <- err
			}()
		}
	}

	return nil
}

func copyFile(path string, session *ssh.Session) error {
	localFile, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func(localFile *os.File) {
		err := localFile.Close()
		if err != nil {
			log.Logger.Warnf("Local file %s closed with error: %v", path, err)
		} else {
			log.Logger.Debugf("Local file %s closed.", path)
		}
	}(localFile)

	remoteFile, err := session.StdinPipe()
	if err != nil {
		return err
	}

	errChan := make(chan error)
	go func() {
		defer func(remoteFile io.WriteCloser) {
			err := remoteFile.Close()
			if err != nil {
				log.Logger.Warnf("Remote file %s closed with error: %v", path, err)
			} else {
				log.Logger.Debugf("Remote file %s closed.", path)
			}
		}(remoteFile)
		_, err := io.Copy(remoteFile, localFile)
		errChan <- err
	}()

	if err := session.Run(fmt.Sprintf("cat > %s", path)); err != nil {
		return err
	}

	return <-errChan
}
