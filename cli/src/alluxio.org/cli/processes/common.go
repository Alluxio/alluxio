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

package processes

import (
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/palantir/stacktrace"
	"golang.org/x/crypto/ssh"

	"alluxio.org/cli/cmd/names"
	"alluxio.org/cli/env"
	"alluxio.org/log"
)

type HostnamesFile struct {
	Name      string
	Mutex     *sync.Mutex
	Once      *sync.Once
	Hostnames []string
}

func NewHostnamesFile(name string) *HostnamesFile {
	return &HostnamesFile{
		Name:  name,
		Mutex: &sync.Mutex{},
		Once:  &sync.Once{},
	}
}

// RunSshCommand parses hostnames from the hosts files such as `conf/masters` and `conf/workers`
// For each hostname, create an SSH client and run the given command
func RunSshCommand(command string, hostGroups ...string) error {
	// prepare client config, ssh port info
	config, port, err := prepareCommand()
	if err != nil {
		return stacktrace.Propagate(err, "prepare command failed")
	}

	// get list of masters or workers, or both
	hosts, err := GetHostnames(hostGroups)
	if err != nil {
		return stacktrace.Propagate(err, "cannot read host names")
	}

	// create wait group and channels
	var wg sync.WaitGroup
	errs := make(chan error)
	for _, h := range hosts {
		h := h
		wg.Add(1)

		go func() {
			defer wg.Done()
			// dial on target remote address with given host, config and ssh port
			dialAddr := fmt.Sprintf("%s:%d", h, port)
			conn, err := ssh.Dial("tcp", dialAddr, config)
			if err != nil {
				errs <- stacktrace.Propagate(err, "dial failed to %v", h)
				return
			}
			defer func(conn *ssh.Client) {
				if err := conn.Close(); err != nil {
					log.Logger.Warnf("connection to %s closed, error: %s", h, err)
				} else {
					log.Logger.Debugf("connection to %s closed", h)
				}
			}(conn)

			// create and set up a session
			session, err := conn.NewSession()
			if err != nil {
				errs <- stacktrace.Propagate(err, "cannot create session at %v.", h)
				return
			}
			defer func(session *ssh.Session) {
				if err := session.Close(); err != nil && err != io.EOF {
					log.Logger.Warnf("session at %s closed, error: %s", h, err)
				} else {
					log.Logger.Debugf("session at %s closed", h)
				}
			}(session)

			session.Stdout = os.Stdout
			session.Stderr = os.Stderr

			// run command on the session, output errors
			if err = session.Run(command); err != nil {
				errs <- stacktrace.Propagate(err, "run command %v failed at %v", command, h)
				return
			}
		}()
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
	log.Logger.Infof("Command %s successful on nodes: %s", command, hosts)
	return nil
}

func prepareCommand() (*ssh.ClientConfig, int, error) {
	// get the current user
	cu, err := user.Current()
	if err != nil {
		return nil, 0, stacktrace.Propagate(err, "cannot find current user")
	}
	cuName := cu.Username
	log.Logger.Debugf("current user: %v", cuName)

	// get public key
	signer, err := getSigner()
	if err != nil {
		return nil, 0, stacktrace.Propagate(err, "cannot get private key")
	}

	// find default ssh port
	port, e := net.LookupPort("tcp", "ssh")
	if e != nil {
		return nil, 0, stacktrace.Propagate(err, "cannot find default ssh port")
	}

	// set client config with current user and signer
	config := &ssh.ClientConfig{
		User: cuName,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
		Timeout:         5 * time.Second,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
	return config, port, nil
}

func addStartFlags(cmd *env.StartProcessCommand, arguments ...string) string {
	cliPath := filepath.Join(env.Env.EnvVar.GetString(env.ConfAlluxioHome.EnvVar), names.BinAlluxio)
	var command []string
	command = append(command, cliPath)
	command = append(command, arguments...)
	if cmd.AsyncStart {
		command = append(command, "-a")
	}
	if cmd.SkipKillOnStart {
		command = append(command, "-N")
	}
	return strings.Join(command, " ")
}

func addStopFlags(cmd *env.StopProcessCommand, arguments ...string) string {
	cliPath := filepath.Join(env.Env.EnvVar.GetString(env.ConfAlluxioHome.EnvVar), names.BinAlluxio)
	var command []string
	command = append(command, cliPath)
	command = append(command, arguments...)
	if cmd.SoftKill {
		command = append(command, "-s")
	}
	return strings.Join(command, " ")
}

const (
	HostGroupMasters = "masters"
	HostGroupWorkers = "workers"
)

func GetHostnames(hostGroups []string) ([]string, error) {
	var hosts []string
	for _, hostGroup := range hostGroups {
		switch hostGroup {
		case HostGroupMasters, HostGroupWorkers:
			hostnames, err := NewHostnamesFile(hostGroup).getHostnames()
			if err != nil {
				return nil, stacktrace.Propagate(err, "error listing hostnames from %v", hostGroup)
			}
			hosts = append(hosts, hostnames...)
		default:
			return nil, stacktrace.NewError("unknown hosts file: %v", hostGroup)
		}

	}
	return hosts, nil
}

func (f *HostnamesFile) getHostnames() ([]string, error) {
	f.Mutex.Lock()
	defer f.Mutex.Unlock()
	var parseErr error
	f.Once.Do(func() {
		p := filepath.Join(env.Env.EnvVar.GetString(env.ConfAlluxioConfDir.EnvVar), f.Name)
		file, err := ioutil.ReadFile(p)
		if err != nil {
			parseErr = err
		}
		for _, line := range strings.Split(string(file), "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "#") {
				continue
			}
			if strings.TrimSpace(line) != "" {
				f.Hostnames = append(f.Hostnames, line)
			}
		}
	})
	if parseErr != nil {
		return nil, stacktrace.Propagate(parseErr, "error parsing hostnames file")
	}
	return f.Hostnames, nil
}

func getSigner() (ssh.Signer, error) {
	// get private key
	homePath, err := os.UserHomeDir()
	if err != nil {
		return nil, stacktrace.Propagate(err, "user home directory not found at %v", homePath)
	}
	privateKeyFile := filepath.Join(homePath, ".ssh", "id_rsa")
	privateKey, err := os.ReadFile(privateKeyFile)
	if err != nil {
		return nil, stacktrace.Propagate(err, "private key file not found at %v", privateKeyFile)
	}
	parsedPrivateKey, err := ssh.ParsePrivateKey(privateKey)
	if err != nil {
		return nil, stacktrace.Propagate(err, "cannot parse public key at %v", privateKeyFile)
	}
	return parsedPrivateKey, nil
}
