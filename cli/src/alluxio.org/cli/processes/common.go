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

	"alluxio.org/cli/env"
	"alluxio.org/log"
)

type Result struct {
	err error
	msg string
}

// for each node, create a client and run
func runCommand(command string, mode string) error {
	// prepare client config, ssh port info
	config, port, err := prepareCommand(mode)
	if err != nil {
		return stacktrace.Propagate(err, "prepare command failed")
	}

	// get list of masters or workers, or both
	nodes, err := readNodes(mode)
	if err != nil {
		return stacktrace.Propagate(err, "cannot read name of masters or workers")
	}

	// create wait group and channels
	var wg sync.WaitGroup
	results := make(chan Result)
	for _, n := range nodes {
		wg.Add(1)
		node := n

		go func() {
			defer wg.Done()
			// dial nodes on target node with given config and ssh port
			dialAddr := fmt.Sprintf("%s:%d", node, port)
			conn, err := ssh.Dial("tcp", dialAddr, config)
			if err != nil {
				result := Result{
					err: err,
					msg: fmt.Sprintf("dial failed to %v", node),
				}
				results <- result
			}
			defer func(conn *ssh.Client) {
				if err := conn.Close(); err != nil {
					log.Logger.Infof("connection to %s closed, error: %s", node, err)
				} else {
					log.Logger.Infof("connection to %s closed", node)
				}
			}(conn)

			// create and set up a session
			session, err := conn.NewSession()
			if err != nil {
				result := Result{
					err: err,
					msg: fmt.Sprintf("cannot create session at %v.", node),
				}
				results <- result
			}
			defer func(session *ssh.Session) {
				err := session.Close()
				if err != nil && err != io.EOF {
					log.Logger.Infof("session at %s closed, error: %s", node, err)
				} else {
					log.Logger.Infof("session at %s closed", node)
				}
			}(session)

			session.Stdout = os.Stdout
			session.Stderr = os.Stderr

			// run command on the session, output errors
			if err = session.Run(command); err != nil {
				result := Result{
					err: err,
					msg: fmt.Sprintf("run command %v failed at %v", command, node),
				}
				results <- result
			}

			// if no errors, return nil
			result := Result{
				err: nil,
				msg: "",
			}
			results <- result
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	return errorHandler(command, nodes, results)
}

func prepareCommand(mode string) (*ssh.ClientConfig, int, error) {
	// get the current user
	cu, err := user.Current()
	if err != nil {
		return &ssh.ClientConfig{}, -1,
			stacktrace.Propagate(err, "cannot find current user")
	}
	cuName := cu.Username
	log.Logger.Debugf("current user: %v", cuName)

	// get public key
	signer, err := getSigner()
	if err != nil {
		return &ssh.ClientConfig{}, -1,
			stacktrace.Propagate(err, "cannot get private key")
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

	// find default ssh port
	port, err := net.LookupPort("tcp", "ssh")
	if err != nil {
		return &ssh.ClientConfig{}, -1,
			stacktrace.Propagate(err, "get default ssh port failed")
	}
	return config, port, nil
}

func addStartFlags(argument string, cmd *env.StartProcessCommand) string {
	cliPath := filepath.Join(env.Env.EnvVar.GetString(env.ConfAlluxioHome.EnvVar), "bin", "alluxio")
	var command []string
	command = append(command, cliPath, argument)
	if cmd.AsyncStart {
		command = append(command, "-a")
	}
	if cmd.SkipKillOnStart {
		command = append(command, "-N")
	}
	return strings.Join(command, " ")
}

func addStopFlags(argument string, cmd *env.StopProcessCommand) string {
	cliPath := filepath.Join(env.Env.EnvVar.GetString(env.ConfAlluxioHome.EnvVar), "bin", "alluxio")
	var command []string
	command = append(command, cliPath, argument)
	if cmd.SoftKill {
		command = append(command, "-s")
	}
	return strings.Join(command, " ")
}

func readNodes(mode string) ([]string, error) {
	if mode != "master" && mode != "worker" && mode != "all" {
		return nil, stacktrace.Propagate(fmt.Errorf("invalid mode for readNodes"),
			"available readNodes modes: [master, worker, all]")
	}

	var nodes []string
	if mode == "master" || mode == "all" {
		masterList, err := readFiles("masters")
		if err != nil {
			return nil, stacktrace.Propagate(err, "cannot get masters")
		}
		nodes = append(nodes, masterList...)
	}
	if mode == "worker" || mode == "all" {
		workerList, err := readFiles("workers")
		if err != nil {
			return nil, stacktrace.Propagate(err, "cannot get workers")
		}
		nodes = append(nodes, workerList...)
	}
	return nodes, nil
}

func readFiles(fileName string) ([]string, error) {
	p := filepath.Join(env.Env.EnvVar.GetString(env.ConfAlluxioConfDir.EnvVar), fileName)
	f, err := ioutil.ReadFile(p)
	if err != nil {
		return nil, stacktrace.Propagate(err, "error reading hostnames at %v", p)
	}
	var nodesList []string
	for _, line := range strings.Split(string(f), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		if strings.TrimSpace(line) != "" {
			nodesList = append(nodesList, line)
		}
	}
	return nodesList, nil
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

func errorHandler(command string, nodes []string, results chan Result) error {
	hasError := 0
	for result := range results {
		if result.err != nil {
			hasError++
			err := stacktrace.Propagate(result.err, result.msg)
			if err != nil {
				return err
			}
		}
	}

	if hasError != 0 {
		return stacktrace.Propagate(fmt.Errorf("run command %s failed", command),
			"number of failures: %v", hasError)
	}
	log.Logger.Infof("run command %s successful on nodes: %s", command, nodes)
	return nil
}
