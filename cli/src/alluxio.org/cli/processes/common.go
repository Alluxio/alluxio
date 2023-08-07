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

var currentUsername = ""
var cliPath = filepath.Join(env.Env.EnvVar.GetString(env.ConfAlluxioHome.EnvVar), "bin", "alluxio")
var privateKeySigner ssh.Signer
var masterList []string
var workerList []string

type Result struct {
	err error
	msg string
}

// for each node, create a client and run
// TODO: now start master one by one, need to do them in parallel
func runCommand(command string, onMasters bool, onWorkers bool) error {
	// get list of masters and workers
	var nodes []string
	if onMasters {
		if len(masterList) == 0 {
			if err := getNodes("masters"); err != nil {
				return stacktrace.Propagate(err, "Cannot get masters")
			}
		}
		nodes = append(nodes, masterList...)
	}
	if onWorkers {
		if len(workerList) == 0 {
			if err := getNodes("workers"); err != nil {
				return stacktrace.Propagate(err, "Cannot get workers")
			}
		}
		nodes = append(nodes, workerList...)
	}

	// get the current user
	if currentUsername == "" {
		currentUser, err := user.Current()
		if err != nil {
			return stacktrace.Propagate(err, "Cannot find current user")
		} else {
			currentUsername = currentUser.Username
			log.Logger.Debugf("Current user: %v", currentUsername)
		}
	}

	// get public key if none
	if privateKeySigner == nil {
		if err := getPrivateKey(); err != nil {
			log.Logger.Fatalf("Cannot get private key, error: %s", err)
		}
	}

	clientConfig := &ssh.ClientConfig{
		User: currentUsername,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(privateKeySigner),
		},
		Timeout:         5 * time.Second,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	// find default ssh port
	port, err := net.LookupPort("tcp", "ssh")
	if err != nil {
		return stacktrace.Propagate(err, "Get default ssh port failed.")
	}

	// dial nodes, run sessions and close
	var waitGroup sync.WaitGroup
	results := make(chan Result)
	for _, node := range nodes {
		waitGroup.Add(1)
		node := node
		go func() {
			defer waitGroup.Done()

			// dial remote node via the ssh port
			dialAddr := fmt.Sprintf("%s:%d", node, port)
			conn, err := ssh.Dial("tcp", dialAddr, clientConfig)
			if err != nil {
				result := Result{
					err: err,
					msg: fmt.Sprintf("Dial failed to %v.", node),
				}
				results <- result
			}
			defer func(conn *ssh.Client) {
				if err := conn.Close(); err != nil {
					log.Logger.Infof("Connection to %s closed. Error: %s", node, err)
				} else {
					log.Logger.Infof("Connection to %s closed.", node)
				}
			}(conn)

			// create a session for each worker
			session, err := conn.NewSession()
			if err != nil {
				result := Result{
					err: err,
					msg: fmt.Sprintf("Cannot create session at %v.", node),
				}
				results <- result
			}
			defer func(session *ssh.Session) {
				err := session.Close()
				if err != nil && err != io.EOF {
					log.Logger.Infof("Session at %s closed. Error: %s", node, err)
				} else {
					log.Logger.Infof("Session at %s closed.", node)
				}
			}(session)

			session.Stdout = os.Stdout
			session.Stderr = os.Stderr

			// run session
			if err = session.Run(command); err != nil {
				result := Result{
					err: err,
					msg: fmt.Sprintf("Run command %v failed at %v", command, node),
				}
				results <- result
			}

			result := Result{
				err: nil,
				msg: "",
			}
			results <- result
		}()
	}

	go func() {
		waitGroup.Wait()
		close(results)
	}()

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
		log.Logger.Warningf("Run command %s failed, number of failures: %v", command, hasError)
		return nil
	}
	log.Logger.Infof("Run command %s successful on nodes: %s", command, nodes)
	return nil
}

func addStartFlags(argument string, cmd *env.StartProcessCommand) string {
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
	var command []string
	command = append(command, cliPath, argument)
	if cmd.SoftKill {
		command = append(command, "-s")
	}
	return strings.Join(command, " ")
}

func getNodes(nodeType string) error {
	var p string
	if nodeType == "masters" {
		p = filepath.Join(env.Env.EnvVar.GetString(env.ConfAlluxioConfDir.EnvVar), "masters")
	} else if nodeType == "workers" {
		p = filepath.Join(env.Env.EnvVar.GetString(env.ConfAlluxioConfDir.EnvVar), "workers")
	} else {
		return stacktrace.Propagate(fmt.Errorf("invalid nodeType"),
			"NodeType must be one of [masters, workers].")
	}

	f, err := ioutil.ReadFile(p)
	if err != nil {
		return stacktrace.Propagate(err, "Error reading hostnames at %v", p)
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
	if nodeType == "masters" {
		masterList = nodesList
	} else if nodeType == "workers" {
		workerList = nodesList
	} else {
		return stacktrace.Propagate(fmt.Errorf("invalid nodeType"),
			"NodeType must be one of [masters, workers].")
	}
	return nil
}

func getPrivateKey() error {
	// get private key
	homePath, err := os.UserHomeDir()
	if err != nil {
		return stacktrace.Propagate(err, "User home directory not found at %v", homePath)
	}
	privateKeyFile := filepath.Join(homePath, ".ssh", "id_rsa")
	privateKey, err := os.ReadFile(privateKeyFile)
	if err != nil {
		return stacktrace.Propagate(err, "Private key file not found at %v", privateKeyFile)
	}
	parsedPrivateKey, err := ssh.ParsePrivateKey(privateKey)
	if err != nil {
		return stacktrace.Propagate(err, "Cannot parse public key at %v", privateKeyFile)
	}
	privateKeySigner = parsedPrivateKey
	return nil
}
