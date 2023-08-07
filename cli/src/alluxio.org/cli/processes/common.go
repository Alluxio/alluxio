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

// for each master, create a client and run
// TODO: now start master one by one, need to do them in parallel
func runCommand(command string, onMasters bool, onWorkers bool) error {
	// get list of masters and workers
	var nodes []string
	if onMasters {
		if len(masterList) == 0 {
			if err := getNodes(true); err != nil {
				log.Logger.Fatalf("Cannot get masters, error: %s", err)
			}
		}
		nodes = append(nodes, masterList...)
	}
	if onWorkers {
		if len(workerList) == 0 {
			if err := getNodes(false); err != nil {
				log.Logger.Fatalf("Cannot get workers, error: %s", err)
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

	// dial nodes, run sessions and close
	var errors []error
	for _, node := range nodes {
		conn, err := dialConnection(node, privateKeySigner)
		if err != nil {
			errors = append(errors, err)
			continue
		}
		if err := runSession(node, conn, command); err != nil {
			errors = append(errors, err)
		}
		if err := conn.Close(); err != nil {
			log.Logger.Infof("Connection to %s closed. Error: %s", node, err)
		} else {
			log.Logger.Infof("Connection to %s closed.", node)
		}
	}

	if len(errors) != 0 {
		log.Logger.Warningf("Run command %s failed, number of failures: %v", command, len(errors))
		return stacktrace.Propagate(errors[0], "First error: ")
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

func getNodes(isMasters bool) error {
	var FilePath string
	if isMasters {
		FilePath = filepath.Join(env.Env.EnvVar.GetString(env.ConfAlluxioConfDir.EnvVar), "masters")
	} else {
		FilePath = filepath.Join(env.Env.EnvVar.GetString(env.ConfAlluxioConfDir.EnvVar), "workers")
	}
	File, err := ioutil.ReadFile(FilePath)
	if err != nil {
		return stacktrace.Propagate(err, "Error reading hostnames at %v", FilePath)
	}
	var nodesList []string
	for _, line := range strings.Split(string(File), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		if strings.TrimSpace(line) != "" {
			nodesList = append(nodesList, line)
		}
	}
	if isMasters {
		masterList = nodesList
	} else {
		workerList = nodesList
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

func dialConnection(remoteAddress string, signer ssh.Signer) (*ssh.Client, error) {
	clientConfig := &ssh.ClientConfig{
		User: currentUsername,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
		Timeout:         5 * time.Second,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	// find default ssh port
	port, err := net.LookupPort("tcp", "ssh")
	if err != nil {
		return nil, stacktrace.Propagate(err, "Get default ssh port failed.")
	}

	// dial remote node via the ssh port
	dialAddr := fmt.Sprintf("%s:%d", remoteAddress, port)
	conn, err := ssh.Dial("tcp", dialAddr, clientConfig)
	if err != nil {
		return nil, stacktrace.Propagate(err, "Dial failed to %v, error: %v", remoteAddress, err)
	}
	return conn, err
}

func runSession(remoteAddress string, conn *ssh.Client, command string) error {
	// create a session for each worker
	session, err := conn.NewSession()
	if err != nil {
		return stacktrace.Propagate(err, "Cannot create session at %v", remoteAddress)
	}

	session.Stdout = os.Stdout
	session.Stderr = os.Stderr

	// run session
	if err = session.Run(command); err != nil {
		return stacktrace.Propagate(err, "Run command %v failed at %v", command, remoteAddress)
	}

	// close session
	if err = session.Close(); err != nil && err != io.EOF {
		log.Logger.Infof("Session at %s closed. Error: %s", remoteAddress, err)
		return err
	} else {
		log.Logger.Infof("Session at %s closed.", remoteAddress)
	}
	return nil
}
