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
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/palantir/stacktrace"
	"golang.org/x/crypto/ssh"

	"alluxio.org/cli/env"
	"alluxio.org/log"
)

var cliPath = filepath.Join(env.Env.EnvVar.GetString(env.ConfAlluxioHome.EnvVar), "bin", "alluxio")

func getNodes(isMasters bool) ([]string, error) {
	var FilePath string
	if isMasters {
		FilePath = filepath.Join(env.Env.EnvVar.GetString(env.ConfAlluxioConfDir.EnvVar), "masters")
	} else {
		FilePath = filepath.Join(env.Env.EnvVar.GetString(env.ConfAlluxioConfDir.EnvVar), "workers")
	}
	File, err := ioutil.ReadFile(FilePath)
	if err != nil {
		return nil, stacktrace.Propagate(err, "Error reading hostnames at %v", FilePath)
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
	return nodesList, nil
}

func getPrivateKey() (ssh.Signer, error) {
	homePath, err := os.UserHomeDir()
	if err != nil {
		return nil, stacktrace.Propagate(err, "User home directory not found at %v", homePath)
	}
	privateKeyFile := filepath.Join(homePath, ".ssh", "id_rsa")
	privateKey, err := os.ReadFile(privateKeyFile)
	if err != nil {
		return nil, stacktrace.Propagate(err, "Private key file not found at %v", privateKeyFile)
	}
	parsedPrivateKey, err := ssh.ParsePrivateKey(privateKey)
	if err != nil {
		return nil, stacktrace.Propagate(err, "Cannot parse public key at %v", privateKeyFile)
	}
	return parsedPrivateKey, nil
}

func dialConnection(remoteAddress string, signer ssh.Signer) (*ssh.Client, error) {
	clientConfig := &ssh.ClientConfig{
		// TODO: how to get user name? Like ${USER} in alluxio-common.sh
		User: "root",
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
		Timeout:         5 * time.Second,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
	// TODO: Some machines might have changed default SSH port. Get ssh port or remind users when get started.
	dialAddr := fmt.Sprintf("%s:%d", remoteAddress, 22)
	conn, err := ssh.Dial("tcp", dialAddr, clientConfig)
	if err != nil {
		return nil, stacktrace.Propagate(err, "Dial failed to %v, error: %v", remoteAddress, err)
	}
	return conn, err
}

func closeConnection(remoteAddress string, conn *ssh.Client) error {
	if err := conn.Close(); err != nil {
		log.Logger.Infof("Connection to %s closed. Error: %s", remoteAddress, err)
		return err
	} else {
		log.Logger.Infof("Connection to %s closed.", remoteAddress)
		return nil
	}
}

func runCommand(remoteAddress string, conn *ssh.Client, command string) error {
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
