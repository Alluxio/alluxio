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
	"bufio"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"alluxio.org/cli/env"
	"alluxio.org/log"
	"golang.org/x/crypto/ssh"
)

func getMasters() ([]string, error) {
	mastersDir := path.Join(env.Env.EnvVar.GetString(env.ConfAlluxioConfDir.EnvVar), "masters")
	mastersFile, err := os.Open(mastersDir)
	if err != nil {
		log.Logger.Errorf("Error reading worker hostnames at %s", mastersDir)
		return nil, err
	}

	mastersReader := bufio.NewReader(mastersFile)
	var mastersList []string
	lastLine := false
	for !lastLine {
		// read lines of the workers file
		line, err := mastersReader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				lastLine = true
			} else {
				log.Logger.Errorf("Error parsing worker file at this line: %s", line)
				return nil, err
			}
		}
		// remove notes
		if strings.Index(line, "#") != -1 {
			line = line[:strings.Index(line, "#")]
		}
		line = strings.TrimSpace(line)
		if line != "" {
			mastersList = append(mastersList, line)
		}
	}
	return mastersList, nil
}

func getWorkers() ([]string, error) {
	workersDir := path.Join(env.Env.EnvVar.GetString(env.ConfAlluxioConfDir.EnvVar), "workers")
	workersFile, err := os.Open(workersDir)
	if err != nil {
		log.Logger.Errorf("Error reading worker hostnames at %s", workersDir)
		return nil, err
	}

	workersReader := bufio.NewReader(workersFile)
	var workersList []string
	lastLine := false
	for !lastLine {
		// read lines of the workers file
		line, err := workersReader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				lastLine = true
			} else {
				log.Logger.Errorf("Error parsing worker file at this line: %s", line)
				return nil, err
			}
		}
		// remove notes
		if strings.Index(line, "#") != -1 {
			line = line[:strings.Index(line, "#")]
		}
		line = strings.TrimSpace(line)
		if line != "" {
			workersList = append(workersList, line)
		}
	}
	return workersList, nil
}

func getPrivateKey() (ssh.Signer, error) {
	homePath, err := os.UserHomeDir()
	if err != nil {
		log.Logger.Errorf("User home directory not found at %s", homePath)
		return nil, err
	}
	privateKey, err := os.ReadFile(path.Join(homePath, ".ssh", "id_rsa"))
	if err != nil {
		log.Logger.Errorf("Private key file not found at %s", path.Join(homePath, ".ssh", "id_rsa"))
		return nil, err
	}
	parsedPrivateKey, err := ssh.ParsePrivateKey(privateKey)
	if err != nil {
		log.Logger.Errorf("Cannot parse public key at %s", path.Join(homePath, ".ssh", "id_rsa"))
		return nil, err
	}
	return parsedPrivateKey, nil
}

func runCommands(workers []string, key ssh.Signer, command string) []error {
	var errors []error
	for _, worker := range workers {
		clientConfig := &ssh.ClientConfig{
			// TODO: how to get user name? Like ${USER} in alluxio-common.sh
			User: "root",
			Auth: []ssh.AuthMethod{
				ssh.PublicKeys(key),
			},
			Timeout:         5 * time.Second,
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}
		// TODO: Some machines might have changed default SSH port. Get ssh port or remind users when get started.
		dialAddr := fmt.Sprintf("%s:%d", worker, 22)
		conn, err := ssh.Dial("tcp", dialAddr, clientConfig)
		defer func(conn *ssh.Client) {
			err := conn.Close()
			if err != nil {
				log.Logger.Infof("Connection to %s closed. Error: %s", dialAddr, err)
			} else {
				log.Logger.Infof("Connection to %s closed.", dialAddr)
			}
		}(conn)

		if err != nil {
			log.Logger.Errorf("Dial failed to %s, error: %s", dialAddr, err)
			errors = append(errors, err)
		}

		// create a session for each worker
		session, err := conn.NewSession()
		if err != nil {
			log.Logger.Errorf("Cannot create session at %s", dialAddr)
			errors = append(errors, err)
		}
		defer func(session *ssh.Session) {
			err := session.Close()
			if err != nil && err != io.EOF {
				log.Logger.Infof("Session at %s closed. Error: %s", dialAddr, err)
			} else {
				log.Logger.Infof("Session at %s closed.", dialAddr)
			}
		}(session)

		session.Stdout = os.Stdout
		session.Stderr = os.Stderr

		// run session
		err = session.Run(command)
		if err != nil {
			log.Logger.Errorf("Run command %s failed at %s", command, dialAddr)
			errors = append(errors, err)
		}
	}
	return errors
}
