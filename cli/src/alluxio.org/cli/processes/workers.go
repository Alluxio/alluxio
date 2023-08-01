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

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/crypto/ssh"

	"alluxio.org/cli/env"
	"alluxio.org/log"
)

var Workers = &WorkersProcess{
	BaseProcess: &env.BaseProcess{
		Name: "workers",
	},
}

type WorkersProcess struct {
	*env.BaseProcess
}

func (p *WorkersProcess) SetEnvVars(envVar *viper.Viper) {
	return
}

func (p *WorkersProcess) Base() *env.BaseProcess {
	return p.BaseProcess
}

func (p *WorkersProcess) StartCmd(cmd *cobra.Command) *cobra.Command {
	cmd.Use = p.Name
	return cmd
}

func (p *WorkersProcess) StopCmd(cmd *cobra.Command) *cobra.Command {
	cmd.Use = p.Name
	return cmd
}

func (p *WorkersProcess) Start(cmd *env.StartProcessCommand) error {
	// get list of all workers, stored at workersList
	workers, err := getWorkers()
	if err != nil {
		log.Logger.Fatalf("Cannot get workers, error: %s", err)
	}

	// get public key for passwordless ssh
	key, err := getPrivateKey()
	if err != nil {
		log.Logger.Fatalf("Cannot get private key, error: %s", err)
	}

	// for each worker, create a client
	// TODO: now start worker one by one, need to do them in parallel
	cliPath := path.Join(env.Env.EnvVar.GetString(env.ConfAlluxioHome.EnvVar), "bin", "cli.sh")
	arguments := "process start worker"
	if cmd.AsyncStart {
		arguments = arguments + " -a"
	}
	if cmd.SkipKillOnStart {
		arguments = arguments + " -N"
	}
	command := cliPath + " " + arguments

	errors := runCommandOnWorkers(workers, key, command)

	if len(errors) == 0 {
		log.Logger.Infof("Run command %s successful on workers: %s", command, workers)
	} else {
		log.Logger.Fatalf("Run command %s failed: %s", command, err)
	}
	return nil
}

func (p *WorkersProcess) Stop(cmd *env.StopProcessCommand) error {
	// get list of all workers, stored at workersList
	workers, err := getWorkers()
	if err != nil {
		log.Logger.Fatalf("Cannot get workers, error: %s", err)
	}

	// get public key for passwordless ssh
	key, err := getPrivateKey()
	if err != nil {
		log.Logger.Fatalf("Cannot get private key, error: %s", err)
	}

	// for each worker, create a client
	// TODO: now start worker one by one, need to do them in parallel
	cliPath := path.Join(env.Env.EnvVar.GetString(env.ConfAlluxioHome.EnvVar), "bin", "cli.sh")
	arguments := "process stop worker"
	command := cliPath + " " + arguments

	errors := runCommandOnWorkers(workers, key, command)

	if len(errors) == 0 {
		log.Logger.Infof("Run command %s on workers: %s", command, workers)
	} else {
		log.Logger.Fatalf("Run command %s failed: %s", command, err)
	}
	return nil
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

func runCommandOnWorkers(workers []string, key ssh.Signer, command string) []error {
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
