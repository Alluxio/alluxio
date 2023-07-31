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

// find all workers
// for each worker, create SSH session and run './bin/cli.sh process start worker'
// output pid and logs

var Workers = &WorkersProcess{
	BaseProcess: &env.BaseProcess{
		Name: "workers",
	},
}

// parameters
const ()

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
	// 1. get list of all workers, stored at workersList
	workersDir := path.Join(env.Env.EnvVar.GetString(env.ConfAlluxioConfDir.EnvVar), "workers")
	workers, err := os.Open(workersDir)
	if err != nil {
		log.Logger.Fatalf("Error reading worker hostnames at %s", workersDir)
		return err
	}

	workersReader := bufio.NewReader(workers)
	var workersList []string
	for {
		// read lines of the workers file
		line, err := workersReader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Logger.Fatalf("Error parsing worker file at this line: %s", line)
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

	// 2. get public key for passwordless ssh
	homePath, err := os.UserHomeDir()
	if err != nil {
		log.Logger.Fatalf("User home directory not found at %s", homePath)
		return err
	}
	privateKey, err := os.ReadFile(path.Join(homePath, ".ssh", "id_rsa"))
	if err != nil {
		log.Logger.Fatalf("Private key file not found at %s", path.Join(homePath, ".ssh", "id_rsa"))
		return err
	}
	parsedPrivateKey, err := ssh.ParsePrivateKey(privateKey)
	if err != nil {
		log.Logger.Fatalf("Cannot parse public key at %s", path.Join(homePath, ".ssh", "id_rsa"))
		return err
	}

	// 3. for each worker, create a client
	// TODO: now start worker one by one, need to do them in parallel
	for _, worker := range workersList {
		clientConfig := &ssh.ClientConfig{
			// TODO: how to get user name? Like ${USER} in alluxio-common.sh
			User: "root",
			// TODO: if configure nothing, can use none as the authentication method, suggest use public key
			Auth: []ssh.AuthMethod{
				ssh.PublicKeys(parsedPrivateKey),
			},
			Timeout:         5 * time.Second,
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		}
		// TODO: get ssh port if needed (some machines might have changed default port)
		dialAddr := fmt.Sprintf("%s:%d", worker, 22)
		// TODO: error to resolve: handshake error, authentication failed
		sshClient, err := ssh.Dial("tcp", dialAddr, clientConfig)
		if err != nil {
			log.Logger.Fatalf("Dial failed to %s, error: %s", dialAddr, err)
			return err
		}

		// 4. create a session for each worker
		session, err := sshClient.NewSession()
		if err != nil {
			log.Logger.Fatalf("Cannot create session at %s", dialAddr)
			return err
		}
		defer func(session *ssh.Session) {
			err := session.Close()
			if err != nil {
				log.Logger.Fatalf("Session closed with error: %s", err)
			}
		}(session)
		session.Stdout = os.Stdout
		session.Stderr = os.Stderr

		// 5. run session
		command := "${ALLUXIO_HOME}/bin/cli.sh process start worker"
		err = session.Run(command)
		if err != nil {
			log.Logger.Fatalf("Run command %s failed at %s", command, dialAddr)
			return err
		}
		return nil
	}

	return nil
}

func (p *WorkersProcess) Stop(command *env.StopProcessCommand) error {
	return nil
}
