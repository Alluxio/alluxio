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
	"path"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

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

	// generate command
	cliPath := path.Join(env.Env.EnvVar.GetString(env.ConfAlluxioHome.EnvVar), "bin", "alluxio")
	arguments := "process start worker"
	if cmd.AsyncStart {
		arguments = arguments + " -a"
	}
	if cmd.SkipKillOnStart {
		arguments = arguments + " -N"
	}
	command := cliPath + " " + arguments

	// for each worker, create a client and run
	// TODO: now start worker one by one, need to do them in parallel
	var errors []error
	for _, worker := range workers {
		conn, err := dialConnection(worker, key)
		if err != nil {
			errors = append(errors, err)
			continue
		}
		if err = runCommand(worker, conn, command); err != nil {
			errors = append(errors, err)
		}
		if err = closeConnection(worker, conn); err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) == 0 {
		log.Logger.Infof("Run command %s successful on workers: %s", command, workers)
	} else {
		log.Logger.Fatalf("Run command %s failed, number of failures: %v", command, len(errors))
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

	// generate command
	cliPath := path.Join(env.Env.EnvVar.GetString(env.ConfAlluxioHome.EnvVar), "bin", "alluxio")
	arguments := "process stop worker"
	if cmd.SoftKill {
		arguments = arguments + " -s"
	}
	command := cliPath + " " + arguments

	// for each worker, create a client and run
	// TODO: now stop worker one by one, need to do them in parallel
	var errors []error
	for _, worker := range workers {
		conn, err := dialConnection(worker, key)
		if err != nil {
			errors = append(errors, err)
			continue
		}
		if err = runCommand(worker, conn, command); err != nil {
			errors = append(errors, err)
		}
		if err = closeConnection(worker, conn); err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) != 0 {
		log.Logger.Fatalf("Run command %v failed. Failed commands: %v", command, len(errors))
	}
	log.Logger.Infof("Run command %s successful on workers: %s", command, workers)
	return nil
}
