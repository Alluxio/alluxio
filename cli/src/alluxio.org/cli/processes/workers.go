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

	"alluxio.org/cli/env"
	"alluxio.org/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
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

	errors := runCommands(workers, key, command)

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

	errors := runCommands(workers, key, command)

	if len(errors) == 0 {
		log.Logger.Infof("Run command %s on workers: %s", command, workers)
	} else {
		log.Logger.Fatalf("Run command %s failed: %s", command, err)
	}
	return nil
}
