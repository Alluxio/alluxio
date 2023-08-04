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

var JobMasters = &JobMastersProcess{
	BaseProcess: &env.BaseProcess{
		Name: "job_masters",
	},
}

type JobMastersProcess struct {
	*env.BaseProcess
}

func (p *JobMastersProcess) SetEnvVars(envVar *viper.Viper) {
	return
}

func (p *JobMastersProcess) Base() *env.BaseProcess {
	return p.BaseProcess
}

func (p *JobMastersProcess) StartCmd(cmd *cobra.Command) *cobra.Command {
	cmd.Use = p.Name
	return cmd
}

func (p *JobMastersProcess) StopCmd(cmd *cobra.Command) *cobra.Command {
	cmd.Use = p.Name
	return cmd
}

func (p *JobMastersProcess) Start(cmd *env.StartProcessCommand) error {
	// get list of all masters, stored at mastersList
	masters, err := getMasters()
	if err != nil {
		log.Logger.Fatalf("Cannot get masters, error: %s", err)
	}

	// get public key for passwordless ssh
	key, err := getPrivateKey()
	if err != nil {
		log.Logger.Fatalf("Cannot get private key, error: %s", err)
	}

	// generate command
	cliPath := path.Join(env.Env.EnvVar.GetString(env.ConfAlluxioHome.EnvVar), "bin", "alluxio")
	arguments := "process start job_master"
	if cmd.AsyncStart {
		arguments = arguments + " -a"
	}
	if cmd.SkipKillOnStart {
		arguments = arguments + " -N"
	}
	command := cliPath + " " + arguments

	// for each master, create a client and run
	// TODO: now start master one by one, need to do them in parallel
	var errors []error
	for _, master := range masters {
		conn, err := dialConnection(master, key)
		if err != nil {
			errors = append(errors, err)
			continue
		}
		err = runCommand(master, conn, command)
		if err != nil {
			errors = append(errors, err)
		}
		err = closeConnection(master, conn)
		if err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) == 0 {
		log.Logger.Infof("Run command %s successful on masters: %s", command, masters)
	} else {
		log.Logger.Fatalf("Run command %s failed, number of failures: %v", command, len(errors))
	}
	return nil
}

func (p *JobMastersProcess) Stop(cmd *env.StopProcessCommand) error {
	// get list of all masters, stored at mastersList
	masters, err := getMasters()
	if err != nil {
		log.Logger.Fatalf("Cannot get masters, error: %s", err)
	}

	// get public key for passwordless ssh
	key, err := getPrivateKey()
	if err != nil {
		log.Logger.Fatalf("Cannot get private key, error: %s", err)
	}

	// generate command
	cliPath := path.Join(env.Env.EnvVar.GetString(env.ConfAlluxioHome.EnvVar), "bin", "alluxio")
	arguments := "process stop job_master"
	if cmd.SoftKill {
		arguments = arguments + " -s"
	}
	command := cliPath + " " + arguments

	// for each master, create a client and run
	// TODO: now stop master one by one, need to do them in parallel
	var errors []error
	for _, master := range masters {
		conn, err := dialConnection(master, key)
		if err != nil {
			errors = append(errors, err)
			continue
		}
		err = runCommand(master, conn, command)
		if err != nil {
			errors = append(errors, err)
		}
		err = closeConnection(master, conn)
		if err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) == 0 {
		log.Logger.Infof("Run command %s successful on masters: %s", command, masters)
	} else {
		log.Logger.Fatalf("Run command %s failed, number of failures: %v", command, len(errors))
	}
	return nil
}
