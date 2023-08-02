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

var Masters = &MastersProcess{
	BaseProcess: &env.BaseProcess{
		Name: "masters",
	},
}

type MastersProcess struct {
	*env.BaseProcess
}

func (p *MastersProcess) SetEnvVars(envVar *viper.Viper) {
	return
}

func (p *MastersProcess) Base() *env.BaseProcess {
	return p.BaseProcess
}

func (p *MastersProcess) StartCmd(cmd *cobra.Command) *cobra.Command {
	cmd.Use = p.Name
	return cmd
}

func (p *MastersProcess) StopCmd(cmd *cobra.Command) *cobra.Command {
	cmd.Use = p.Name
	return cmd
}

func (p *MastersProcess) Start(cmd *env.StartProcessCommand) error {
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
	cliPath := path.Join(env.Env.EnvVar.GetString(env.ConfAlluxioHome.EnvVar), "bin", "cli.sh")
	arguments := "process start master"
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
		err = runCommand(conn, command)
		if err != nil {
			errors = append(errors, err)
		}
		err = closeConnection(conn)
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

func (p *MastersProcess) Stop(cmd *env.StopProcessCommand) error {
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
	cliPath := path.Join(env.Env.EnvVar.GetString(env.ConfAlluxioHome.EnvVar), "bin", "cli.sh")
	arguments := "process stop master"
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
		err = runCommand(conn, command)
		if err != nil {
			errors = append(errors, err)
		}
		err = closeConnection(conn)
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
