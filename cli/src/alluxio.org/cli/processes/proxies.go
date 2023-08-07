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

var Proxies = &ProxiesProcess{
	BaseProcess: &env.BaseProcess{
		Name: "proxies",
	},
}

type ProxiesProcess struct {
	*env.BaseProcess
}

func (p *ProxiesProcess) SetEnvVars(envVar *viper.Viper) {
	return
}

func (p *ProxiesProcess) Base() *env.BaseProcess {
	return p.BaseProcess
}

func (p *ProxiesProcess) StartCmd(cmd *cobra.Command) *cobra.Command {
	cmd.Use = p.Name
	return cmd
}

func (p *ProxiesProcess) StopCmd(cmd *cobra.Command) *cobra.Command {
	cmd.Use = p.Name
	return cmd
}

func (p *ProxiesProcess) Start(cmd *env.StartProcessCommand) error {
	// get list of all masters and workers, stored at allList
	masters, err := getMasters()
	if err != nil {
		log.Logger.Fatalf("Cannot get masters, error: %s", err)
	}
	workers, err := getWorkers()
	if err != nil {
		log.Logger.Fatalf("Cannot get workers, error: %s", err)
	}
	allNodes := append(masters, workers...)

	// get public key for passwordless ssh
	key, err := getPrivateKey()
	if err != nil {
		log.Logger.Fatalf("Cannot get private key, error: %s", err)
	}

	// generate command
	cliPath := path.Join(env.Env.EnvVar.GetString(env.ConfAlluxioHome.EnvVar), "bin", "alluxio")
	arguments := "process start proxy"
	if cmd.AsyncStart {
		arguments = arguments + " -a"
	}
	if cmd.SkipKillOnStart {
		arguments = arguments + " -N"
	}
	command := cliPath + " " + arguments

	// for each node, create a client
	// TODO: now start nodes one by one, need to do them in parallel
	var errors []error
	for _, node := range allNodes {
		conn, err := dialConnection(node, key)
		if err != nil {
			errors = append(errors, err)
			continue
		}
		if err = runCommand(node, conn, command); err != nil {
			errors = append(errors, err)
		}
		if err = closeConnection(node, conn); err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) == 0 {
		log.Logger.Infof("Run command %s successful on nodes: %s", command, allNodes)
	} else {
		log.Logger.Fatalf("Run command %s failed, number of failures: %v", command, len(errors))
	}
	return nil
}

func (p *ProxiesProcess) Stop(cmd *env.StopProcessCommand) error {
	// get list of all masters and workers, stored at allList
	masters, err := getMasters()
	if err != nil {
		log.Logger.Fatalf("Cannot get masters, error: %s", err)
	}
	workers, err := getWorkers()
	if err != nil {
		log.Logger.Fatalf("Cannot get workers, error: %s", err)
	}
	allNodes := append(masters, workers...)

	// get public key for passwordless ssh
	key, err := getPrivateKey()
	if err != nil {
		log.Logger.Fatalf("Cannot get private key, error: %s", err)
	}

	// generate command
	cliPath := path.Join(env.Env.EnvVar.GetString(env.ConfAlluxioHome.EnvVar), "bin", "alluxio")
	arguments := "process stop proxy"
	if cmd.SoftKill {
		arguments = arguments + " -s"
	}
	command := cliPath + " " + arguments

	// for each node, create a client
	// TODO: now stop nodes one by one, need to do them in parallel
	var errors []error
	for _, node := range allNodes {
		conn, err := dialConnection(node, key)
		if err != nil {
			errors = append(errors, err)
			continue
		}
		if err = runCommand(node, conn, command); err != nil {
			errors = append(errors, err)
		}
		if err = closeConnection(node, conn); err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) != 0 {
		log.Logger.Fatalf("Run command %v failed. Failed commands: %v", command, len(errors))
	}
	log.Logger.Infof("Run command %s successful on nodes: %s", command, allNodes)
	return nil
}
