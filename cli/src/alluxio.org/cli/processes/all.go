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
	"os/exec"
	"path"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"alluxio.org/cli/env"
)

var All = &AllProcess{
	BaseProcess: &env.BaseProcess{
		Name: "all",
	},
}

type AllProcess struct {
	*env.BaseProcess
}

func (p *AllProcess) SetEnvVars(envVar *viper.Viper) {
	return
}

func (p *AllProcess) Base() *env.BaseProcess {
	return p.BaseProcess
}

func (p *AllProcess) StartCmd(cmd *cobra.Command) *cobra.Command {
	cmd.Use = p.Name
	return cmd
}

func (p *AllProcess) StopCmd(cmd *cobra.Command) *cobra.Command {
	cmd.Use = p.Name
	return cmd
}

func (p *AllProcess) Start(cmd *env.StartProcessCommand) error {
	// generate commands
	cliPath := path.Join(env.Env.EnvVar.GetString(env.ConfAlluxioHome.EnvVar), "bin", "cli.sh")
	components := []string{"masters", "job_masters", "workers", "job_workers", "proxies"}
	var commands []string
	for _, component := range components {
		arguments := "process start" + " " + component
		if cmd.AsyncStart {
			arguments = arguments + " -a"
		}
		if cmd.SkipKillOnStart {
			arguments = arguments + " -N"
		}
		commands = append(commands, cliPath+" "+arguments)
	}

	for _, command := range commands {
		exec.Command(command)
	}

	return nil
}

func (p *AllProcess) Stop(cmd *env.StopProcessCommand) error {
	// generate commands
	cliPath := path.Join(env.Env.EnvVar.GetString(env.ConfAlluxioHome.EnvVar), "bin", "cli.sh")
	components := []string{"masters", "job_masters", "workers", "job_workers", "proxies"}
	var commands []string
	for _, component := range components {
		arguments := "process stop" + " " + component
		commands = append(commands, cliPath+" "+arguments)
	}

	for _, command := range commands {
		exec.Command(command)
	}

	return nil
}
