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
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"alluxio.org/cli/env"
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
	arguments := strings.Join([]string{env.Service{}.Name, cmd.Name, WorkerProcess{}.Name}, "")
	return runCommand(addStartFlags(arguments, cmd), "worker")
}

func (p *WorkersProcess) Stop(cmd *env.StopProcessCommand) error {
	arguments := strings.Join([]string{env.Service{}.Name, cmd.Name, WorkerProcess{}.Name}, "")
	return runCommand(addStopFlags(arguments, cmd), "worker")
}
