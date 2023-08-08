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
	arguments := strings.Join([]string{env.Service{}.Name, cmd.Name, MasterProcess{}.Name}, " ")
	return runCommand(addStartFlags(arguments, cmd), "master")
}

func (p *MastersProcess) Stop(cmd *env.StopProcessCommand) error {
	arguments := strings.Join([]string{env.Service{}.Name, cmd.Name, MasterProcess{}.Name}, " ")
	return runCommand(addStopFlags(arguments, cmd), "master")
}
