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
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"alluxio.org/cli/env"
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
	arguments := "process start job_master"
	return runCommand(addStartFlags(arguments, cmd), true, false)
}

func (p *JobMastersProcess) Stop(cmd *env.StopProcessCommand) error {
	arguments := "process stop job_master"
	return runCommand(addStopFlags(arguments, cmd), true, false)
}
