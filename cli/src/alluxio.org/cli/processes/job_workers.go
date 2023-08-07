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

var JobWorkers = &JobWorkersProcess{
	BaseProcess: &env.BaseProcess{
		Name: "job_workers",
	},
}

type JobWorkersProcess struct {
	*env.BaseProcess
}

func (p *JobWorkersProcess) SetEnvVars(envVar *viper.Viper) {
	return
}

func (p *JobWorkersProcess) Base() *env.BaseProcess {
	return p.BaseProcess
}

func (p *JobWorkersProcess) StartCmd(cmd *cobra.Command) *cobra.Command {
	cmd.Use = p.Name
	return cmd
}

func (p *JobWorkersProcess) StopCmd(cmd *cobra.Command) *cobra.Command {
	cmd.Use = p.Name
	return cmd
}

func (p *JobWorkersProcess) Start(cmd *env.StartProcessCommand) error {
	arguments := "process start job_worker"
	return runCommand(addStartFlags(arguments, cmd), false, true)
}

func (p *JobWorkersProcess) Stop(cmd *env.StopProcessCommand) error {
	arguments := "process stop job_worker"
	return runCommand(addStopFlags(arguments, cmd), false, true)
}
