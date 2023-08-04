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
	"alluxio.org/log"
)

var All = &AllProcess{
	BaseProcess: &env.BaseProcess{
		Name: "all",
	},
}

type AllProcess struct {
	*env.BaseProcess
}

var allProcesses = []env.Process{
	Masters,
	JobMasters,
	Workers,
	JobWorkers,
	Proxies,
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
	for _, subProcess := range allProcesses {
		subProcess := subProcess
		err := subProcess.Start(cmd)
		if err != nil {
			log.Logger.Errorf("Error: %s", err)
		}
	}
	return nil
}

func (p *AllProcess) Stop(cmd *env.StopProcessCommand) error {
	for _, subProcess := range allProcesses {
		subProcess := subProcess
		err := subProcess.Stop(cmd)
		if err != nil {
			log.Logger.Errorf("Error: %s", err)
		}
	}
	return nil
}
