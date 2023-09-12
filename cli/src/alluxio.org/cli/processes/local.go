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
	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"alluxio.org/cli/env"
)

var Local = &LocalProcess{
	BaseProcess: &env.BaseProcess{
		Name: "local",
	},
	Processes: []env.Process{
		Master,
		Worker,
	},
}

type LocalProcess struct {
	*env.BaseProcess
	Processes []env.Process
}

func (p *LocalProcess) SetEnvVars(envVar *viper.Viper) {
	return
}

func (p *LocalProcess) Base() *env.BaseProcess {
	return p.BaseProcess
}

func (p *LocalProcess) StartCmd(cmd *cobra.Command) *cobra.Command {
	cmd.Use = p.Name
	return cmd
}

func (p *LocalProcess) StopCmd(cmd *cobra.Command) *cobra.Command {
	cmd.Use = p.Name
	return cmd
}

func (p *LocalProcess) Start(cmd *env.StartProcessCommand) error {
	for i := 0; i < len(p.Processes); i++ {
		subProcess := p.Processes[i]
		if err := subProcess.Start(cmd); err != nil {
			return stacktrace.Propagate(err, "Error starting subprocesses for %s", p.Processes[i])
		}
	}
	return nil
}

func (p *LocalProcess) Stop(cmd *env.StopProcessCommand) error {
	for i := len(p.Processes) - 1; i >= 0; i-- {
		subProcess := p.Processes[i]
		if err := subProcess.Stop(cmd); err != nil {
			return stacktrace.Propagate(err, "Error stopping subprocesses for %s", p.Processes[i])
		}
	}
	return nil
}
