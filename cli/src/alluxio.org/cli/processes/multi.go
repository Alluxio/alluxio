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

	"alluxio.org/cli/cmd/process"
	"alluxio.org/cli/env"
)

var (
	JobMasters = &MultiProcess{
		BaseProcess: &env.BaseProcess{
			Name: "job_masters",
		},
		HostnameFiles:     []string{HostGroupMasters},
		SingleProcessName: JobMaster.Name,
	}
	JobWorkers = &MultiProcess{
		BaseProcess: &env.BaseProcess{
			Name: "job_workers",
		},
		HostnameFiles:     []string{HostGroupWorkers},
		SingleProcessName: JobWorker.Name,
	}
	Masters = &MultiProcess{
		BaseProcess: &env.BaseProcess{
			Name: "masters",
		},
		HostnameFiles:     []string{HostGroupMasters},
		SingleProcessName: Master.Name,
	}
	Proxies = &MultiProcess{
		BaseProcess: &env.BaseProcess{
			Name: "proxies",
		},
		HostnameFiles:     []string{HostGroupMasters, HostGroupWorkers},
		SingleProcessName: Proxy.Name,
	}
	Workers = &MultiProcess{
		BaseProcess: &env.BaseProcess{
			Name: "workers",
		},
		HostnameFiles:     []string{HostGroupWorkers},
		SingleProcessName: Worker.Name,
	}
)

type MultiProcess struct {
	*env.BaseProcess
	HostnameFiles     []string
	SingleProcessName string
}

func (p *MultiProcess) SetEnvVars(_ *viper.Viper) {
	return
}

func (p *MultiProcess) Base() *env.BaseProcess {
	return p.BaseProcess
}

func (p *MultiProcess) StartCmd(cmd *cobra.Command) *cobra.Command {
	cmd.Use = p.Name
	return cmd
}

func (p *MultiProcess) StopCmd(cmd *cobra.Command) *cobra.Command {
	cmd.Use = p.Name
	return cmd
}

func (p *MultiProcess) Start(cmd *env.StartProcessCommand) error {
	return RunSshCommand(
		addStartFlags(cmd,
			process.Service.Name,
			env.StartProcessName,
			p.SingleProcessName,
		), p.HostnameFiles...)
}

func (p *MultiProcess) Stop(cmd *env.StopProcessCommand) error {
	return RunSshCommand(
		addStopFlags(cmd,
			process.Service.Name,
			env.StopProcessName,
			p.SingleProcessName,
		), p.HostnameFiles...)
}
