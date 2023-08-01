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
	"fmt"
	"strings"

	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"alluxio.org/cli/env"
)

var Worker = &WorkerProcess{
	BaseProcess: &env.BaseProcess{
		Name:                 "worker",
		JavaClassName:        "alluxio.worker.AlluxioWorker",
		JavaOptsEnvVarKey:    ConfAlluxioWorkerJavaOpts.EnvVar,
		ProcessOutFile:       "worker.out",
		MonitorJavaClassName: "alluxio.worker.AlluxioWorkerMonitor",
	},
}

const (
	envAlluxioWorkerLogger = "ALLUXIO_WORKER_LOGGER"
	workerLoggerType       = "WORKER_LOGGER"
)

var (
	ConfAlluxioWorkerJavaOpts = env.RegisterTemplateEnvVar(&env.AlluxioConfigEnvVar{
		EnvVar: "ALLUXIO_WORKER_JAVA_OPTS",
	})
	confAlluxioWorkerAttachOpts = env.RegisterTemplateEnvVar(&env.AlluxioConfigEnvVar{
		EnvVar: "ALLUXIO_WORKER_ATTACH_OPTS",
	})
)

type WorkerProcess struct {
	*env.BaseProcess
}

func (p *WorkerProcess) Base() *env.BaseProcess {
	return p.BaseProcess
}

func (p *WorkerProcess) SetEnvVars(envVar *viper.Viper) {
	// ALLUXIO_WORKER_JAVA_OPTS = {default logger opts} ${ALLUXIO_JAVA_OPTS} ${ALLUXIO_WORKER_JAVA_OPTS}
	envVar.SetDefault(envAlluxioWorkerLogger, workerLoggerType)
	workerJavaOpts := fmt.Sprintf(env.JavaOptFormat, env.ConfAlluxioLoggerType, envVar.Get(envAlluxioWorkerLogger))

	workerJavaOpts += envVar.GetString(env.ConfAlluxioJavaOpts.EnvVar)
	workerJavaOpts += envVar.GetString(p.JavaOptsEnvVarKey)

	envVar.Set(p.JavaOptsEnvVarKey, strings.TrimSpace(workerJavaOpts)) // leading spaces need to be trimmed as a exec.Command argument
}

func (p *WorkerProcess) StartCmd(cmd *cobra.Command) *cobra.Command {
	cmd.Use = p.Name
	return cmd
}

func (p *WorkerProcess) Start(cmd *env.StartProcessCommand) error {
	cmdArgs := []string{env.Env.EnvVar.GetString(env.ConfJava.EnvVar)}
	if attachOpts := env.Env.EnvVar.GetString(confAlluxioWorkerAttachOpts.EnvVar); attachOpts != "" {
		cmdArgs = append(cmdArgs, strings.Split(attachOpts, " ")...)
	}
	cmdArgs = append(cmdArgs, "-cp", env.Env.EnvVar.GetString(env.EnvAlluxioServerClasspath))

	workerJavaOpts := env.Env.EnvVar.GetString(p.JavaOptsEnvVarKey)
	cmdArgs = append(cmdArgs, strings.Split(workerJavaOpts, " ")...)

	// specify a default of -Xmx4g if no memory setting is specified
	const xmxOpt = "-Xmx"
	if !strings.Contains(workerJavaOpts, xmxOpt) && !strings.Contains(workerJavaOpts, "MaxRAMPercentage") {
		cmdArgs = append(cmdArgs, fmt.Sprintf("%v4g", xmxOpt))
	}
	// specify a default of -XX:MaxDirectMemorySize=4g if not set
	const maxDirectMemorySize = "-XX:MaxDirectMemorySize"
	if !strings.Contains(workerJavaOpts, maxDirectMemorySize) {
		cmdArgs = append(cmdArgs, fmt.Sprintf("%v=4g", maxDirectMemorySize))
	}

	cmdArgs = append(cmdArgs, p.JavaClassName)

	if err := p.Launch(cmd, cmdArgs); err != nil {
		return stacktrace.Propagate(err, "error launching process")
	}
	return nil
}

func (p *WorkerProcess) StopCmd(cmd *cobra.Command) *cobra.Command {
	cmd.Use = p.Name
	return cmd
}
