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

package process

import (
	"fmt"
	"strings"

	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"alluxio.org/cli/env"
)

var JobWorker = &WorkerProcess{
	BaseProcess: &env.BaseProcess{
		Name:                 "job_worker",
		JavaClassName:        "alluxio.worker.AlluxioJobWorker",
		JavaOptsEnvVarKey:    confAlluxioJobWorkerJavaOpts.EnvVar,
		ProcessOutFile:       "job_worker.out",
		MonitorJavaClassName: "alluxio.worker.job.AlluxioJobWorkerMonitor",
	},
}

const (
	envAlluxioJobWorkerLogger = "ALLUXIO_JOB_WORKER_LOGGER"
	jobWorkerLoggerType       = "JOB_WORKER_LOGGER"
)

var (
	confAlluxioJobWorkerJavaOpts = env.RegisterTemplateEnvVar(&env.AlluxioConfigEnvVar{
		EnvVar: "ALLUXIO_JOB_WORKER_JAVA_OPTS",
	})
	confAlluxioJobWorkerAttachOpts = env.RegisterTemplateEnvVar(&env.AlluxioConfigEnvVar{
		EnvVar: "ALLUXIO_JOB_WORKER_ATTACH_OPTS",
	})
)

type JobWorkerProcess struct {
	*env.BaseProcess
}

func (p *JobWorkerProcess) Base() *env.BaseProcess {
	return p.BaseProcess
}
func (p *JobWorkerProcess) InitCommandTree(processCmd *cobra.Command) {
	jobWorkerCmd := &cobra.Command{
		Use:   Worker.Name,
		Short: "Manages the Alluxio job worker process",
	}
	processCmd.AddCommand(jobWorkerCmd)

	startCmd := env.NewProcessStartCmd(p)
	jobWorkerCmd.AddCommand(startCmd)

	jobWorkerCmd.AddCommand(env.NewProcessStopCmd(p))
}

func (p *JobWorkerProcess) SetEnvVars(envVar *viper.Viper) {
	// ALLUXIO_JOB_WORKER_JAVA_OPTS = {default logger opts} ${ALLUXIO_JAVA_OPTS} ${ALLUXIO_WORKER_JAVA_OPTS}
	envVar.SetDefault(envAlluxioJobWorkerLogger, jobWorkerLoggerType)
	jobWorkerJavaOpts := fmt.Sprintf(env.JavaOptFormat, env.ConfAlluxioLoggerType, envVar.Get(envAlluxioJobWorkerLogger))

	jobWorkerJavaOpts += envVar.GetString(env.ConfAlluxioJavaOpts.EnvVar)
	jobWorkerJavaOpts += envVar.GetString(p.JavaOptsEnvVarKey)

	envVar.Set(p.JavaOptsEnvVarKey, strings.TrimSpace(jobWorkerJavaOpts)) // leading spaces need to be trimmed as a exec.Command argument
}

func (p *JobWorkerProcess) Start() error {
	cmdArgs := []string{env.Env.EnvVar.GetString(env.ConfJava.EnvVar)}
	if attachOpts := env.Env.EnvVar.GetString(confAlluxioJobWorkerAttachOpts.EnvVar); attachOpts != "" {
		cmdArgs = append(cmdArgs, strings.Split(attachOpts, " ")...)
	}
	cmdArgs = append(cmdArgs, "-cp", env.Env.EnvVar.GetString(env.EnvAlluxioServerClasspath))

	jobWorkerJavaOpts := env.Env.EnvVar.GetString(p.JavaOptsEnvVarKey)
	cmdArgs = append(cmdArgs, strings.Split(jobWorkerJavaOpts, " ")...)

	cmdArgs = append(cmdArgs, p.JavaClassName)

	if err := p.Launch(cmdArgs); err != nil {
		return stacktrace.Propagate(err, "error launching process")
	}
	return nil
}
