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

var JobWorker = &WorkerProcess{
	BaseProcess: &env.BaseProcess{
		Name:                 "job_worker",
		JavaClassName:        "alluxio.worker.AlluxioJobWorker",
		JavaOpts:             ConfAlluxioJobWorkerJavaOpts,
		ProcessOutFile:       "job_worker.out",
		MonitorJavaClassName: "alluxio.worker.job.AlluxioJobWorkerMonitor",
	},
}

const (
	envAlluxioJobWorkerLogger = "ALLUXIO_JOB_WORKER_LOGGER"
	jobWorkerLoggerType       = "JOB_WORKER_LOGGER"
)

var (
	ConfAlluxioJobWorkerJavaOpts = env.RegisterTemplateEnvVar(&env.AlluxioConfigEnvVar{
		EnvVar:     "ALLUXIO_JOB_WORKER_JAVA_OPTS",
		IsJavaOpts: true,
	})
	confAlluxioJobWorkerAttachOpts = env.RegisterTemplateEnvVar(&env.AlluxioConfigEnvVar{
		EnvVar:     "ALLUXIO_JOB_WORKER_ATTACH_OPTS",
		IsJavaOpts: true,
	})
)

type JobWorkerProcess struct {
	*env.BaseProcess
}

func (p *JobWorkerProcess) Base() *env.BaseProcess {
	return p.BaseProcess
}

func (p *JobWorkerProcess) SetEnvVars(envVar *viper.Viper) {
	envVar.SetDefault(envAlluxioJobWorkerLogger, jobWorkerLoggerType)
	// ALLUXIO_JOB_WORKER_JAVA_OPTS = {default logger opts} ${ALLUXIO_JAVA_OPTS} ${ALLUXIO_WORKER_JAVA_OPTS}
	javaOpts := []string{
		fmt.Sprintf(env.JavaOptFormat, env.ConfAlluxioLoggerType, envVar.Get(envAlluxioJobWorkerLogger)),
	}
	javaOpts = append(javaOpts, env.ConfAlluxioJavaOpts.JavaOptsToArgs(envVar)...)
	javaOpts = append(javaOpts, p.JavaOpts.JavaOptsToArgs(envVar)...)
	envVar.Set(p.JavaOpts.EnvVar, strings.Join(javaOpts, " "))
}

func (p *JobWorkerProcess) StartCmd(cmd *cobra.Command) *cobra.Command {
	cmd.Use = p.Name
	return cmd
}

func (p *JobWorkerProcess) Start(cmd *env.StartProcessCommand) error {
	cmdArgs := []string{env.Env.EnvVar.GetString(env.ConfJava.EnvVar)}
	cmdArgs = append(cmdArgs, confAlluxioJobWorkerAttachOpts.JavaOptsToArgs(env.Env.EnvVar)...)
	cmdArgs = append(cmdArgs, "-cp", env.Env.EnvVar.GetString(env.EnvAlluxioServerClasspath))
	cmdArgs = append(cmdArgs, p.JavaOpts.JavaOptsToArgs(env.Env.EnvVar)...)
	cmdArgs = append(cmdArgs, p.JavaClassName)

	if err := p.Launch(cmd, cmdArgs); err != nil {
		return stacktrace.Propagate(err, "error launching process")
	}
	return nil
}

func (p *JobWorkerProcess) StopCmd(cmd *cobra.Command) *cobra.Command {
	cmd.Use = p.Name
	return cmd
}
