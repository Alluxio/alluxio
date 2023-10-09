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

var JobMaster = &JobMasterProcess{
	BaseProcess: &env.BaseProcess{
		Name:                 "job_master",
		JavaClassName:        "alluxio.master.AlluxioJobMaster",
		JavaOpts:             ConfAlluxioJobMasterJavaOpts,
		ProcessOutFile:       "job_master.out",
		MonitorJavaClassName: "alluxio.master.job.AlluxioJobMasterMonitor",
	},
}

const (
	confAlluxioJobMasterAuditLoggerType = "alluxio.job.master.audit.logger.type"
	envAlluxioAuditJobMasterLogger      = "ALLUXIO_AUDIT_JOB_MASTER_LOGGER"
	envAlluxioJobMasterLogger           = "ALLUXIO_JOB_MASTER_LOGGER"
	jobMasterAuditLoggerType            = "JOB_MASTER_AUDIT_LOGGER"
	jobMasterLoggerType                 = "JOB_MASTER_LOGGER"
)

var (
	ConfAlluxioJobMasterJavaOpts = env.RegisterTemplateEnvVar(&env.AlluxioConfigEnvVar{
		EnvVar:     "ALLUXIO_JOB_MASTER_JAVA_OPTS",
		IsJavaOpts: true,
	})
	confAlluxioJobMasterAttachOpts = env.RegisterTemplateEnvVar(&env.AlluxioConfigEnvVar{
		EnvVar:     "ALLUXIO_JOB_MASTER_ATTACH_OPTS",
		IsJavaOpts: true,
	})
)

type JobMasterProcess struct {
	*env.BaseProcess
}

func (p *JobMasterProcess) Base() *env.BaseProcess {
	return p.BaseProcess
}

func (p *JobMasterProcess) SetEnvVars(envVar *viper.Viper) {
	envVar.SetDefault(envAlluxioJobMasterLogger, jobMasterLoggerType)
	envVar.SetDefault(envAlluxioAuditJobMasterLogger, jobMasterAuditLoggerType)
	// ALLUXIO_JOB_MASTER_JAVA_OPTS = {default logger opts} ${ALLUXIO_JAVA_OPTS} ${ALLUXIO_JOB_MASTER_JAVA_OPTS}
	javaOpts := []string{
		fmt.Sprintf(env.JavaOptFormat, env.ConfAlluxioLoggerType, envVar.Get(envAlluxioJobMasterLogger)),
		fmt.Sprintf(env.JavaOptFormat, confAlluxioJobMasterAuditLoggerType, envVar.Get(envAlluxioAuditJobMasterLogger)),
	}
	javaOpts = append(javaOpts, env.ConfAlluxioJavaOpts.JavaOptsToArgs(envVar)...)
	javaOpts = append(javaOpts, p.JavaOpts.JavaOptsToArgs(envVar)...)
	envVar.Set(p.JavaOpts.EnvVar, strings.Join(javaOpts, " "))
}

func (p *JobMasterProcess) StartCmd(cmd *cobra.Command) *cobra.Command {
	cmd.Use = p.Name
	return cmd
}

func (p *JobMasterProcess) Start(cmd *env.StartProcessCommand) error {
	cmdArgs := []string{env.Env.EnvVar.GetString(env.ConfJava.EnvVar)}
	cmdArgs = append(cmdArgs, confAlluxioJobMasterAttachOpts.JavaOptsToArgs(env.Env.EnvVar)...)
	cmdArgs = append(cmdArgs, "-cp", env.Env.EnvVar.GetString(env.EnvAlluxioServerClasspath))
	cmdArgs = append(cmdArgs, p.JavaOpts.JavaOptsToArgs(env.Env.EnvVar)...)
	cmdArgs = append(cmdArgs, p.JavaClassName)

	if err := p.Launch(cmd, cmdArgs); err != nil {
		return stacktrace.Propagate(err, "error launching process")
	}
	return nil
}

func (p *JobMasterProcess) StopCmd(cmd *cobra.Command) *cobra.Command {
	cmd.Use = p.Name
	return cmd
}
