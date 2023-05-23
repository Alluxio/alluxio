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

var JobMaster = &JobMasterProcess{
	BaseProcess: &env.BaseProcess{
		Name:                 "job_master",
		JavaClassName:        "alluxio.master.AlluxioJobMaster",
		JavaOptsEnvVarKey:    confAlluxioJobMasterJavaOpts.EnvVar,
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
	confAlluxioJobMasterJavaOpts = env.RegisterTemplateEnvVar(&env.AlluxioConfigEnvVar{
		EnvVar: "ALLUXIO_JOB_MASTER_JAVA_OPTS",
	})
	confAlluxioJobMasterAttachOpts = env.RegisterTemplateEnvVar(&env.AlluxioConfigEnvVar{
		EnvVar: "ALLUXIO_JOB_MASTER_ATTACH_OPTS",
	})
)

type JobMasterProcess struct {
	*env.BaseProcess
}

func (p *JobMasterProcess) Base() *env.BaseProcess {
	return p.BaseProcess
}
func (p *JobMasterProcess) InitCommandTree(processCmd *cobra.Command) {
	jobMasterCmd := &cobra.Command{
		Use:   JobMaster.Name,
		Short: "Manages the Alluxio job master process",
	}
	processCmd.AddCommand(jobMasterCmd)

	startCmd := env.NewProcessStartCmd(p)
	jobMasterCmd.AddCommand(startCmd)

	jobMasterCmd.AddCommand(env.NewProcessStopCmd(p))
}

func (p *JobMasterProcess) SetEnvVars(envVar *viper.Viper) {
	// ALLUXIO_JOB_MASTER_JAVA_OPTS = {default logger opts} ${ALLUXIO_JAVA_OPTS} ${ALLUXIO_JOB_MASTER_JAVA_OPTS}
	envVar.SetDefault(envAlluxioJobMasterLogger, jobMasterLoggerType)
	jobMasterJavaOpts := fmt.Sprintf(env.JavaOptFormat, env.ConfAlluxioLoggerType, envVar.Get(envAlluxioJobMasterLogger))
	envVar.SetDefault(envAlluxioAuditJobMasterLogger, jobMasterAuditLoggerType)
	jobMasterJavaOpts += fmt.Sprintf(env.JavaOptFormat, confAlluxioJobMasterAuditLoggerType, envVar.Get(envAlluxioAuditJobMasterLogger))

	jobMasterJavaOpts += envVar.GetString(env.ConfAlluxioJavaOpts.EnvVar)
	jobMasterJavaOpts += envVar.GetString(p.JavaOptsEnvVarKey)

	envVar.Set(p.JavaOptsEnvVarKey, strings.TrimSpace(jobMasterJavaOpts)) // leading spaces need to be trimmed as a exec.Command argument
}

func (p *JobMasterProcess) Start() error {
	cmdArgs := []string{env.Env.EnvVar.GetString(env.ConfJava.EnvVar)}
	if attachOpts := env.Env.EnvVar.GetString(confAlluxioJobMasterAttachOpts.EnvVar); attachOpts != "" {
		cmdArgs = append(cmdArgs, strings.Split(attachOpts, " ")...)
	}
	cmdArgs = append(cmdArgs, "-cp", env.Env.EnvVar.GetString(env.EnvAlluxioServerClasspath))

	jobMasterJavaOpts := env.Env.EnvVar.GetString(p.JavaOptsEnvVarKey)
	cmdArgs = append(cmdArgs, strings.Split(jobMasterJavaOpts, " ")...)

	cmdArgs = append(cmdArgs, p.JavaClassName)

	if err := p.Launch(cmdArgs); err != nil {
		return stacktrace.Propagate(err, "error launching process")
	}
	return nil
}
