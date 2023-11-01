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
	"os"
	"strings"

	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"alluxio.org/cli/cmd/conf"
	"alluxio.org/cli/cmd/journal"
	"alluxio.org/cli/env"
	"alluxio.org/log"
)

var Master = &MasterProcess{
	BaseProcess: &env.BaseProcess{
		Name:                 "master",
		JavaClassName:        "alluxio.master.AlluxioMaster",
		JavaOpts:             ConfAlluxioMasterJavaOpts,
		ProcessOutFile:       "master.out",
		MonitorJavaClassName: "alluxio.master.AlluxioMasterMonitor",
	},
}

const (
	confAlluxioMasterAuditLoggerType = "alluxio.master.audit.logger.type"
	confAlluxioMasterJournalFolder   = "alluxio.master.journal.folder"

	envAlluxioAuditMasterLogger = "ALLUXIO_AUDIT_MASTER_LOGGER"
	envAlluxioMasterLogger      = "ALLUXIO_MASTER_LOGGER"
	masterAuditLoggerType       = "MASTER_AUDIT_LOGGER"
	masterLoggerType            = "MASTER_LOGGER"
)

var (
	ConfAlluxioMasterJavaOpts = env.RegisterTemplateEnvVar(&env.AlluxioConfigEnvVar{
		EnvVar:     "ALLUXIO_MASTER_JAVA_OPTS",
		IsJavaOpts: true,
	})
	confAlluxioMasterAttachOpts = env.RegisterTemplateEnvVar(&env.AlluxioConfigEnvVar{
		EnvVar:     "ALLUXIO_MASTER_ATTACH_OPTS",
		IsJavaOpts: true,
	})
)

type MasterProcess struct {
	*env.BaseProcess
}

func (p *MasterProcess) Base() *env.BaseProcess {
	return p.BaseProcess
}

func (p *MasterProcess) SetEnvVars(envVar *viper.Viper) {
	envVar.SetDefault(envAlluxioMasterLogger, masterLoggerType)
	envVar.SetDefault(envAlluxioAuditMasterLogger, masterAuditLoggerType)
	// ALLUXIO_MASTER_JAVA_OPTS = {default logger opts} ${ALLUXIO_JAVA_OPTS} ${ALLUXIO_MASTER_JAVA_OPTS}
	javaOpts := []string{
		fmt.Sprintf(env.JavaOptFormat, env.ConfAlluxioLoggerType, envVar.Get(envAlluxioMasterLogger)),
		fmt.Sprintf(env.JavaOptFormat, confAlluxioMasterAuditLoggerType, envVar.Get(envAlluxioAuditMasterLogger)),
	}
	javaOpts = append(javaOpts, env.ConfAlluxioJavaOpts.JavaOptsToArgs(envVar)...)
	javaOpts = append(javaOpts, p.JavaOpts.JavaOptsToArgs(envVar)...)
	envVar.Set(p.JavaOpts.EnvVar, strings.Join(javaOpts, " "))
}

func (p *MasterProcess) StartCmd(cmd *cobra.Command) *cobra.Command {
	cmd.Use = p.Name
	return cmd
}

func (p *MasterProcess) Start(cmd *env.StartProcessCommand) error {
	if err := p.checkJournal(); err != nil {
		return stacktrace.Propagate(err, "error validating journal")
	}

	cmdArgs := []string{env.Env.EnvVar.GetString(env.ConfJava.EnvVar)}
	cmdArgs = append(cmdArgs, confAlluxioMasterAttachOpts.JavaOptsToArgs(env.Env.EnvVar)...)
	cmdArgs = append(cmdArgs, "-cp", env.Env.EnvVar.GetString(env.EnvAlluxioServerClasspath))
	cmdArgs = append(cmdArgs, p.JavaOpts.JavaOptsToArgs(env.Env.EnvVar)...)

	// specify a default of -Xmx8g if no memory setting is specified
	const xmxOpt = "-Xmx"
	if !argsContainsOpt(cmdArgs, xmxOpt, "MaxRAMPercentage") {
		cmdArgs = append(cmdArgs, fmt.Sprintf("%v8g", xmxOpt))
	}
	// specify a default of -XX:MetaspaceSize=256M if not set
	const metaspaceSizeOpt = "-XX:MetaspaceSize"
	if !argsContainsOpt(cmdArgs, metaspaceSizeOpt) {
		cmdArgs = append(cmdArgs, fmt.Sprintf("%v=256M", metaspaceSizeOpt))
	}

	cmdArgs = append(cmdArgs, p.JavaClassName)

	if err := p.Launch(cmd, cmdArgs); err != nil {
		return stacktrace.Propagate(err, "error launching process")
	}
	return nil
}

func (p *MasterProcess) StopCmd(cmd *cobra.Command) *cobra.Command {
	cmd.Use = p.Name
	return cmd
}

func (p *MasterProcess) checkJournal() error {
	journalDir, err := conf.Get.FetchValue(confAlluxioMasterJournalFolder)
	if err != nil {
		return stacktrace.Propagate(err, "error fetching value for %v", confAlluxioMasterJournalFolder)
	}
	stat, err := os.Stat(journalDir)
	if os.IsNotExist(err) {
		log.Logger.Info("Journal directory does not exist, formatting")
		if err := journal.Format.Format(); err != nil {
			return stacktrace.Propagate(err, "error formatting journal")
		}
		return nil
	}
	if err != nil {
		return stacktrace.Propagate(err, "error listing path at %v", journalDir)
	}
	if !stat.IsDir() {
		return stacktrace.NewError("Journal location %v is not a directory. Please remove the file and retry.", journalDir)
	}
	// journal folder path exists and is a directory
	return nil
}
