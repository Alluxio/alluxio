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
	"io"
	"os"
	"strings"

	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"alluxio.org/cli/cmd/conf"
	"alluxio.org/cli/cmd/format"
	"alluxio.org/cli/env"
	"alluxio.org/log"
)

var Master = &MasterProcess{
	BaseProcess: &env.BaseProcess{
		Name:                 "master",
		JavaClassName:        "alluxio.master.AlluxioMaster",
		JavaOptsEnvVarKey:    confAlluxioMasterJavaOpts.EnvVar,
		ProcessOutFile:       "master.out",
		MonitorJavaClassName: "alluxio.master.AlluxioMasterMonitor",
	},
}

const (
	confAlluxioMasterAuditLoggerType       = "alluxio.master.audit.logger.type"
	confAlluxioMasterJournalFolder         = "alluxio.master.journal.folder"
	confAlluxioMasterJournalInitFromBackup = "alluxio.master.journal.init.from.backup"

	envAlluxioAuditMasterLogger = "ALLUXIO_AUDIT_MASTER_LOGGER"
	envAlluxioMasterLogger      = "ALLUXIO_MASTER_LOGGER"
	masterAuditLoggerType       = "MASTER_AUDIT_LOGGER"
	masterLoggerType            = "MASTER_LOGGER"
)

var (
	confAlluxioMasterJavaOpts = env.RegisterTemplateEnvVar(&env.AlluxioConfigEnvVar{
		EnvVar: "ALLUXIO_MASTER_JAVA_OPTS",
	})
	confAlluxioMasterAttachOpts = env.RegisterTemplateEnvVar(&env.AlluxioConfigEnvVar{
		EnvVar: "ALLUXIO_MASTER_ATTACH_OPTS",
	})
)

type MasterProcess struct {
	*env.BaseProcess

	// Format            bool
	JournalBackupFile string
}

func (p *MasterProcess) Base() *env.BaseProcess {
	return p.BaseProcess
}
func (p *MasterProcess) InitCommandTree(processCmd *cobra.Command) {
	masterCmd := &cobra.Command{
		Use:   Master.Name,
		Short: "Manages the Alluxio master process",
	}
	processCmd.AddCommand(masterCmd)

	startCmd := env.NewProcessStartCmd(p)
	// TODO: docs mention this should be located on a URI path of the root mount; what happens in 3.x when there is no root mount
	startCmd.Flags().StringVarP(&p.JournalBackupFile, "journalBackup", "i", "", "A journal backup to restore the master from")
	// startCmd.Flags().BoolVar(&p.Format, "format", false, "Format master")
	masterCmd.AddCommand(startCmd)

	masterCmd.AddCommand(env.NewProcessStopCmd(p))
}

func (p *MasterProcess) SetEnvVars(envVar *viper.Viper) {
	// ALLUXIO_MASTER_JAVA_OPTS = {default logger opts} ${ALLUXIO_JAVA_OPTS} ${ALLUXIO_MASTER_JAVA_OPTS}
	envVar.SetDefault(envAlluxioMasterLogger, masterLoggerType)
	masterJavaOpts := fmt.Sprintf(env.JavaOptFormat, env.ConfAlluxioLoggerType, envVar.Get(envAlluxioMasterLogger))
	envVar.SetDefault(envAlluxioAuditMasterLogger, masterAuditLoggerType)
	masterJavaOpts += fmt.Sprintf(env.JavaOptFormat, confAlluxioMasterAuditLoggerType, envVar.Get(envAlluxioAuditMasterLogger))

	masterJavaOpts += envVar.GetString(env.ConfAlluxioJavaOpts.EnvVar)
	masterJavaOpts += envVar.GetString(p.JavaOptsEnvVarKey)

	envVar.Set(p.JavaOptsEnvVarKey, strings.TrimSpace(masterJavaOpts)) // leading spaces need to be trimmed as a exec.Command argument
}

func (p *MasterProcess) Start() error {
	//if p.Format {
	//	log.Logger.Info("Running format")
	//	// TODO: run format
	//} else {
	if err := p.checkJournal(); err != nil {
		return stacktrace.Propagate(err, "error validating journal")
	}
	//}

	args := []string{env.Env.EnvVar.GetString(env.ConfJava.EnvVar)}
	if attachOpts := env.Env.EnvVar.GetString(confAlluxioMasterAttachOpts.EnvVar); attachOpts != "" {
		args = append(args, strings.Split(attachOpts, " ")...)
	}
	args = append(args, "-cp", env.Env.EnvVar.GetString(env.EnvAlluxioServerClasspath))

	masterJavaOpts := env.Env.EnvVar.GetString(p.JavaOptsEnvVarKey)
	args = append(args, strings.Split(masterJavaOpts, " ")...)

	if p.JournalBackupFile != "" {
		args = append(args, strings.TrimSpace(fmt.Sprintf(env.JavaOptFormat, confAlluxioMasterJournalInitFromBackup, p.JournalBackupFile)))
	}
	// specify a default of -Xmx8g if no memory setting is specified
	const xmxOpt = "-Xmx"
	if !strings.Contains(masterJavaOpts, xmxOpt) && !strings.Contains(masterJavaOpts, "MaxRAMPercentage") {
		args = append(args, fmt.Sprintf("%v8g", xmxOpt))
	}
	// specify a default of -XX:MetaspaceSize=256M if not set
	const metaspaceSizeOpt = "-XX:MetaspaceSize"
	if !strings.Contains(masterJavaOpts, metaspaceSizeOpt) {
		args = append(args, fmt.Sprintf("%v=256M", metaspaceSizeOpt))
	}

	args = append(args, p.JavaClassName)

	if err := p.Launch(args); err != nil {
		return stacktrace.Propagate(err, "error launching process")
	}
	return nil
}

func (p *MasterProcess) checkJournal() error {
	journalDir, err := conf.GetConf.FetchValue(confAlluxioMasterJournalFolder)
	if err != nil {
		return stacktrace.Propagate(err, "error fetching value for %v", confAlluxioMasterJournalFolder)
	}
	stat, err := os.Stat(journalDir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return stacktrace.Propagate(err, "error listing path at %v", journalDir)
	}
	if !stat.IsDir() {
		return stacktrace.NewError("Journal location %v is not a directory. Please remove the file and retry.", journalDir)
	}
	isEmpty, err := DirIsEmpty(journalDir)
	if err != nil {
		return stacktrace.Propagate(err, "error listing contents of %v", journalDir)
	}
	if !isEmpty {
		log.Logger.Info("Running formatJournal")
		if err := format.FormatJournal.Format(); err != nil {
			return stacktrace.Propagate(err, "error formatting journal")
		}
	}
	return nil
}

func DirIsEmpty(dir string) (bool, error) {
	f, err := os.Open(dir)
	if err != nil {
		return false, stacktrace.Propagate(err, "error opening %v", dir)
	}
	defer f.Close()

	if _, err := f.Readdirnames(1); err == io.EOF {
		return true, nil
	}
	return false, stacktrace.Propagate(err, "error listing directory at %v", dir)
}
