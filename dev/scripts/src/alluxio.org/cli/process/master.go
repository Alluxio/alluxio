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
	"os/exec"
	"path/filepath"
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
		Name:              "master",
		JavaClassName:     "alluxio.master.AlluxioMaster",
		JavaOptsEnvVarKey: confAlluxioMasterJavaOpts.EnvVar,
		ProcessOutFile:    "master.out",
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

	startCmd := &cobra.Command{
		Use:   "start",
		Short: "Start the process",
		RunE: func(cmd *cobra.Command, args []string) error {
			return p.Start()
		},
	}
	// startCmd.Flags().BoolVar(&p.Format, "format", false, "Format master")
	masterCmd.AddCommand(startCmd)
}

func (p *MasterProcess) SetEnvVars(envVar *viper.Viper) {
	// ALLUXIO_MASTER_JAVA_OPTS = {default logger opts} ${ALLUXIO_JAVA_OPTS} {user provided opts} {default opts if missing}
	loggerType := masterLoggerType
	if p.EnableConsoleLogging {
		loggerType = "Console," + loggerType
	}
	envVar.SetDefault(envAlluxioMasterLogger, loggerType)
	masterJavaOpts := fmt.Sprintf(env.JavaOptFormat, env.ConfAlluxioLoggerType, envVar.Get(envAlluxioMasterLogger))
	envVar.SetDefault(envAlluxioAuditMasterLogger, masterAuditLoggerType)
	masterJavaOpts += fmt.Sprintf(env.JavaOptFormat, confAlluxioMasterAuditLoggerType, envVar.Get(envAlluxioAuditMasterLogger))

	masterJavaOpts += envVar.GetString(env.ConfAlluxioJavaOpts.EnvVar)
	masterJavaOpts += envVar.GetString(p.JavaOptsEnvVarKey)

	if p.JournalBackupFile != "" {
		masterJavaOpts += fmt.Sprintf(env.JavaOptFormat, confAlluxioMasterJournalInitFromBackup, p.JournalBackupFile)
	}
	// specify a default of -Xmx8g if no memory setting is specified
	const xmxOpt = "-Xmx"
	if !strings.Contains(masterJavaOpts, xmxOpt) && !strings.Contains(masterJavaOpts, "MaxRAMPercentage") {
		masterJavaOpts += fmt.Sprintf(" %v8g", xmxOpt)
	}
	// specify a default of -XX:MetaspaceSize=256M if not set
	const metaspaceSizeOpt = "-XX:MetaspaceSize"
	if !strings.Contains(masterJavaOpts, metaspaceSizeOpt) {
		masterJavaOpts += fmt.Sprintf(" %v=256M", metaspaceSizeOpt)
	}
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
	args = append(args, strings.Split(env.Env.EnvVar.GetString(p.JavaOptsEnvVarKey), " ")...)
	args = append(args, p.JavaClassName)

	startCmd := exec.Command("nohup", args...)
	for _, k := range env.Env.EnvVar.AllKeys() {
		startCmd.Env = append(startCmd.Env, fmt.Sprintf("%s=%v", k, env.Env.EnvVar.Get(k)))
	}

	outFile := filepath.Join(env.Env.EnvVar.GetString(env.ConfAlluxioLogsDir.EnvVar), p.ProcessOutFile)
	f, err := os.OpenFile(outFile, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return stacktrace.Propagate(err, "error opening file at %v", outFile)
	}
	startCmd.Stdout = f
	startCmd.Stderr = f

	log.Logger.Info("Running master")
	log.Logger.Debugf("%v > %v 2>&1 &", startCmd.String(), outFile)
	if err := startCmd.Start(); err != nil {
		return stacktrace.Propagate(err, "error starting master")
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
