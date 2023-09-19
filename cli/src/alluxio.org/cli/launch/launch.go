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

package launch

import (
	"path/filepath"

	"github.com/palantir/stacktrace"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"alluxio.org/cli/env"
	"alluxio.org/log"
)

type Launcher struct {
	JarEnvVars          map[bool]map[string]string
	AppendClasspathJars map[string]func(*viper.Viper, string) string

	RootPath   string
	DebugLog   bool
	IsDeployed bool
}

func (l *Launcher) AddFlags(cmd *cobra.Command) error {
	const rootPathName = "rootPath"
	cmd.PersistentFlags().StringVar(&l.RootPath, rootPathName, "", "Path to root of Alluxio installation")
	if err := cmd.MarkPersistentFlagRequired(rootPathName); err != nil {
		return stacktrace.Propagate(err, "error marking %v flag required", rootPathName)
	}
	if err := cmd.PersistentFlags().MarkHidden(rootPathName); err != nil {
		return stacktrace.Propagate(err, "error marking %v flag hidden", rootPathName)
	}
	cmd.PersistentFlags().BoolVar(&l.DebugLog, "debug-log", false, "True to enable debug logging")
	const deployedEnv = "deployed-env"
	cmd.PersistentFlags().BoolVar(&l.IsDeployed, deployedEnv, false, "True to set paths to be compatible with a deployed environment")
	cmd.PersistentFlags().MarkHidden(deployedEnv)
	return nil
}

func (l *Launcher) GetPreRunFunc() func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		if l.DebugLog {
			log.Logger.SetLevel(logrus.DebugLevel)
		}
		if err := env.InitAlluxioEnv(filepath.Clean(l.RootPath), l.JarEnvVars[l.IsDeployed], l.AppendClasspathJars); err != nil {
			return stacktrace.Propagate(err, "error defining alluxio environment")
		}
		return nil
	}
}

func (l *Launcher) Run() error {
	rootCmd := &cobra.Command{
		Use: "bin/alluxio",
	}
	rootCmd.CompletionOptions.DisableDefaultCmd = true
	l.AddFlags(rootCmd)
	rootCmd.PersistentPreRunE = l.GetPreRunFunc()
	env.InitServiceCommandTree(rootCmd)

	return rootCmd.Execute()
}
