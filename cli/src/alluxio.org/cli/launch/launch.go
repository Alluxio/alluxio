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

func Run(jarEnvVars map[bool]map[string]string, appendClasspathJars map[string]func(*viper.Viper, string) string) error {
	rootCmd := &cobra.Command{
		Use: "bin/alluxio",
	}
	const rootPathName = "rootPath"
	var flagRootPath string
	rootCmd.PersistentFlags().StringVar(&flagRootPath, rootPathName, "", "Path to root of Alluxio installation")
	if err := rootCmd.MarkPersistentFlagRequired(rootPathName); err != nil {
		return stacktrace.Propagate(err, "error marking %v flag required", rootPathName)
	}
	if err := rootCmd.PersistentFlags().MarkHidden(rootPathName); err != nil {
		return stacktrace.Propagate(err, "error marking %v flag hidden", rootPathName)
	}
	var flagDebugLog bool
	rootCmd.PersistentFlags().BoolVar(&flagDebugLog, "debug-log", false, "True to enable debug logging")
	var flagIsDeployed bool
	rootCmd.PersistentFlags().BoolVar(&flagIsDeployed, "deployed-env", false, "True to set paths to be compatible with a deployed environment")

	rootCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		if flagDebugLog {
			log.Logger.SetLevel(logrus.DebugLevel)
		}
		if err := env.InitAlluxioEnv(filepath.Clean(flagRootPath), jarEnvVars[flagIsDeployed], appendClasspathJars); err != nil {
			return stacktrace.Propagate(err, "error defining alluxio environment")
		}
		return nil
	}

	env.InitServiceCommandTree(rootCmd)

	return rootCmd.Execute()
}
