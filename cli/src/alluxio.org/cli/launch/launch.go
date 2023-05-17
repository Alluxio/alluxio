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

	"alluxio.org/cli/env"
	"alluxio.org/log"
)

func Run() error {
	rootCmd := &cobra.Command{}
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
	rootCmd.PersistentFlags().BoolVar(&flagDebugLog, "debugLog", false, "True to enable debug logging")
	rootCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		if flagDebugLog {
			log.Logger.SetLevel(logrus.DebugLevel)
		}
		if err := env.InitAlluxioEnv(filepath.Clean(flagRootPath)); err != nil {
			return stacktrace.Propagate(err, "error defining alluxio environment")
		}
		return nil
	}

	env.InitServiceCommandTree(rootCmd)

	rootCmd.SilenceUsage = true
	return rootCmd.Execute()
}
