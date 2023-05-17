package cmd

import (
	"alluxio.org/cli/env"
	"path/filepath"

	"github.com/palantir/stacktrace"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"alluxio.org/log"
)

func Run() error {
	rootCmd := &cobra.Command{}
	var flagRootPath string
	rootCmd.PersistentFlags().StringVar(&flagRootPath, "rootPath", "", "Path to root of Alluxio installation")
	if err := rootCmd.MarkPersistentFlagRequired("rootPath"); err != nil {
		return stacktrace.Propagate(err, "error marking rootPath flag required")
	}
	var flagDebugLog bool
	rootCmd.PersistentFlags().BoolVar(&flagDebugLog, "debug", false, "True to enable debug logging")
	rootCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		if flagDebugLog {
			log.Logger.SetLevel(logrus.DebugLevel)
		}
		if err := env.InitAlluxioEnv(filepath.Clean(flagRootPath)); err != nil {
			return stacktrace.Propagate(err, "error defining alluxio environment")
		}
		return nil
	}

	env.InitProcessCommands(rootCmd)
	// TODO: init CLI commands

	rootCmd.SilenceUsage = true
	return rootCmd.Execute()
}
