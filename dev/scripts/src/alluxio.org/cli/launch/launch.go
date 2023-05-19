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

	env.InitProcessCommandTree(rootCmd)
	env.InitCommandTree(rootCmd)

	rootCmd.SilenceUsage = true
	return rootCmd.Execute()
}
