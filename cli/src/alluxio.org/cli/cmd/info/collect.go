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

package info

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"

	"alluxio.org/cli/env"
)

var Collect = &CollectCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "collect",
		JavaClassName: "alluxio.cli.bundler.CollectInfo",
	},
}

type CollectCommand struct {
	*env.BaseJavaCommand

	additionalLogs       []string
	endTime              string
	excludeLogs          []string
	excludeWorkerMetrics bool
	includeLogs          []string
	local                bool
	maxThreads           int
	outputPath           string
	startTime            string
}

func (c *CollectCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

const dateFormat = "2006-01-02T15:04:05"

func (c *CollectCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   fmt.Sprintf("%v [command]", c.CommandName),
		Short: "Collects information such as logs, config, metrics, and more from the running Alluxio cluster and bundle into a single tarball",
		Long: `Collects information such as logs, config, metrics, and more from the running Alluxio cluster and bundle into a single tarball
[command] must be one of the following values:
  all      runs all the commands below
  cluster: runs a set of Alluxio commands to collect information about the Alluxio cluster
  conf:    collects the configuration files under ${ALLUXIO_HOME}/config/
  env:     runs a set of linux commands to collect information about the cluster
  jvm:     collects jstack from the JVMs
  log:     collects the log files under ${ALLUXIO_HOME}/logs/
  metrics: collects Alluxio system metrics

WARNING: This command MAY bundle credentials. To understand the risks refer to the docs here.
https://docs.alluxio.io/os/user/edge/en/operation/Troubleshooting.html#collect-alluxio-cluster-information
`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	cmd.Flags().StringSliceVar(&c.additionalLogs, "additional-logs", nil, "Additional file name prefixes from ${ALLUXIO_HOME}/logs to include in the tarball, inclusive of the default log files")
	cmd.Flags().StringVar(&c.endTime, "end-time", "", "Logs that do not contain entries before this time will be ignored, format must be like "+dateFormat)
	cmd.Flags().StringSliceVar(&c.excludeLogs, "exclude-logs", nil, "File name prefixes from ${ALLUXIO_HOME}/logs to exclude; this is evaluated after adding files from --additional-logs")
	cmd.Flags().BoolVar(&c.excludeWorkerMetrics, "exclude-worker-metrics", false, "True to skip worker metrics collection")
	cmd.Flags().StringSliceVar(&c.includeLogs, "include-logs", nil, "File name prefixes from ${ALLUXIO_HOME}/logs to include in the tarball, ignoring the default log files; cannot be used with --exclude-logs or --additional-logs")
	cmd.Flags().BoolVar(&c.local, "local", false, "True to only collect information from the local machine")
	cmd.Flags().IntVar(&c.maxThreads, "max-threads", 1, "Parallelism of the command; use a smaller value to limit network I/O when transferring tarballs")
	const outputPath = "output-path"
	cmd.Flags().StringVar(&c.outputPath, outputPath, "", "Output directory to write collect info tarball to")
	cmd.MarkFlagRequired(outputPath)
	cmd.Flags().StringVar(&c.startTime, "start-time", "", "Logs that do not contain entries after this time will be ignored, format must be like "+dateFormat)
	return cmd
}

func (c *CollectCommand) Run(args []string) error {
	// TODO: use flags instead of arguments to parse user input
	commands := map[string]string{
		"all":     "all",
		"cluster": "collectAlluxioInfo",
		"conf":    "collectConfig",
		"env":     "collectEnv",
		"jvm":     "collectJvmInfo",
		"log":     "collectLog",
		"metrics": "collectMetrics",
	}
	commandArg, ok := commands[args[0]]
	if !ok {
		var cmds []string
		for c := range commands {
			cmds = append(cmds, c)
		}
		return stacktrace.NewError("first argument must be one of %v", strings.Join(cmds, ", "))
	}

	var javaArgs []string
	if c.additionalLogs != nil {
		if c.includeLogs != nil {
			return stacktrace.NewError("cannot set both --include-logs and --additional-logs")
		}
		javaArgs = append(javaArgs, "--additional-logs", strings.Join(c.additionalLogs, ","))
	}
	if c.endTime != "" {
		if _, err := time.Parse(dateFormat, c.endTime); err != nil {
			return stacktrace.Propagate(err, "could not parse end time %v", c.endTime)
		}
		javaArgs = append(javaArgs, "--end-time", c.endTime)
	}
	if c.excludeLogs != nil {
		if c.includeLogs != nil {
			return stacktrace.NewError("cannot set both --include-logs and --exclude-logs")
		}
		javaArgs = append(javaArgs, "--exclude-logs", strings.Join(c.excludeLogs, ","))
	}
	if c.excludeWorkerMetrics {
		javaArgs = append(javaArgs, "--exclude-worker-metrics")
	}
	if c.includeLogs != nil {
		// already checked exclusivity with --additional-logs and --exclude-logs
		javaArgs = append(javaArgs, "--include-logs", strings.Join(c.includeLogs, ","))
	}
	if c.local {
		javaArgs = append(javaArgs, "--local")
	}
	if c.maxThreads > 1 {
		javaArgs = append(javaArgs, "--max-threads", strconv.Itoa(c.maxThreads))
	}
	if c.startTime != "" {
		if _, err := time.Parse(dateFormat, c.startTime); err != nil {
			return stacktrace.Propagate(err, "could not parse start time %v", c.startTime)
		}
		javaArgs = append(javaArgs, "--start-time", c.startTime)
	}

	javaArgs = append(javaArgs, commandArg, c.outputPath)

	return c.Base().Run(javaArgs)
}
