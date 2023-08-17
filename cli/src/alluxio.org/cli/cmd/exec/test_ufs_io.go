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

package exec

import (
	"strconv"

	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"

	"alluxio.org/cli/env"
	"alluxio.org/log"
)

var TestUfsIO = &TestUfsIOCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "ufsIOTest",
		JavaClassName: "alluxio.stress.cli.UfsIOBench",
	},
}

type TestUfsIOCommand struct {
	*env.BaseJavaCommand
	path         string
	ioSize       string
	threads      int
	cluster      bool
	clusterLimit int
	javaOpt      []string
}

func (c *TestUfsIOCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *TestUfsIOCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use:   c.CommandName,
		Short: "A benchmarking tool for the I/O between Alluxio and UFS.",
		Long: "A benchmarking tool for the I/O between Alluxio and UFS." +
			"This test will measure the I/O throughput between Alluxio workers and the specified UFS path. " +
			"Each worker will create concurrent clients to first generate test files of the specified size " +
			"then read those files. The write/read I/O throughput will be measured in the process.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	cmd.Flags().StringVar(&c.path, "path", "",
		"specifies the path to write/read temporary data in.")
	cmd.Flags().StringVar(&c.ioSize, "io-size", "4G",
		"specifies the amount of data each thread writes/reads.")
	cmd.Flags().IntVar(&c.threads, "threads", 4,
		"specifies the number of threads to concurrently use on each worker.")
	cmd.Flags().BoolVar(&c.cluster, "cluster", false,
		"specifies the benchmark is run in the Alluxio cluster.\n"+
			"If not specified, this benchmark will run locally.")
	cmd.Flags().IntVar(&c.clusterLimit, "cluster-limit", 0,
		"specifies how many Alluxio workers to run the benchmark concurrently.\n"+
			"If >0, it will only run on that number of workers.\n"+
			"If 0, it will run on all available cluster workers.\n"+
			"If <0, will run on the workers from the end of the worker list.\n"+
			"This flag is only used if --cluster is enabled.")
	cmd.Flags().StringSliceVar(&c.javaOpt, "java-opt", nil,
		"The java options to add to the command line to for the task.\n"+
			"This can be repeated. The options must be quoted and prefixed with a space.\n"+
			"For example: --java-opt \" -Xmx4g\" --java-opt \" -Xms2g\".")
	err := cmd.MarkFlagRequired("path")
	if err != nil {
		log.Logger.Errorln("Required flag --path not specified.")
	}
	return cmd
}

func (c *TestUfsIOCommand) Run(args []string) error {
	if c.threads <= 0 {
		return stacktrace.NewError("Flag --threads should be a positive number.")
	}

	var javaArgs []string
	if c.path != "" {
		javaArgs = append(javaArgs, "--path", c.path)
	}
	if c.ioSize != "" {
		javaArgs = append(javaArgs, "--io-size", c.ioSize)
	}
	javaArgs = append(javaArgs, "--threads", strconv.Itoa(c.threads))
	if c.cluster != false {
		javaArgs = append(javaArgs, "--cluster", "--cluster-limit", strconv.Itoa(c.clusterLimit))
	}
	for _, option := range c.javaOpt {
		javaArgs = append(javaArgs, "--java-opt", "\""+option+"\"")
	}
	javaArgs = append(javaArgs, args...)

	return c.Base().Run(javaArgs)
}
