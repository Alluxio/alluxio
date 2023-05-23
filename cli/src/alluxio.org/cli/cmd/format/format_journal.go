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

package format

import (
	"bytes"
	"fmt"

	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"

	"alluxio.org/cli/env"
	"alluxio.org/log"
)

var FormatJournal = &FormatJournalCommand{
	BaseCommand: &env.BaseCommand{
		Name:          "formatJournal",
		JavaClassName: "alluxio.cli.Format",
		Parameter:     "master",
		ShellJavaOpts: fmt.Sprintf(env.JavaOptFormat, env.ConfAlluxioLoggerType, "Console"),
	},
}

type FormatJournalCommand struct {
	*env.BaseCommand
}

func (c *FormatJournalCommand) Base() *env.BaseCommand {
	return c.BaseCommand
}

func (c *FormatJournalCommand) InitCommandTree(rootCmd *cobra.Command) {
	cmd := &cobra.Command{
		Use:   FormatJournal.Name,
		Short: "Format Alluxio master journal locally",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	}
	cmd.Flags().BoolVar(&c.DebugMode, "attachDebug", false, "True to attach debug opts")
	cmd.Flags().StringVar(&c.InlineJavaOpts, "javaOpts", "", `Java options to apply, ex. "-Dkey=value"`)
	rootCmd.AddCommand(cmd)
}

func (c *FormatJournalCommand) Format() error {
	cmd := c.RunJavaClassCmd(nil)
	errBuf := &bytes.Buffer{}
	cmd.Stderr = errBuf

	log.Logger.Debugln(cmd.String())
	if err := cmd.Run(); err != nil {
		return stacktrace.Propagate(err, "error running %v\nstderr: %v", c.Name, errBuf.String())
	}
	return nil
}
