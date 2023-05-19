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
