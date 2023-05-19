package conf

import (
	"bytes"
	"fmt"

	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"

	"alluxio.org/cli/env"
	"alluxio.org/log"
)

var GetConf = &GetConfCommand{
	BaseCommand: &env.BaseCommand{
		Name:          "getConf",
		JavaClassName: "alluxio.cli.GetConf",
		ShellJavaOpts: fmt.Sprintf(env.JavaOptFormat, env.ConfAlluxioConfValidationEnabled, false),
	},
}

type GetConfCommand struct {
	*env.BaseCommand
}

func (c *GetConfCommand) Base() *env.BaseCommand {
	return c.BaseCommand
}

func (c *GetConfCommand) InitCommandTree(rootCmd *cobra.Command) {
	cmd := &cobra.Command{
		Use:   GetConf.Name,
		Short: "Look up a configuration key, or print all configuration.",
		Long: `
GetConf prints the configured value for the given key. If the key
is invalid, the exit code will be nonzero. If the key is valid
but isn't set, an empty string is printed. If no key is
specified, all configuration is printed.`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	}
	cmd.Flags().BoolVar(&c.DebugMode, "attachDebug", false, "True to attach debug opts")
	cmd.Flags().StringVar(&c.InlineJavaOpts, "javaOpts", "", `Java options to apply, ex. "-Dkey=value"`)
	// TODO: add optional args for --master, --source, --unit
	rootCmd.AddCommand(cmd)
}

func (c *GetConfCommand) FetchValue(key string) (string, error) {
	cmd := c.RunJavaClassCmd([]string{key})

	errBuf := &bytes.Buffer{}
	cmd.Stderr = errBuf

	log.Logger.Debugln(cmd.String())
	out, err := cmd.Output()
	if err != nil {
		return "", stacktrace.Propagate(err, "error getting conf for %v\nstderr: %v", key, errBuf.String())
	}
	return string(out), nil
}
