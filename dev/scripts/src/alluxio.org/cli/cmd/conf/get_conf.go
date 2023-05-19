package conf

import (
	"alluxio.org/log"
	"fmt"
	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"
	"os"
	"os/exec"
	"strings"

	"alluxio.org/cli/env"
)

var GetConf = &GetConfCommand{
	BaseCommand: &env.BaseCommand{
		Name:          "getConf",
		JavaClassName: "alluxio.cli.GetConf",
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
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	}
	cmd.Flags().BoolVar(&c.DebugMode, "attachDebug", false, "True to attach debug opts")
	cmd.Flags().StringVar(&c.JavaOpts, "javaOpts", "", `Java options to apply (ex. "-Dkey=value"`)
	rootCmd.AddCommand(cmd)
}

func (c *GetConfCommand) Run(args []string) error {
	getConfCmd := exec.Command(env.Env.EnvVar.GetString(env.ConfJava.EnvVar), c.prepare(args)...)
	for _, k := range env.Env.EnvVar.AllKeys() {
		getConfCmd.Env = append(getConfCmd.Env, fmt.Sprintf("%s=%v", k, env.Env.EnvVar.Get(k)))
	}
	getConfCmd.Stdout = os.Stdout
	getConfCmd.Stderr = os.Stderr

	log.Logger.Debugln(getConfCmd.String())
	if err := getConfCmd.Run(); err != nil {
		return stacktrace.Propagate(err, "error running getConf")
	}
	return nil
}

func (c *GetConfCommand) FetchValue(key string) (string, error) {
	getConfCmd := exec.Command(env.Env.EnvVar.GetString(env.ConfJava.EnvVar), c.prepare([]string{key})...)
	for _, k := range env.Env.EnvVar.AllKeys() {
		getConfCmd.Env = append(getConfCmd.Env, fmt.Sprintf("%s=%v", k, env.Env.EnvVar.Get(k)))
	}
	log.Logger.Debugln(getConfCmd.String())
	// TODO: stream stderr to byte buffer?
	out, err := getConfCmd.Output()
	if err != nil {
		return "", stacktrace.Propagate(err, "error getting conf for %v", key)
	}
	return string(out), nil
}

func (c *GetConfCommand) prepare(args []string) []string {
	alluxioShellJavaOpts := "-Dalluxio.conf.validation.enabled=false" // TODO: refactor into BaseCommand and use constant for key

	cmdArgs := []string{}
	if c.DebugMode {
		if opts := env.Env.EnvVar.GetString("ALLUXIO_USER_ATTACH_OPTS"); opts != "" { // TODO: use constant for env var key
			cmdArgs = append(cmdArgs, strings.Split(opts, " ")...)
		}
	}
	cmdArgs = append(cmdArgs, "-cp", env.Env.EnvVar.GetString(env.EnvAlluxioClientClasspath))
	if opts := env.Env.EnvVar.GetString(env.EnvAlluxioUserJavaOpts); opts != "" {
		cmdArgs = append(cmdArgs, strings.Split(opts, " ")...)
	}
	if opts := alluxioShellJavaOpts; opts != "" { // TODO: refer to argument value of shell java opts instead of hardcoded
		cmdArgs = append(cmdArgs, strings.Split(opts, " ")...)
	}
	cmdArgs = append(cmdArgs, c.JavaClassName)
	cmdArgs = append(cmdArgs, args...)
	return cmdArgs
}
