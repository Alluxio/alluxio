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

package env

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"

	"alluxio.org/log"
)

var commandRegistry = map[string]Command{}

func RegisterCommand(p Command) Command {
	name := p.Base().Name
	if _, ok := commandRegistry[name]; ok {
		panic(fmt.Sprintf("Command %v is already registered", name))
	}
	commandRegistry[name] = p
	return p
}

func InitCommandTree(rootCmd *cobra.Command) {
	for _, p := range commandRegistry {
		p.InitCommandTree(rootCmd)
	}
}

type Command interface {
	Base() *BaseCommand
	InitCommandTree(*cobra.Command)
	Run([]string) error
}

type BaseCommand struct {
	Name          string
	JavaClassName string
	Parameter     string

	DebugMode bool
	QuietMode bool

	InlineJavaOpts string // java opts provided by the user as part of the inline command
	ShellJavaOpts  string // default java opts encoded as part of the specific command
}

// RunJavaClassCmd constructs a java command with a predetermined order of variable opts
// ${JAVA} ${ALLUXIO_USER_ATTACH_OPTS} -cp ${ALLUXIO_CLIENT_CLASSPATH} ${ALLUXIO_USER_JAVA_OPTS} \
//   {command default opts} {user inline opts} {command java class} {command parameter} {user inline args}
// where:
// - ${ALLUXIO_USER_*} are environment variables set by the user in alluxio-env.sh
// - {command *} are encoded as part of the command's definition
// - {user inline *} are specified by the user when entering the command
func (c *BaseCommand) RunJavaClassCmd(args []string) *exec.Cmd {
	var cmdArgs []string
	if c.DebugMode {
		if opts := Env.EnvVar.GetString(ConfAlluxioUserAttachOpts.EnvVar); opts != "" {
			cmdArgs = append(cmdArgs, strings.Split(opts, " ")...)
		}
	}
	cmdArgs = append(cmdArgs, "-cp", Env.EnvVar.GetString(EnvAlluxioClientClasspath))
	if opts := Env.EnvVar.GetString(ConfAlluxioUserJavaOpts.EnvVar); opts != "" {
		cmdArgs = append(cmdArgs, strings.Split(opts, " ")...)
	}
	if opts := strings.TrimSpace(c.ShellJavaOpts); opts != "" {
		cmdArgs = append(cmdArgs, strings.Split(opts, " ")...)
	}
	if opts := strings.TrimSpace(c.InlineJavaOpts); opts != "" {
		cmdArgs = append(cmdArgs, strings.Split(opts, " ")...)
	}
	cmdArgs = append(cmdArgs, c.JavaClassName)
	if c.Parameter != "" {
		cmdArgs = append(cmdArgs, c.Parameter)
	}
	cmdArgs = append(cmdArgs, args...)

	ret := exec.Command(Env.EnvVar.GetString(ConfJava.EnvVar), cmdArgs...)
	for _, k := range Env.EnvVar.AllKeys() {
		ret.Env = append(ret.Env, fmt.Sprintf("%s=%v", k, Env.EnvVar.Get(k)))
	}
	return ret
}

func (c *BaseCommand) Run(args []string) error {
	cmd := c.RunJavaClassCmd(args)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	log.Logger.Debugln(cmd.String())
	if err := cmd.Run(); err != nil {
		return stacktrace.Propagate(err, "error running %v", c.Name)
	}
	return nil
}
