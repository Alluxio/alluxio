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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"alluxio.org/log"
)

type Command interface {
	ToCommand() *cobra.Command
}

type BaseJavaCommand struct {
	CommandName   string
	JavaClassName string
	Parameters    []string

	DebugMode bool

	UseServerClasspath bool     // defaults to ALLUXIO_CLIENT_CLASSPATH, use ALLUXIO_SERVER_CLASSPATH if true
	InlineJavaOpts     []string // java opts provided by the user as part of the inline command
	ShellJavaOpts      string   // default java opts encoded as part of the specific command
}

func (c *BaseJavaCommand) InitRunJavaClassCmd(cmd *cobra.Command) *cobra.Command {
	cmd.Flags().BoolVar(&c.DebugMode, "attach-debug", false, fmt.Sprintf("True to attach debug opts specified by $%v", ConfAlluxioUserAttachOpts.EnvVar))
	cmd.Flags().StringSliceVarP(&c.InlineJavaOpts, "java-opts", "D", nil, `Alluxio properties to apply, ex. -Dkey=value`)
	return cmd
}

// RunJavaClassCmd constructs a java command with a predetermined order of variable opts
// ${JAVA} ${ALLUXIO_USER_ATTACH_OPTS} -cp ${ALLUXIO_CLIENT_CLASSPATH} ${ALLUXIO_USER_JAVA_OPTS} \
//   {command default opts} {user inline opts} {command java class} {command parameter} {user inline args}
// where:
// - ${ALLUXIO_USER_*} are environment variables set by the user in alluxio-env.sh
// - {command *} are encoded as part of the command's definition
// - {user inline *} are specified by the user when entering the command
func (c *BaseJavaCommand) RunJavaClassCmd(args []string) *exec.Cmd {
	var cmdArgs []string
	if c.DebugMode {
		if opts := Env.EnvVar.GetString(ConfAlluxioUserAttachOpts.EnvVar); opts != "" {
			cmdArgs = append(cmdArgs, strings.Split(opts, " ")...)
		}
	}
	classpath := EnvAlluxioClientClasspath
	if c.UseServerClasspath {
		classpath = EnvAlluxioServerClasspath
	}
	cmdArgs = append(cmdArgs, "-cp", Env.EnvVar.GetString(classpath))

	if opts := Env.EnvVar.GetString(ConfAlluxioUserJavaOpts.EnvVar); opts != "" {
		cmdArgs = append(cmdArgs, strings.Split(opts, " ")...)
	}
	if opts := strings.TrimSpace(c.ShellJavaOpts); opts != "" {
		cmdArgs = append(cmdArgs, strings.Split(opts, " ")...)
	}
	for _, o := range c.InlineJavaOpts {
		if opts := strings.TrimSpace(o); opts != "" {
			cmdArgs = append(cmdArgs, fmt.Sprintf("-D%v", opts))
		}
	}
	cmdArgs = append(cmdArgs, c.JavaClassName)
	if len(c.Parameters) > 0 {
		cmdArgs = append(cmdArgs, c.Parameters...)
	}
	cmdArgs = append(cmdArgs, args...)

	ret := exec.Command(Env.EnvVar.GetString(ConfJava.EnvVar), cmdArgs...)
	for _, k := range Env.EnvVar.AllKeys() {
		ret.Env = append(ret.Env, fmt.Sprintf("%s=%v", k, Env.EnvVar.Get(k)))
	}
	return ret
}

func (c *BaseJavaCommand) Run(args []string) error {
	return c.RunWithIO(args, nil, os.Stdout, os.Stderr)
}

func (c *BaseJavaCommand) RunWithIO(args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	cmd := c.RunJavaClassCmd(args)

	cmd.Stdin = stdin
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	log.Logger.Debugln(cmd.String())
	if err := cmd.Run(); err != nil {
		return stacktrace.Propagate(err, "error running %v", c.CommandName)
	}
	return nil
}

func (c *BaseJavaCommand) RunAndFormat(format string, stdin io.Reader, args []string) error {
	switch strings.ToLower(format) {
	case "json":
		return c.RunWithIO(args, stdin, os.Stdout, os.Stderr)
	case "yaml":
		buf := &bytes.Buffer{}
		if err := c.RunWithIO(args, stdin, buf, os.Stderr); err != nil {
			io.Copy(os.Stdout, buf)
			return err
		}
		var obj interface{}
		if err := json.Unmarshal(buf.Bytes(), &obj); err != nil {
			return stacktrace.Propagate(err, "error unmarshalling json from java command")
		}
		if out, err := yaml.Marshal(obj); err == nil {
			os.Stdout.Write(out)
		} else {
			return stacktrace.Propagate(err, "error marshalling yaml")
		}
	default:
		return stacktrace.NewError("unfamiliar output format %s", format)
	}
	return nil
}
