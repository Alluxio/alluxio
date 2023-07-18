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
	"alluxio.org/cli/env"
	"alluxio.org/log"
	"bytes"
	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"
)

var TestUfs = &TestUfsCommand{
	BaseJavaCommand: &env.BaseJavaCommand{
		CommandName:   "testUfs",
		JavaClassName: "alluxio.cli.UnderFileSystemContractTest",
	},
}

type TestUfsCommand struct {
	*env.BaseJavaCommand
	path string
}

func (c *TestUfsCommand) Base() *env.BaseJavaCommand {
	return c.BaseJavaCommand
}

func (c *TestUfsCommand) ToCommand() *cobra.Command {
	cmd := c.Base().InitRunJavaClassCmd(&cobra.Command{
		Use: "testUfs --path <ufs_path>",
		Short: "Test the integration between Alluxio and the given UFS.\n" +
			"UFS tests validate the semantics Alluxio expects of the UFS.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.Run(args)
		},
	})
	cmd.Flags().StringVar(&c.path, "path", "",
		"[required] the full UFS path to run tests against.")
	return cmd
}

func (c *TestUfsCommand) Run(args []string) error {
	var javaArgs []string
	if c.path != "" {
		javaArgs = append(javaArgs, "--path", c.path)
	}
	javaArgs = append(javaArgs, args...)

	return c.Base().Run(javaArgs)
}

func (c *TestUfsCommand) FetchValue(key string) (string, error) {
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
