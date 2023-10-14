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

package generate

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/palantir/stacktrace"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"alluxio.org/cli/env"
)

var UserCliDoc = &UserCliCommand{
	Dst: filepath.Join("docs", "en", "operation", "User-CLI.md"),
}

type UserCliCommand struct {
	Dst string
}

func (c *UserCliCommand) ToCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "user-cli",
		Short: "Generate content for `operation/User-CLI.md`",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			var rootCmd *cobra.Command
			for rootCmd = cmd; rootCmd.HasParent(); rootCmd = rootCmd.Parent() {
			}

			f, err := os.Create(c.Dst)
			if err != nil {
				return stacktrace.Propagate(err, "error creating output file")
			}
			defer f.Close()
			w := bufio.NewWriter(f)
			fmt.Fprintln(w,
				`---
layout: global
title: User Command Line Interface
---

{% comment %}
This is a generated file created by running command "bin/alluxio generate user-cli"
The command parses the golang command definitions and descriptions to generate the markdown in this file
{% endcomment %}

Alluxio's command line interface provides user access to various operations, such as:
- Start or stop processes
- Filesystem operations
- Administrative commands`)
			fmt.Fprintln(w)
			fmt.Fprintln(w, "Invoke the executable to view the possible subcommands:")
			fmt.Fprintln(w, "```shell")
			fmt.Fprintln(w, "$ ./bin/alluxio")
			fmt.Fprintln(w, rootCmd.UsageString())
			fmt.Fprintln(w, "```")
			fmt.Fprintln(w)
			fmt.Fprintln(w, "To set JVM system properties as part of the command, set the `-D` flag in the form of `-Dproperty=value`.")
			fmt.Fprintln(w)
			fmt.Fprintln(w, "To attach debugging java options specified by `$ALLUXIO_USER_ATTACH_OPTS`, set the `--attach-debug` flag")
			fmt.Fprintln(w)
			fmt.Fprintln(w, "Note that, as a part of Alluxio deployment, the Alluxio shell will also take the configuration in `${ALLUXIO_HOME}/conf/alluxio-site.properties` when it is run from Alluxio installation at `${ALLUXIO_HOME}`.")
			fmt.Fprintln(w)

			for _, serviceCmd := range rootCmd.Commands() {
				if serviceCmd.Name() == "help" {
					// help is a built in command from the library. avoid documenting it
					continue
				}
				fmt.Fprint(w, "## ")
				fmt.Fprintln(w, serviceCmd.Name())

				desc := serviceCmd.Short
				if serviceCmd.Long != "" {
					desc = serviceCmd.Long
				}
				fmt.Fprintln(w, desc)
				fmt.Fprintln(w)

				for _, opCmd := range serviceCmd.Commands() {
					printCommandDocs(serviceCmd.Name(), opCmd, w)
				}
			}
			w.Flush()
			return nil
		},
	}
}

func printCommandDocs(serviceName string, opCmd *cobra.Command, w io.Writer) {
	fmt.Fprintln(w, "###", serviceName, opCmd.Name())

	// collect relevant flags defined for the command
	inheritedFlags := opCmd.InheritedFlags()
	definedFlags := pflag.NewFlagSet(fmt.Sprintf("%v_%v", serviceName, opCmd.Name()), pflag.ContinueOnError)
	opCmd.Flags().VisitAll(func(f *pflag.Flag) {
		if f.Hidden {
			return
		}
		if f.Name == env.AttachDebugName || f.Name == env.JavaOptsName {
			return
		}
		if inheritedFlags.Lookup(f.Name) == nil {
			definedFlags.AddFlag(f)
		}
	})
	if definedFlags.HasFlags() {
		fmt.Fprintf(w, "Usage: `%v`\n\n", opCmd.UseLine())
	} else {
		// remove the [flags] part of the usage as there are no flags to mention
		fmt.Fprintf(w, "Usage: `%v`\n\n", strings.Replace(opCmd.UseLine(), " [flags]", "", 1))
	}

	desc := opCmd.Short
	if opCmd.Long != "" {
		desc = opCmd.Long
	}
	fmt.Fprintln(w, desc)
	fmt.Fprintln(w)

	if definedFlags.HasFlags() {
		fmt.Fprintln(w, "Flags:")
		definedFlags.VisitAll(func(f *pflag.Flag) {
			fmt.Fprintf(w, "- `--%v`", f.Name)
			if f.Shorthand != "" {
				fmt.Fprintf(w, ",`-%v`", f.Shorthand)
			}
			_, required := f.Annotations[cobra.BashCompOneRequiredFlag]
			_, usage := pflag.UnquoteUsage(f)

			// prepend the flag description with "(Required)" if required
			// print default value if flag is not required
			var requiredPrefix, defVal string
			if required {
				requiredPrefix = "(Required) "
			} else {
				v := f.DefValue
				if f.Value.Type() == "string" {
					// add quotes for string flags
					v = fmt.Sprintf("%q", defVal)
				}
				defVal = fmt.Sprintf(" (Default: %v)", v)
			}
			fmt.Fprintf(w, ": %v%v%v\n", requiredPrefix, usage, defVal)

		})
		fmt.Fprintln(w)
	}

	if opCmd.HasExample() {
		fmt.Fprintln(w, "Examples:")
		for _, ex := range strings.Split(opCmd.Example, "\n\n") {
			fmt.Fprintln(w, "```shell")
			fmt.Fprintln(w, ex)
			fmt.Fprintln(w, "```")
			fmt.Fprintln(w)
		}
		fmt.Fprintln(w)
	}
}
