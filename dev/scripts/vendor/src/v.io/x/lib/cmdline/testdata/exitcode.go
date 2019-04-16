// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import "v.io/x/lib/cmdline"

// cmdExitCode represents the exitcode command.
var cmdExitCode = &cmdline.Command{
	Runner:   cmdline.RunnerFunc(runExitCode),
	Name:     "exitcode",
	Short:    "Short description of command exitcode",
	Long:     "Long description of command exitcode.",
	ArgsName: "[args]",
	ArgsLong: "[args] are ignored",
}

func runExitCode(env *cmdline.Env, _ []string) error {
	return cmdline.ErrExitCode(42)
}

func main() {
	cmdline.Main(cmdExitCode)
}
