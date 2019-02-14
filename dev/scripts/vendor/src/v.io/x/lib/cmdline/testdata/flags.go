// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"fmt"

	"v.io/x/lib/cmdline"
)

// cmdFlags represents the flags command.
var cmdFlags = &cmdline.Command{
	Runner:   cmdline.RunnerFunc(runFlags),
	Name:     "flags",
	Short:    "Short description of command flags",
	Long:     "Long description of command flags.",
	ArgsName: "[args]",
	ArgsLong: "[args] are ignored",
}

func runFlags(env *cmdline.Env, args []string) error {
	fmt.Fprintf(env.Stdout, "global1=%q shared=%q local=%q %q\n", flagGlobal1, flagShared, flagLocal, args)
	return nil
}

var (
	flagGlobal1, flagShared, flagLocal string
)

func main() {
	cmdFlags.Flags.StringVar(&flagGlobal1, "global1", "", "description of global1")
	cmdFlags.Flags.StringVar(&flagShared, "shared", "", "description of shared")
	cmdFlags.Flags.StringVar(&flagLocal, "local", "", "description of local")
	cmdline.Main(cmdFlags)
}
