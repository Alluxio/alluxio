// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import "v.io/x/lib/cmdline"

// cmdNested represents the nested command.
var cmdNested = &cmdline.Command{
	Name:     "nested",
	Short:    "Short description of command nested",
	Long:     "Long description of command nested.",
	LookPath: true,
	Children: []*cmdline.Command{cmdChild},
}

// cmdChild represents the child command.
var cmdChild = &cmdline.Command{
	Runner: cmdline.RunnerFunc(runChild),
	Name:   "child",
	Short:  "Short description of command child",
	Long:   "Long description of command child.",
}

func runChild(env *cmdline.Env, _ []string) error {
	return env.UsageErrorf("wombats!")
}

func main() {
	cmdline.Main(cmdNested)
}
