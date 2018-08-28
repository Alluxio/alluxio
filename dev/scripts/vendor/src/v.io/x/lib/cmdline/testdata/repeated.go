// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import "v.io/x/lib/cmdline"

// cmdRepeated represents the repeated command.
var cmdRepeated = &cmdline.Command{
	Runner: cmdline.RunnerFunc(runRepeated),
	Name:   "repeated",
	Short:  "REPEATED SHOULD NEVER APPEAR",
	Long:   "REPEATED SHOULD NEVER APPEAR.",
}

func runRepeated(env *cmdline.Env, _ []string) error {
	return nil
}

func main() {
	cmdline.Main(cmdRepeated)
}
