// Copyright 2015 The Vanadium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import "v.io/x/lib/cmdline"

// cmdFlat represents the flat command.
var cmdFlat = &cmdline.Command{
	Runner:   cmdline.RunnerFunc(runFlat),
	Name:     "flat",
	Short:    "Short description of command flat",
	Long:     "Long description of command flat.",
	ArgsName: "[args]",
	ArgsLong: "[args] are ignored",
}

func runFlat(env *cmdline.Env, _ []string) error {
	return nil
}

func main() {
	cmdline.Main(cmdFlat)
}
