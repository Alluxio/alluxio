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

package cmd

import (
	"fmt"
	"v.io/x/lib/cmdline"
	"strings"
)

var (
	Root = &cmdline.Command{
		Name:  "generate-tarballs",
		Short: "tool for creating alluxio tarballs",
		Long: `
	The publish tool contains functionality for generating either a single alluxio tarball,
or generating a suite of release tarballs.
	`,
		Children: []*cmdline.Command{
			cmdSingle,
			cmdRelease,
		},
	}

	debugFlag bool
	ufsModulesFlag string
)

func updateRootFlags() error {
	if strings.ToLower(ufsModulesFlag) == "all" {
		ufsModulesFlag = strings.Join(validModules(ufsModules), ",")
	}
	return nil
}

func checkRootFlags() error {
	for _, module := range strings.Split(ufsModulesFlag, ",") {
		if _, ok := ufsModules[module]; !ok {
			return fmt.Errorf("ufs module %v not recognized", module)
		}
	}
	return nil
}

func init() {
	Root.Flags.BoolVar(&debugFlag, "debug", false, "whether to run this tool in debug mode to generate additional console output")
	Root.Flags.StringVar(&ufsModulesFlag, "ufs-modules", strings.Join(defaultModules(ufsModules), ","),
		fmt.Sprintf("a comma-separated list of ufs modules to compile into the distribution tarball(s). Specify 'all' to build all ufs modules. Supported ufs modules: [%v]", strings.Join(validModules(ufsModules), ",")))
}
