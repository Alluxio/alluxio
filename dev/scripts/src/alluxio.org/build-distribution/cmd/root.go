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
	"flag"
	"fmt"
	"strings"
)

var (
	customUfsModuleFlag string
	debugFlag           bool
	includedLibJarsFlag string
	ufsModulesFlag      string
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

// common flags that are used regardless of subcommand type
// these flags provide additional settings unrelated to tarball generation
func additionalFlags(cmd *flag.FlagSet) {
	cmd.StringVar(&customUfsModuleFlag, "custom-ufs-module", "",
		"a percent-separated list of custom ufs modules which has the form of a pipe-separated pair of the module name and its comma-separated maven arguments."+
			" e.g. hadoop-a.b|-pl,underfs/hdfs,-Pufs-hadoop-A,-Dufs.hadoop.version=a.b.c%hadoop-x.y|-pl,underfs/hdfs,-Pufs-hadoop-X,-Dufs.hadoop.version=x.y.z")
	cmd.BoolVar(&debugFlag, "debug", false, "whether to run this tool in debug mode to generate additional console output")
	cmd.StringVar(&ufsModulesFlag, "ufs-modules", strings.Join(defaultModules(ufsModules), ","),
		fmt.Sprintf("a comma-separated list of ufs modules to compile into the distribution tarball(s). Specify 'all' to build all ufs modules. Supported ufs modules: [%v]", strings.Join(validModules(ufsModules), ",")))
	cmd.StringVar(&includedLibJarsFlag, "lib-jars", "all",
		"a comma-separated list of jars under lib/ to include in addition to all underfs-hdfs modules. All jars under lib/ will be included by default."+
			" e.g. underfs-cos,table-server-underdb-glue")
}
