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
	"strings"
)

var (
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
