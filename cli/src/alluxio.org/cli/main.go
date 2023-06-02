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

package main

import (
	"fmt"
	"os"

	"alluxio.org/cli/cmd/conf"
	"alluxio.org/cli/env"
	"alluxio.org/cli/launch"
)

func main() {
	for _, c := range []*env.Service{
		conf.Service,
	} {
		env.RegisterService(c)
	}

	if err := launch.Run(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
