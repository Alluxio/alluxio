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
	"os"

	"alluxio.org/cli/cmd/conf"
	"alluxio.org/cli/cmd/fs"
	"alluxio.org/cli/cmd/info"
	"alluxio.org/cli/cmd/journal"
	"alluxio.org/cli/cmd/process"
	"alluxio.org/cli/cmd/quorum"
	"alluxio.org/cli/env"
	"alluxio.org/cli/launch"
	"alluxio.org/cli/processes"
)

func main() {
	for _, p := range []env.Process{
		processes.JobMaster,
		processes.JobWorker,
		processes.Master,
		processes.Masters,
		processes.Proxy,
		processes.Proxies,
		processes.Worker,
		processes.Workers,
	} {
		env.RegisterProcess(p)
	}

	for _, c := range []*env.Service{
		conf.Service,
		fs.Service,
		info.Service,
		journal.Service,
		process.Service,
		quorum.Service,
	} {
		env.RegisterService(c)
	}

	if err := launch.Run(); err != nil {
		os.Exit(1)
	}
}
