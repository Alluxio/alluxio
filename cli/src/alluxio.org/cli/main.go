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
	"path/filepath"
	"strings"

	"github.com/spf13/viper"

	"alluxio.org/cli/cmd/cache"
	"alluxio.org/cli/cmd/conf"
	"alluxio.org/cli/cmd/exec"
	"alluxio.org/cli/cmd/fs"
	"alluxio.org/cli/cmd/generate"
	"alluxio.org/cli/cmd/info"
	"alluxio.org/cli/cmd/initiate"
	"alluxio.org/cli/cmd/job"
	"alluxio.org/cli/cmd/journal"
	"alluxio.org/cli/cmd/process"
	"alluxio.org/cli/env"
	"alluxio.org/cli/launch"
	"alluxio.org/cli/processes"
)

func main() {
	for _, p := range []env.Process{
		processes.All,
		processes.JobMaster,
		processes.JobMasters,
		processes.JobWorker,
		processes.JobWorkers,
		processes.Local,
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
		cache.Service,
		conf.Service,
		exec.Service,
		fs.Service,
		generate.Service,
		initiate.Service,
		info.Service,
		job.Service,
		journal.Service,
		process.Service,
	} {
		env.RegisterService(c)
	}

	// isDeployed bool -> env var key -> relative path from root with version placeholder
	jarEnvVars := map[bool]map[string]string{
		false: {
			env.EnvAlluxioAssemblyClientJar: filepath.Join("assembly", "client", "target", "alluxio-assembly-client-%v-jar-with-dependencies.jar"),
			env.EnvAlluxioAssemblyServerJar: filepath.Join("assembly", "server", "target", "alluxio-assembly-server-%v-jar-with-dependencies.jar"),
		},
		true: {
			env.EnvAlluxioAssemblyClientJar: filepath.Join("assembly", "alluxio-client-%v.jar"),
			env.EnvAlluxioAssemblyServerJar: filepath.Join("assembly", "alluxio-server-%v.jar"),
		},
	}
	appendClasspathJars := map[string]func(*viper.Viper, string) string{
		env.EnvAlluxioClientClasspath: func(envVar *viper.Viper, ver string) string {
			return strings.Join([]string{
				envVar.GetString(env.EnvAlluxioClientClasspath),
				filepath.Join(envVar.GetString(env.ConfAlluxioHome.EnvVar), "lib", fmt.Sprintf("alluxio-integration-tools-validation-%v.jar", ver)),
			}, ":")

		},
	}

	if err := launch.Run(jarEnvVars, appendClasspathJars); err != nil {
		os.Exit(1)
	}
}
