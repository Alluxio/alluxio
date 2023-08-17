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

package job

import "alluxio.org/cli/env"

var Service = &env.Service{
	Name:        "job",
	Description: "Command line tool for interacting with the job service.",
	Commands: []env.Command{
		Cancel,
		CmdStatus,
		JobStatus,
		Leader,
		List,
		Load,
	},
}
