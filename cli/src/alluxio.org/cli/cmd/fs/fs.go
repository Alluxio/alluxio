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

package fs

import (
	"alluxio.org/cli/cmd/names"
	"alluxio.org/cli/env"
)

var Service = &env.Service{
	Name:        "fs",
	Description: "Operations to interface with the Alluxio filesystem",
	Documentation: `Operations to interface with the Alluxio filesystem
For commands that take Alluxio URIs as an argument such as ls or mkdir, the argument should be either
- A complete Alluxio URI, such as alluxio://<masterHostname>:<masterPort>/<path>
- A path without its scheme header, such as /path, in order to use the default hostname and port set in alluxio-site.properties

> Note: All fs commands require the Alluxio cluster to be running.

Most of the commands which require path components allow wildcard arguments for ease of use.
For example, the command "bin/alluxio fs rm '/data/2014*'" deletes anything in the data directory with a prefix of 2014.

Some shells will attempt to glob the input paths, causing strange errors.
As a workaround, you can disable globbing (depending on the shell type; for example, set -f) or by escaping wildcards
For example, the command "bin/alluxio fs cat /\\*" uses the escape backslash character twice.
This is because the shell script will eventually call a java program which should have the final escaped parameters "cat /\\*".
`,
	Commands: Cmds(names.FileSystemShellJavaClass),
}

func Cmds(className string) []env.Command {
	var ret []env.Command
	for _, c := range []func(string) env.Command{
		Cat,
		CheckCached,
		Checksum,
		Chgrp,
		Chmod,
		Chown,
		ConsistentHash,
		Cp,
		Head,
		Location,
		Ls,
		Mkdir,
		Mv,
		Rm,
		Stat,
		Tail,
		Test,
		Touch,
	} {
		ret = append(ret, c(className))
	}

	return ret
}
