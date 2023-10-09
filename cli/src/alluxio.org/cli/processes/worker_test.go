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

package processes

import (
	"strings"
	"testing"

	"alluxio.org/cli/env"
)

func TestWorkerProcess_startCmdArgs(t *testing.T) {
	env.RegisterProcess(Worker)
	for name, tc := range map[string]struct {
		alluxioEnv    map[string]string
		expectNumArgs int
	}{
		"none": {
			expectNumArgs: 15,
		},
		"2 alluxio java opts": {
			alluxioEnv: map[string]string{
				env.ConfAlluxioJavaOpts.EnvVar: "-DalluxioJava1=opts1 -DalluxioJava2=opts2",
			},
			expectNumArgs: 17,
		},
		"2 alluxio java opts with spaces": {
			alluxioEnv: map[string]string{
				env.ConfAlluxioJavaOpts.EnvVar: " -DalluxioJava1=opts1 -DalluxioJava2=opts2 ",
			},
			expectNumArgs: 17,
		},
		"2 worker java opts with spaces": {
			alluxioEnv: map[string]string{
				ConfAlluxioWorkerJavaOpts.EnvVar: " -DaworkerJava1=opts1 -DworkerJava2=opts2 ",
			},
			expectNumArgs: 17,
		},
		"2 worker attach java opts with spaces": {
			alluxioEnv: map[string]string{
				confAlluxioWorkerAttachOpts.EnvVar: " -DaworkerAttach1=opts1 -DworkerAttach2=opts2 ",
			},
			expectNumArgs: 17,
		},
		"1 each of alluxio java, worker java, worker attach": {
			alluxioEnv: map[string]string{
				env.ConfAlluxioJavaOpts.EnvVar:     "-DalluxioJava1=opts1",
				ConfAlluxioWorkerJavaOpts.EnvVar:   "-DaworkerJava1=opts1",
				confAlluxioWorkerAttachOpts.EnvVar: "-DaworkerAttach1=opts1",
			},
			expectNumArgs: 18,
		},
		"1 each of alluxio java, worker java, worker attach with spaces": {
			alluxioEnv: map[string]string{
				env.ConfAlluxioJavaOpts.EnvVar:     " -DalluxioJava1=opts1 ",
				ConfAlluxioWorkerJavaOpts.EnvVar:   " -DaworkerJava1=opts1 ",
				confAlluxioWorkerAttachOpts.EnvVar: " -DaworkerAttach1=opts1 ",
			},
			expectNumArgs: 18,
		},
	} {
		env.InitEnvForTesting(tc.alluxioEnv)
		args := Worker.startCmdArgs()
		if len(args) != tc.expectNumArgs {
			t.Errorf("case %v: expected %v args but got %v:\n%v",
				name, tc.expectNumArgs, len(args), strings.Join(args, "\n"))
		}
		for i, arg := range args {
			if arg == "" {
				t.Errorf("case %v: %dth argument is empty", name, i)
			}
			if arg != strings.TrimSpace(arg) {
				t.Errorf("case %v: %dth argument has leading or trailing spaces:\n%v", name, i, arg)
			}
		}
	}
}
