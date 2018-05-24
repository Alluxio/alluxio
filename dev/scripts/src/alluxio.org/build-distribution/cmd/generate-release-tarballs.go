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

	"v.io/x/lib/cmdline"
)

var (
	cmdRelease = &cmdline.Command{
		Name:   "release",
		Short:  "Generates all release tarballs",
		Long:   "Generates all release tarballs",
		Runner: cmdline.RunnerFunc(release),
	}

	hadoopDistributionsFlag string
)

func init() {
	cmdRelease.Flags.StringVar(&hadoopDistributionsFlag, "hadoop-distributions", strings.Join(validHadoopDistributions(), ","), "a comma-separated list of hadoop distributions to generate Alluxio distributions for")
}

func checkReleaseFlags() error {
	for _, distribution := range strings.Split(hadoopDistributionsFlag, ",") {
		_, ok := hadoopDistributions[distribution]
		if !ok {
			return fmt.Errorf("hadoop distribution %s not recognized\n", distribution)
		}
	}
	return nil
}

func release(_ *cmdline.Env, _ []string) error {
	if err := checkReleaseFlags(); err != nil {
		return err
	}
	if err := generateTarballs(); err != nil {
		return err
	}
	return nil
}

func generateTarballs() error {
	for _, distribution := range strings.Split(hadoopDistributionsFlag, ",") {
		targetFlag = fmt.Sprintf("alluxio-%v-%v.tar.gz", versionMarker, distribution)
		fmt.Printf("Generating distribution for %v at %v", distribution, targetFlag)
		if err := generateTarball(distribution); err != nil {
			return err
		}
	}
	return nil
}
