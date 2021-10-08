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
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strings"
)

const versionMarker = "${VERSION}"

// hadoopDistributions maps hadoop distributions to versions
var hadoopDistributions = map[string]version{
	"hadoop-2.2": parseVersion("2.2.0"),
	"hadoop-2.3": parseVersion("2.3.0"),
	"hadoop-2.4": parseVersion("2.4.1"),
	"hadoop-2.5": parseVersion("2.5.2"),
	"hadoop-2.6": parseVersion("2.6.5"),
	"hadoop-2.7": parseVersion("2.7.3"),
	"hadoop-2.8": parseVersion("2.8.5"),
	"hadoop-2.9": parseVersion("2.9.2"),
	"hadoop-3.0": parseVersion("3.0.3"),
	"hadoop-3.1": parseVersion("3.1.1"),
	"hadoop-3.2": parseVersion("3.2.1"),
	"hadoop-3.3": parseVersion("3.3.0"),
	// This distribution type is built with 2.7.3, but doesn't include the hadoop version in the name.
	"default": parseVersion("2.7.3"),
}

type module struct {
	name      string // the name used in the generated tarball
	isDefault bool   // whether to build the module by default
	mavenArgs string // maven args for building the module
}

// ufsModules is a map from ufs module to information for building the module.
var ufsModules = map[string]module{
	"ufs-hadoop-2.2": {"hadoop-2.2", true, "-pl underfs/hdfs -Pufs-hadoop-2 -Dufs.hadoop.version=2.2.0"},
	"ufs-hadoop-2.3": {"hadoop-2.3", false, "-pl underfs/hdfs -Pufs-hadoop-2 -Dufs.hadoop.version=2.3.0"},
	"ufs-hadoop-2.4": {"hadoop-2.4", false, "-pl underfs/hdfs -Pufs-hadoop-2 -Dufs.hadoop.version=2.4.1"},
	"ufs-hadoop-2.5": {"hadoop-2.5", false, "-pl underfs/hdfs -Pufs-hadoop-2 -Dufs.hadoop.version=2.5.2"},
	"ufs-hadoop-2.6": {"hadoop-2.6", false, "-pl underfs/hdfs -Pufs-hadoop-2 -Dufs.hadoop.version=2.6.5 -PhdfsActiveSync"},
	"ufs-hadoop-2.7": {"hadoop-2.7", true, "-pl underfs/hdfs -Pufs-hadoop-2 -Dufs.hadoop.version=2.7.3 -PhdfsActiveSync"},
	"ufs-hadoop-2.8": {"hadoop-2.8", true, "-pl underfs/hdfs -Pufs-hadoop-2 -Dufs.hadoop.version=2.8.5 -PhdfsActiveSync"},
	"ufs-hadoop-2.9": {"hadoop-2.9", true, "-pl underfs/hdfs -Pufs-hadoop-2 -Dufs.hadoop.version=2.9.2 -PhdfsActiveSync"},
	"ufs-hadoop-3.0": {"hadoop-3.0", false, "-pl underfs/hdfs -Pufs-hadoop-3 -Dufs.hadoop.version=3.0.0 -PhdfsActiveSync"},
	"ufs-hadoop-3.1": {"hadoop-3.1", false, "-pl underfs/hdfs -Pufs-hadoop-3 -Dufs.hadoop.version=3.1.1 -PhdfsActiveSync"},
	"ufs-hadoop-3.2": {"hadoop-3.2", true, "-pl underfs/hdfs -Pufs-hadoop-3 -Dufs.hadoop.version=3.2.1 -PhdfsActiveSync"},
	"ufs-hadoop-3.3": {"hadoop-3.3", false, "-pl underfs/hdfs -Pufs-hadoop-3 -Dufs.hadoop.version=3.3.0 -PhdfsActiveSync"},
}

var libJars = map[string]struct{}{
	"integration-tools-hms":        {},
	"integration-tools-validation": {},
	"table-server-underdb-glue":    {},
	"table-server-underdb-hive":    {},
	"underfs-abfs":                 {},
	"underfs-adl":                  {},
	"underfs-cephfs":               {},
	"underfs-cephfs-hadoop":        {},
	"underfs-cos":                  {},
	"underfs-cosn":                 {},
	"underfs-gcs":                  {},
	"underfs-local":                {},
	"underfs-oss":                  {},
	"underfs-ozone":                {},
	"underfs-s3a":                  {},
	"underfs-obs":                  {},
	"underfs-swift":                {},
	"underfs-wasb":                 {},
	"underfs-web":                  {},
}

func validModules(modules map[string]module) []string {
	result := []string{}
	for moduleName := range modules {
		result = append(result, moduleName)
	}
	sort.Strings(result)
	return result
}

func defaultModules(modules map[string]module) []string {
	result := []string{}
	for moduleName := range modules {
		if modules[moduleName].isDefault {
			result = append(result, moduleName)
		}
	}
	sort.Strings(result)
	return result
}

func validHadoopDistributions() []string {
	var result []string
	for distribution := range hadoopDistributions {
		result = append(result, distribution)
	}
	sort.Strings(result)
	return result
}

func run(desc, cmd string, args ...string) string {
	fmt.Printf("  %s ... ", desc)
	if debugFlag {
		fmt.Printf("\n    command: %s %s ... ", cmd, strings.Join(args, " "))
	}
	c := exec.Command(cmd, args...)
	stdout := &bytes.Buffer{}
	if debugFlag {
		// Stream the cmd's output (stdout and stderr) to os.Stdout, so that users can see the output while cmd is running.
		stdoutR, stdoutW := io.Pipe()
		stderrR, stderrW := io.Pipe()
		c.Stdout = stdoutW
		c.Stderr = stderrW
		stdouts := io.MultiWriter(stdout, os.Stdout)
		go func() {
			io.Copy(stdouts, stdoutR)
		}()
		go func() {
			io.Copy(os.Stderr, stderrR)
		}()
		if c.Run() != nil {
			os.Exit(1)
		}
	} else {
		c.Stdout = stdout
		stderr := &bytes.Buffer{}
		c.Stderr = stderr
		if err := c.Run(); err != nil {
			fmt.Printf("\"%v %v\" failed: %v\nstderr: <%v>\nstdout: <%v>\n", cmd, strings.Join(args, " "), err, stderr.String(), stdout.String())
			os.Exit(1)
		}
	}
	fmt.Println("done")
	return stdout.String()
}
