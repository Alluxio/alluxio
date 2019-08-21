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
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"

	"v.io/x/lib/cmdline"
)

const (
	// The version of the hadoop client that the Alluxio client will be built for
	defaultHadoopClient = "hadoop-2.7"
)
var (
	cmdSingle = &cmdline.Command{
		Name:   "single",
		Short:  "Generates an alluxio tarball",
		Long:   "Generates an alluxio tarball",
		Runner: cmdline.RunnerFunc(single),
	}

	hadoopDistributionFlag string
	targetFlag             string
	mvnArgsFlag            string
)

func init() {
	cmdSingle.Flags.StringVar(&hadoopDistributionFlag, "hadoop-distribution", "hadoop-2.2", "the hadoop distribution to build this Alluxio distribution tarball")
	cmdSingle.Flags.StringVar(&targetFlag, "target", fmt.Sprintf("alluxio-%v-bin.tar.gz", versionMarker),
		fmt.Sprintf("an optional target name for the generated tarball. The default is alluxio-%v.tar.gz. The string %q will be substituted with the built version. "+
			`Note that trailing ".tar.gz" will be stripped to determine the name for the Root directory of the generated tarball`, versionMarker, versionMarker))
	cmdSingle.Flags.StringVar(&mvnArgsFlag, "mvn-args", "", `a comma-separated list of additional Maven arguments to build with, e.g. -mvn-args "-Pspark,-Dhadoop.version=2.2.0"`)
}

func single(_ *cmdline.Env, _ []string) error {
	if err := updateRootFlags(); err != nil {
		return err
	}
	if err := checkRootFlags(); err != nil {
		return err
	}
	if err := generateTarball([]string{hadoopDistributionFlag}); err != nil {
		return err
	}
	return nil
}

func replace(path, old, new string) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ReadFile() failed: %v\n", err)
		os.Exit(1)
	}
	data = bytes.Replace(data, []byte(old), []byte(new), -1)
	if err := ioutil.WriteFile(path, data, os.FileMode(0644)); err != nil {
		fmt.Fprintf(os.Stderr, "WriteFile() failed: %v\n", err)
		os.Exit(1)
	}
}

func mkdir(path string) {
	if err := os.MkdirAll(path, os.FileMode(0755)); err != nil {
		fmt.Fprintf(os.Stderr, "MkdirAll() failed: %v\n", err)
		os.Exit(1)
	}
}

func chdir(path string) {
	if err := os.Chdir(path); err != nil {
		fmt.Fprintf(os.Stderr, "Chdir() failed: %v\n", err)
		os.Exit(1)
	}
}

func getCommonMvnArgs(hadoopVersion version) []string {
	args := []string{"-T", "2C", "-am", "clean", "install", "-DskipTests", "-Dfindbugs.skip", "-Dmaven.javadoc.skip", "-Dcheckstyle.skip", "-Pmesos"}
	if mvnArgsFlag != "" {
		for _, arg := range strings.Split(mvnArgsFlag, ",") {
			args = append(args, arg)
		}
	}

	args = append(args, fmt.Sprintf("-Dhadoop.version=%v", hadoopVersion), fmt.Sprintf("-P%v", hadoopVersion.hadoopProfile()))
	if includeYarnIntegration(hadoopVersion) {
		args = append(args, "-Pyarn")
	}
	if hadoopVersion.major == 1 {
		// checker requires hadoop 2+ to compile.
		args = append(args, "-Dchecker.hadoop.version=2.2.0")
	}
	return args
}

func includeYarnIntegration(hadoopVersion version) bool {
	return hadoopVersion.major >= 2 && hadoopVersion.minor >= 4
}

func getVersion() (string, error) {
	versionLine := run("grepping for the version", "grep", "-m1", "<version>", "pom.xml")
	re := regexp.MustCompile(".*<version>(.*)</version>.*")
	match := re.FindStringSubmatch(versionLine)
	if len(match) < 2 {
		return "", errors.New("failed to find version")
	}
	return match[1], nil
}

func addModules(srcPath, dstPath, name, moduleFlag, version string, modules map[string]module) {
	for _, moduleName := range strings.Split(moduleFlag, ",") {
		moduleEntry, ok := modules[moduleName]
		if !ok {
			// This should be impossible, we validate modulesFlag at the start.
			fmt.Fprintf(os.Stderr, "Unrecognized %v module: %v", name, moduleName)
			os.Exit(1)
		}
		moduleJar := fmt.Sprintf("alluxio-%v-%v-%v.jar", name, moduleEntry.name, version)
		run(fmt.Sprintf("adding %v module %v to lib/", name, moduleName), "mv", filepath.Join(srcPath, "lib", moduleJar), filepath.Join(dstPath, "lib"))
	}
}

func buildModules(srcPath, name, ufsType, moduleFlag, version string, modules map[string]module, mvnArgs []string) {
	// Compile modules for the main build
	for _, moduleName := range strings.Split(moduleFlag, ",") {
		moduleEntry := modules[moduleName]
		moduleMvnArgs := mvnArgs
		for _, arg := range strings.Split(moduleEntry.mavenArgs, " ") {
			moduleMvnArgs = append(moduleMvnArgs, arg)
		}
		var versionMvnArg = "2.2.0"
		for _, arg := range moduleMvnArgs {
			if strings.Contains(arg, "ufs.hadoop.version") {
				versionMvnArg = strings.Split(arg, "=")[1]
			}
		}
		run(fmt.Sprintf("compiling %v module %v", name, moduleName), "mvn", moduleMvnArgs...)
		var srcJar string
		if ufsType == "hdfs" {
			srcJar = fmt.Sprintf("alluxio-%v-%v-%v-%v.jar", name, ufsType, versionMvnArg, version)
		} else {
			srcJar = fmt.Sprintf("alluxio-%v-%v-%v.jar", name, ufsType, version)
		}
		dstJar := fmt.Sprintf("alluxio-%v-%v-%v.jar", name, moduleEntry.name, version)
		run(fmt.Sprintf("saving %v module %v", name, moduleName), "mv", filepath.Join(srcPath, "lib", srcJar), filepath.Join(srcPath, "lib", dstJar))
	}
}

func addAdditionalFiles(srcPath, dstPath string, hadoopVersion version, version string) {
	chdir(srcPath)
	pathsToCopy := []string{
		"integration/docker/bin/alluxio-job-worker.sh",
		"integration/docker/bin/alluxio-job-master.sh",
		"bin/alluxio",
		"bin/alluxio-masters.sh",
		"bin/alluxio-monitor.sh",
		"bin/alluxio-mount.sh",
		"bin/alluxio-start.sh",
		"bin/alluxio-stop.sh",
		"bin/alluxio-workers.sh",
		"bin/launch-process",
		fmt.Sprintf("client/alluxio-%v-client.jar", version),
		"conf/alluxio-env.sh.template",
		"conf/alluxio-site.properties.template",
		"conf/core-site.xml.template",
		"conf/log4j.properties",
		"conf/masters",
		"conf/metrics.properties.template",
		"conf/workers",
		"integration/checker/bin/alluxio-checker.sh",
		"integration/checker/bin/hive-checker.sh",
		"integration/checker/bin/mapreduce-checker.sh",
		"integration/checker/bin/spark-checker.sh",
		"integration/docker/Dockerfile",
		"integration/docker/Dockerfile.fuse",
		"integration/docker/entrypoint.sh",
		"integration/docker/bin/alluxio-master.sh",
		"integration/docker/bin/alluxio-proxy.sh",
		"integration/docker/bin/alluxio-worker.sh",
		"integration/docker/conf/alluxio-site.properties.template",
		"integration/docker/conf/alluxio-env.sh.template",
		"integration/fuse/bin/alluxio-fuse",
		"integration/kubernetes/alluxio-fuse.yaml.template",
		"integration/kubernetes/alluxio-fuse-client.yaml.template",
		"integration/kubernetes/alluxio-journal-volume.yaml.template",
		"integration/kubernetes/alluxio-configMap.yaml.template",
		"integration/kubernetes/alluxio-master.yaml.template",
		"integration/kubernetes/alluxio-worker.yaml.template",
		"integration/kubernetes/helm/alluxio/Chart.yaml",
		"integration/kubernetes/helm/alluxio/templates/_helpers.tpl",
		"integration/kubernetes/helm/alluxio/templates/alluxio-configMap.yaml",
		"integration/kubernetes/helm/alluxio/templates/alluxio-master.yaml",
		"integration/kubernetes/helm/alluxio/templates/alluxio-worker.yaml",
		"integration/kubernetes/helm/alluxio/templates/service-account.yaml",
		"integration/kubernetes/helm/alluxio/values.yaml",
		"integration/mesos/bin/alluxio-env-mesos.sh",
		"integration/mesos/bin/alluxio-mesos-start.sh",
		"integration/mesos/bin/alluxio-master-mesos.sh",
		"integration/mesos/bin/alluxio-mesos-stop.sh",
		"integration/mesos/bin/alluxio-worker-mesos.sh",
		"integration/mesos/bin/common.sh",
		fmt.Sprintf("lib/alluxio-underfs-cos-%v.jar", version),
		fmt.Sprintf("lib/alluxio-underfs-gcs-%v.jar", version),
		fmt.Sprintf("lib/alluxio-underfs-local-%v.jar", version),
		fmt.Sprintf("lib/alluxio-underfs-oss-%v.jar", version),
		fmt.Sprintf("lib/alluxio-underfs-s3a-%v.jar", version),
		fmt.Sprintf("lib/alluxio-underfs-swift-%v.jar", version),
		fmt.Sprintf("lib/alluxio-underfs-wasb-%v.jar", version),
		"libexec/alluxio-config.sh",
		"LICENSE",
	}
	if includeYarnIntegration(hadoopVersion) {
		pathsToCopy = append(pathsToCopy, []string{
			"integration/yarn/bin/alluxio-application-master.sh",
			"integration/yarn/bin/alluxio-master-yarn.sh",
			"integration/yarn/bin/alluxio-worker-yarn.sh",
			"integration/yarn/bin/alluxio-yarn.sh",
			"integration/yarn/bin/alluxio-yarn-setup.sh",
			"integration/yarn/bin/common.sh",
		}...)
	}
	for _, path := range pathsToCopy {
		mkdir(filepath.Join(dstPath, filepath.Dir(path)))
		run(fmt.Sprintf("adding %v", path), "cp", path, filepath.Join(dstPath, path))
	}

	// Create empty directories for default UFS and Docker integration.
	mkdir(filepath.Join(dstPath, "underFSStorage"))
	mkdir(filepath.Join(dstPath, "integration/docker/conf"))

	mkdir(filepath.Join(dstPath, "lib"))
	addModules(srcPath, dstPath, "underfs", ufsModulesFlag, version, ufsModules)
}

func generateTarball(hadoopClients []string) error {
	hadoopVersion, ok := hadoopDistributions[defaultHadoopClient]
	if !ok {
		return fmt.Errorf("hadoop distribution %s not recognized\n", defaultHadoopClient)
	}
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}

	srcPath, err := ioutil.TempDir("", "alluxio")
	if err != nil {
		return fmt.Errorf("Failed to create temp directory: %v", err)
	}

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return errors.New("Failed to determine file of the go script")
	}
	// Relative path from this class to the root directory.
	repoPath := filepath.Join(filepath.Dir(file), "../../../../../../")
	run(fmt.Sprintf("copying source from %v to %v", repoPath, srcPath), "cp", "-R", repoPath+"/.", srcPath)

	chdir(srcPath)
	run("running git clean -fdx", "git", "clean", "-fdx")

	version, err := getVersion()
	if err != nil {
		return err
	}

	// Update the assembly jar paths.
	replace("libexec/alluxio-config.sh", "assembly/client/target/alluxio-assembly-client-${VERSION}-jar-with-dependencies.jar", "assembly/alluxio-client-${VERSION}.jar")
	replace("libexec/alluxio-config.sh", "assembly/server/target/alluxio-assembly-server-${VERSION}-jar-with-dependencies.jar", "assembly/alluxio-server-${VERSION}.jar")
	// Update the FUSE jar path
	replace("integration/fuse/bin/alluxio-fuse", "target/alluxio-integration-fuse-${VERSION}-jar-with-dependencies.jar", "alluxio-fuse-${VERSION}.jar")
	// Update the checker jar paths
	for _, file := range []string{"bin/hive-checker.sh", "bin/mapreduce-checker.sh", "bin/spark-checker.sh"} {
		replace(filepath.Join("integration/checker", file), "target/alluxio-checker-${VERSION}-jar-with-dependencies.jar", "alluxio-checker-${VERSION}.jar")
	}

	mvnArgs := getCommonMvnArgs(hadoopVersion)
	run("compiling repo", "mvn", mvnArgs...)
	// Compile ufs modules for the main build
	buildModules(srcPath, "underfs", "hdfs", ufsModulesFlag, version, ufsModules, mvnArgs)

	tarball := strings.Replace(targetFlag, versionMarker, version, 1)
	dstDir := strings.TrimSuffix(filepath.Base(tarball), ".tar.gz")
	dstDir = strings.TrimSuffix(dstDir, "-bin")
	dstPath := filepath.Join(cwd, dstDir)
	run(fmt.Sprintf("removing any existing %v", dstPath), "rm", "-rf", dstPath)
	fmt.Printf("Creating %s:\n", tarball)

	for _, dir := range []string{
		"assembly", "client", "logs", "integration/fuse", "integration/checker", "logs/user",
	} {
		mkdir(filepath.Join(dstPath, dir))
	}

	if err := os.Chmod(filepath.Join(dstPath, "logs/user"), 0777); err != nil {
		return err
	}

	run("adding Alluxio client assembly jar", "mv", fmt.Sprintf("assembly/client/target/alluxio-assembly-client-%v-jar-with-dependencies.jar", version), filepath.Join(dstPath, "assembly", fmt.Sprintf("alluxio-client-%v.jar", version)))
	run("adding Alluxio server assembly jar", "mv", fmt.Sprintf("assembly/server/target/alluxio-assembly-server-%v-jar-with-dependencies.jar", version), filepath.Join(dstPath, "assembly", fmt.Sprintf("alluxio-server-%v.jar", version)))
	run("adding Alluxio FUSE jar", "mv", fmt.Sprintf("integration/fuse/target/alluxio-integration-fuse-%v-jar-with-dependencies.jar", version), filepath.Join(dstPath, "integration", "fuse", fmt.Sprintf("alluxio-fuse-%v.jar", version)))
	run("adding Alluxio checker jar", "mv", fmt.Sprintf("integration/checker/target/alluxio-checker-%v-jar-with-dependencies.jar", version), filepath.Join(dstPath, "integration", "checker", fmt.Sprintf("alluxio-checker-%v.jar", version)))

	masterWebappDir := "webui/master"
	run("creating webui master webapp directory", "mkdir", "-p", filepath.Join(dstPath, masterWebappDir))
	run("copying webui master webapp build directory", "cp", "-r", filepath.Join(masterWebappDir, "build"), filepath.Join(dstPath, masterWebappDir))

	workerWebappDir := "webui/worker"
	run ("creating webui worker webapp directory", "mkdir", "-p", filepath.Join(dstPath, workerWebappDir))
	run("copying webui worker webapp build directory", "cp", "-r", filepath.Join(workerWebappDir, "build"), filepath.Join(dstPath, workerWebappDir))

	if includeYarnIntegration(hadoopVersion) {
		// Update the YARN jar path
		replace("integration/yarn/bin/alluxio-yarn.sh", "target/alluxio-integration-yarn-${VERSION}-jar-with-dependencies.jar", "alluxio-yarn-${VERSION}.jar")
		// Create directories for the yarn integration
		mkdir(filepath.Join(dstPath, "integration", "yarn"))
		run("adding Alluxio YARN jar", "mv", fmt.Sprintf("integration/yarn/target/alluxio-integration-yarn-%v-jar-with-dependencies.jar", version), filepath.Join(dstPath, "integration", "yarn", fmt.Sprintf("alluxio-yarn-%v.jar", version)))
	}

	addAdditionalFiles(srcPath, dstPath, hadoopVersion, version)

	chdir(cwd)
	os.Setenv("COPYFILE_DISABLE","1")
	run("creating the new distribution tarball", "tar", "-czvf", tarball, dstDir)
	run("removing the temporary repositories", "rm", "-rf", srcPath, dstPath)

	return nil
}
