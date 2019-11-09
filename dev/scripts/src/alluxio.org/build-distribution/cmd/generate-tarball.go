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
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
)

const (
	// The version of the hadoop client that the Alluxio client will be built for
	defaultHadoopClient = "hadoop-2.7"
)
var (
	hadoopDistributionFlag = defaultHadoopClient
	targetFlag             string
	mvnArgsFlag            string
	skipUIFlag             bool
)

func Single(args []string) error {
	singleCmd := flag.NewFlagSet("single", flag.ExitOnError)
	// flags
	singleCmd.StringVar(&hadoopDistributionFlag, "hadoop-distribution", defaultHadoopClient, "the hadoop distribution to build this Alluxio distribution tarball")
	singleCmd.BoolVar(&skipUIFlag, "skip-ui", false, fmt.Sprintf("set this flag to skip building the webui. This will speed up the build times "+
		"but the generated tarball will have no Alluxio WebUI although REST services will still be available."))
	generateFlags(singleCmd)
	additionalFlags(singleCmd)
	singleCmd.Parse(args[2:]) // error handling by flag.ExitOnError

	if err := updateRootFlags(); err != nil {
		return err
	}
	if err := checkRootFlags(); err != nil {
		return err
	}
	if err := generateTarball([]string{}); err != nil {
		return err
	}
	return nil
}

// flags used by single and release to generate tarball
func generateFlags(cmd *flag.FlagSet) {
	cmd.StringVar(&mvnArgsFlag, "mvn-args", "", `a comma-separated list of additional Maven arguments to build with, e.g. -mvn-args "-Pspark,-Dhadoop.version=2.2.0"`)
	cmd.StringVar(&targetFlag, "target", fmt.Sprintf("alluxio-%v-bin.tar.gz", versionMarker),
		fmt.Sprintf("an optional target name for the generated tarball. The default is alluxio-%v.tar.gz. The string %q will be substituted with the built version. "+
			`Note that trailing ".tar.gz" will be stripped to determine the name for the Root directory of the generated tarball`, versionMarker, versionMarker))
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
		var versionMvnArg = "2.7.3"
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
		"bin/alluxio",
		"bin/alluxio-common.sh",
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
		"integration/docker/conf/alluxio-site.properties.template",
		"integration/docker/conf/alluxio-env.sh.template",
		"integration/fuse/bin/alluxio-fuse",
		"integration/kubernetes/alluxio-fuse.yaml.template",
		"integration/kubernetes/alluxio-fuse-client.yaml.template",
		"integration/kubernetes/helm-generate.sh",
		"integration/kubernetes/helm-chart/alluxio/.helmignore",
		"integration/kubernetes/helm-chart/alluxio/Chart.yaml",
		"integration/kubernetes/helm-chart/alluxio/values.yaml",
		"integration/kubernetes/helm-chart/alluxio/templates/_helpers.tpl",
		"integration/kubernetes/helm-chart/alluxio/templates/service-account.yaml",
		"integration/kubernetes/helm-chart/alluxio/templates/config/alluxio-conf.yaml",
		"integration/kubernetes/helm-chart/alluxio/templates/format/master.yaml",
		"integration/kubernetes/helm-chart/alluxio/templates/fuse/client-daemonset.yaml",
		"integration/kubernetes/helm-chart/alluxio/templates/fuse/daemonset.yaml",
		"integration/kubernetes/helm-chart/alluxio/templates/job/format-journal-job.yaml",
		"integration/kubernetes/helm-chart/alluxio/templates/master/journal-pv.yaml",
		"integration/kubernetes/helm-chart/alluxio/templates/master/journal-pvc.yaml",
		"integration/kubernetes/helm-chart/alluxio/templates/master/service.yaml",
		"integration/kubernetes/helm-chart/alluxio/templates/master/statefulset.yaml",
		"integration/kubernetes/helm-chart/alluxio/templates/worker/daemonset.yaml",
		"integration/kubernetes/multiMaster-embeddedJournal/alluxio-configmap.yaml.template",
		"integration/kubernetes/multiMaster-embeddedJournal/config.yaml",
		"integration/kubernetes/multiMaster-embeddedJournal/job/alluxio-format-journal-job.yaml.template",
		"integration/kubernetes/multiMaster-embeddedJournal/master/alluxio-master-service.yaml.template",
		"integration/kubernetes/multiMaster-embeddedJournal/master/alluxio-master-statefulset.yaml.template",
		"integration/kubernetes/multiMaster-embeddedJournal/worker/alluxio-worker-daemonset.yaml.template",
		"integration/kubernetes/singleMaster-hdfsJournal/alluxio-configmap.yaml.template",
		"integration/kubernetes/singleMaster-hdfsJournal/config.yaml",
		"integration/kubernetes/singleMaster-hdfsJournal/job/alluxio-format-journal-job.yaml.template",
		"integration/kubernetes/singleMaster-hdfsJournal/master/alluxio-master-service.yaml.template",
		"integration/kubernetes/singleMaster-hdfsJournal/master/alluxio-master-statefulset.yaml.template",
		"integration/kubernetes/singleMaster-hdfsJournal/worker/alluxio-worker-daemonset.yaml.template",
		"integration/kubernetes/singleMaster-localJournal/alluxio-configmap.yaml.template",
		"integration/kubernetes/singleMaster-localJournal/config.yaml",
		"integration/kubernetes/singleMaster-localJournal/job/alluxio-format-journal-job.yaml.template",
		"integration/kubernetes/singleMaster-localJournal/master/alluxio-master-journal-pv.yaml.template",
		"integration/kubernetes/singleMaster-localJournal/master/alluxio-master-journal-pvc.yaml.template",
		"integration/kubernetes/singleMaster-localJournal/master/alluxio-master-service.yaml.template",
		"integration/kubernetes/singleMaster-localJournal/master/alluxio-master-statefulset.yaml.template",
		"integration/kubernetes/singleMaster-localJournal/worker/alluxio-worker-daemonset.yaml.template",
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
		fmt.Sprintf("lib/alluxio-table-server-underdb-hive-%v.jar", version),
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
	hadoopVersion, ok := hadoopDistributions[hadoopDistributionFlag]
	if !ok {
		return fmt.Errorf("hadoop distribution %s not recognized\n", hadoopDistributionFlag)
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

	// Reset the shaded hadoop version if it's auto-resolved by the maven release plugin
	replace("shaded/hadoop/pom.xml", "<version>2.2.0</version>", "<version>${ufs.hadoop.version}</version>")
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
	if skipUIFlag {
		mvnArgsNoUI := append(mvnArgs, "-pl", "!webui")
		run("compiling repo without UI", "mvn", mvnArgsNoUI...)
	} else {
		run("compiling repo", "mvn", mvnArgs...)
	}

	// Compile ufs modules for the main build
	buildModules(srcPath, "underfs", "hdfs", ufsModulesFlag, version, ufsModules, mvnArgs)

	versionString := version
	if skipUIFlag {
		versionString = versionString + "-noUI"
	}
	tarball := strings.Replace(targetFlag, versionMarker, versionString, 1)

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

	if !skipUIFlag {
		masterWebappDir := "webui/master"
		run("creating webui master webapp directory", "mkdir", "-p", filepath.Join(dstPath, masterWebappDir))
		run("copying webui master webapp build directory", "cp", "-r", filepath.Join(masterWebappDir, "build"), filepath.Join(dstPath, masterWebappDir))

		workerWebappDir := "webui/worker"
		run("creating webui worker webapp directory", "mkdir", "-p", filepath.Join(dstPath, workerWebappDir))
		run("copying webui worker webapp build directory", "cp", "-r", filepath.Join(workerWebappDir, "build"), filepath.Join(dstPath, workerWebappDir))
	}
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
