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

var (
	customUfsModuleFlag string
	skipUIFlag          bool
	skipHelmFlag        bool
)

func Single(args []string) error {
	singleCmd := flag.NewFlagSet("single", flag.ExitOnError)
	// flags
	generateFlags(singleCmd)
	singleCmd.StringVar(&customUfsModuleFlag, "custom-ufs-module", "",
		"a percent-separated list of custom ufs modules which has the form of a pipe-separated pair of the module name and its comma-separated maven arguments."+
			" e.g. hadoop-a.b|-pl,underfs/hdfs,-Pufs-hadoop-A,-Dufs.hadoop.version=a.b.c%hadoop-x.y|-pl,underfs/hdfs,-Pufs-hadoop-X,-Dufs.hadoop.version=x.y.z")
	singleCmd.BoolVar(&skipUIFlag, "skip-ui", false, fmt.Sprintf("set this flag to skip building the webui. This will speed up the build times "+
		"but the generated tarball will have no Alluxio WebUI although REST services will still be available."))
	singleCmd.BoolVar(&skipHelmFlag, "skip-helm", true, fmt.Sprintf("set this flag to skip using Helm to generate YAML templates for K8s deployment scenarios"))
	singleCmd.Parse(args[2:]) // error handling by flag.ExitOnError

	if customUfsModuleFlag != "" {
		customUfsModules := strings.Split(customUfsModuleFlag, "%")
		for _, customUfsModule := range customUfsModules {
			customUfsModuleFlagArray := strings.Split(customUfsModule, "|")
			if len(customUfsModuleFlagArray) == 2 {
				customUfsModuleFlagArray[1] = strings.ReplaceAll(customUfsModuleFlagArray[1], ",", " ")
				ufsModules["ufs-"+customUfsModuleFlagArray[0]] = module{customUfsModuleFlagArray[0], true, customUfsModuleFlagArray[1]}
			} else {
				fmt.Fprintf(os.Stderr, "customUfsModuleFlag specified, but invalid: %s\n", customUfsModuleFlag)
				os.Exit(1)
			}
		}
	}
	if err := handleUfsModulesAndLibJars(); err != nil {
		return err
	}
	if debugFlag {
		fmt.Fprintf(os.Stdout, "hadoopDistributionFlag=: %s\n", hadoopDistributionFlag)
		fmt.Fprintf(os.Stdout, "customUfsModuleFlag=: %s\n", customUfsModuleFlag)
		fmt.Fprintf(os.Stdout, "includedLibJarsFlag=: %s\n", includedLibJarsFlag)
		fmt.Fprintf(os.Stdout, "mvnArgsFlag=: %s\n", mvnArgsFlag)
		fmt.Fprintf(os.Stdout, "targetFlag=: %s\n", targetFlag)
		fmt.Fprintf(os.Stdout, "ufs-modules=: %s\n", ufsModulesFlag)

		for _, ufsModule := range ufsModules {
			fmt.Fprintf(os.Stdout, "ufsModule=: %s\n", ufsModule)
		}
	}
	if err := generateTarball(skipUIFlag, skipHelmFlag); err != nil {
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
	args := []string{"-T", "1", "-am", "clean", "install", "-DskipTests", "-Dfindbugs.skip", "-Dmaven.javadoc.skip", "-Dcheckstyle.skip", "-Pno-webui-linter", "-Prelease"}
	if mvnArgsFlag != "" {
		for _, arg := range strings.Split(mvnArgsFlag, ",") {
			args = append(args, arg)
		}
	}

	args = append(args, fmt.Sprintf("-Dhadoop.version=%v", hadoopVersion), fmt.Sprintf("-P%v", hadoopVersion.hadoopProfile()))
	if includeYarnIntegration(hadoopVersion) {
		args = append(args, "-Pyarn")
	}
	return args
}

func includeYarnIntegration(hadoopVersion version) bool {
	return hadoopVersion.compare(2, 4, 0) >= 0
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
		var versionMvnArg = "3.3.0"
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
		"integration/docker/.dockerignore",
		"integration/docker/conf/alluxio-env.sh.template",
		"integration/docker/conf/alluxio-site.properties.template",
		"integration/docker/csi/alluxio/controllerserver.go",
		"integration/docker/csi/alluxio/driver.go",
		"integration/docker/csi/alluxio/nodeserver.go",
		"integration/docker/csi/go.mod",
		"integration/docker/csi/go.sum",
		"integration/docker/csi/main.go",
		"integration/docker/Dockerfile",
		"integration/docker/Dockerfile-dev",
		"integration/docker/dockerfile-common.sh",
		"integration/docker/entrypoint.sh",
		"integration/fuse/bin/alluxio-fuse",
		"integration/metrics/docker-compose-master.yaml",
		"integration/metrics/docker-compose-worker.yaml",
		"integration/metrics/otel-agent-config.yaml",
		"integration/metrics/otel-agent-config-worker.yaml",
		"integration/metrics/otel-collector-config.yaml",
		"integration/metrics/prometheus.yaml",
		"libexec/alluxio-config.sh",
		"LICENSE",
	}

	for _, jar := range strings.Split(includedLibJarsFlag, ",") {
		pathsToCopy = append(pathsToCopy, fmt.Sprintf("lib/alluxio-%v-%v.jar", jar, version))
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

func generateTarball(skipUI, skipHelm bool) error {
	hadoopVersion, ok := hadoopDistributions[hadoopDistributionFlag]
	if !ok {
		hadoopVersion = parseVersion(hadoopDistributionFlag)
		fmt.Printf("hadoop distribution %s not recognized, change to %s\n", hadoopDistributionFlag, hadoopVersion)
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

	mvnArgs := getCommonMvnArgs(hadoopVersion)
	if skipUI {
		mvnArgsNoUI := append(mvnArgs, "-pl", "!webui")
		run("compiling repo without UI", "mvn", mvnArgsNoUI...)
	} else {
		run("compiling repo", "mvn", mvnArgs...)
	}

	// Compile ufs modules for the main build
	buildModules(srcPath, "underfs", "hdfs", ufsModulesFlag, version, ufsModules, mvnArgs)

	versionString := version
	if skipUI {
		versionString = versionString + "-noUI"
	}
	if skipHelm {
		versionString = versionString + "-noHelm"
	}
	tarball := strings.Replace(targetFlag, versionMarker, versionString, 1)

	dstDir := strings.TrimSuffix(filepath.Base(tarball), ".tar.gz")
	dstDir = strings.TrimSuffix(dstDir, "-bin")
	dstPath := filepath.Join(cwd, dstDir)
	run(fmt.Sprintf("removing any existing %v", dstPath), "rm", "-rf", dstPath)
	fmt.Printf("Creating %s:\n", tarball)

	for _, dir := range []string{
		"assembly", "client", "logs", "integration/fuse", "integration/kubernetes", "logs/user",
	} {
		mkdir(filepath.Join(dstPath, dir))
	}

	if err := os.Chmod(filepath.Join(dstPath, "logs/user"), 0777); err != nil {
		return err
	}

	run("adding Alluxio client assembly jar", "mv", fmt.Sprintf("assembly/client/target/alluxio-assembly-client-%v-jar-with-dependencies.jar", version), filepath.Join(dstPath, "assembly", fmt.Sprintf("alluxio-client-%v.jar", version)))
	run("adding Alluxio server assembly jar", "mv", fmt.Sprintf("assembly/server/target/alluxio-assembly-server-%v-jar-with-dependencies.jar", version), filepath.Join(dstPath, "assembly", fmt.Sprintf("alluxio-server-%v.jar", version)))
	run("adding Alluxio FUSE jar", "mv", fmt.Sprintf("integration/fuse/target/alluxio-integration-fuse-%v-jar-with-dependencies.jar", version), filepath.Join(dstPath, "integration", "fuse", fmt.Sprintf("alluxio-fuse-%v.jar", version)))

	// Generate Helm templates in the dstPath
	run("adding Helm chart", "cp", "-r", filepath.Join(srcPath, "integration/kubernetes/helm-chart"), filepath.Join(dstPath, "integration/kubernetes/helm-chart"))
	run("adding YAML generator script", "cp", filepath.Join(srcPath, "integration/kubernetes/helm-generate.sh"), filepath.Join(dstPath, "integration/kubernetes/helm-generate.sh"))
	if !skipHelm {
		chdir(filepath.Join(dstPath, "integration/kubernetes/"))
		run("generate Helm templates", "bash", "helm-generate.sh", "all")
		chdir(srcPath)
	}

	if !skipUI {
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
	os.Setenv("COPYFILE_DISABLE", "1")
	run("creating the new distribution tarball", "tar", "-czvf", tarball, dstDir)
	run("removing the temporary repositories", "rm", "-rf", srcPath, dstPath)

	return nil
}
