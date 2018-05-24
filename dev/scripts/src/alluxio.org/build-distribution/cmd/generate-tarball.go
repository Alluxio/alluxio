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

	webappDir = "core/server/common/src/main/webapp"
	webappWar = "assembly/webapp.war"
)

func init() {
	cmdSingle.Flags.StringVar(&hadoopDistributionFlag, "hadoop-distribution", "hadoop-2.2", "the hadoop distribution to build this Alluxio distribution tarball")
	cmdSingle.Flags.StringVar(&targetFlag, "target", fmt.Sprintf("alluxio-%v.tar.gz", versionMarker),
		fmt.Sprintf("an optional target name for the generated tarball. The default is alluxio-%v.tar.gz. The string %q will be substituted with the built version. "+
			`Note that trailing ".tar.gz" will be stripped to determine the name for the Root directory of the generated tarball`, versionMarker, versionMarker))
	cmdSingle.Flags.StringVar(&mvnArgsFlag, "mvn-args", "", `a comma-separated list of additional Maven arguments to build with, e.g. -mvn-args "-Pspark,-Dhadoop.version=2.2.0"`)
}

func single(_ *cmdline.Env, _ []string) error {
	if err := generateTarball(hadoopDistributionFlag); err != nil {
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

func symlink(oldname, newname string) {
	if err := os.Symlink(oldname, newname); err != nil {
		fmt.Fprintf(os.Stderr, "Symlink(%v, %v) failed: %v\n", oldname, newname, err)
		os.Exit(1)
	}
}

func getCommonMvnArgs(hadoopVersion version) []string {
	args := []string{"clean", "install", "-DskipTests", "-Dfindbugs.skip", "-Dmaven.javadoc.skip", "-Dcheckstyle.skip", "-Pmesos"}
	if mvnArgsFlag != "" {
		for _, arg := range strings.Split(mvnArgsFlag, ",") {
			args = append(args, arg)
		}
	}

	args = append(args, fmt.Sprintf("-Dhadoop.version=%v", hadoopVersion), fmt.Sprintf("-P%v", hadoopVersion.hadoopProfile()))
	if includeYarnIntegration(hadoopVersion) {
		args = append(args, "-Pyarn")
	}
	if callHomeFlag {
		args = append(args, "-Dcall.home.enabled=true")
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

func addAdditionalFiles(srcPath, dstPath string, hadoopVersion version, version string) {
	chdir(srcPath)
	pathsToCopy := []string{
		"bin/alluxio",
		"bin/alluxio-masters.sh",
		"bin/alluxio-mount.sh",
		"bin/alluxio-start.sh",
		"bin/alluxio-stop.sh",
		"bin/alluxio-workers.sh",
		fmt.Sprintf("client/alluxio-%v-client.jar", version),
		"conf/alluxio-env.sh.template",
		"conf/alluxio-site.properties.template",
		"conf/core-site.xml.template",
		"conf/log4j.properties",
		"conf/masters",
		"conf/metrics.properties.template",
		"conf/workers",
		"integration/docker/Dockerfile",
		"integration/docker/entrypoint.sh",
		"integration/docker/bin/alluxio-master.sh",
		"integration/docker/bin/alluxio-proxy.sh",
		"integration/docker/bin/alluxio-worker.sh",
		"integration/docker/conf/alluxio-site.properties.template",
		"integration/docker/conf/alluxio-env.sh.template",
		"integration/fuse/bin/alluxio-fuse",
		"integration/kubernetes/alluxio-journal-volume.yaml.template",
		"integration/kubernetes/alluxio-master.yaml.template",
		"integration/kubernetes/alluxio-worker.yaml.template",
		"integration/kubernetes/conf/alluxio.properties.template",
		"integration/mesos/bin/alluxio-env-mesos.sh",
		"integration/mesos/bin/alluxio-mesos-start.sh",
		"integration/mesos/bin/alluxio-master-mesos.sh",
		"integration/mesos/bin/alluxio-mesos-stop.sh",
		"integration/mesos/bin/alluxio-worker-mesos.sh",
		"integration/mesos/bin/common.sh",
		fmt.Sprintf("lib/alluxio-underfs-gcs-%v.jar", version),
		fmt.Sprintf("lib/alluxio-underfs-hdfs-%v.jar", version),
		fmt.Sprintf("lib/alluxio-underfs-local-%v.jar", version),
		fmt.Sprintf("lib/alluxio-underfs-oss-%v.jar", version),
		fmt.Sprintf("lib/alluxio-underfs-s3a-%v.jar", version),
		fmt.Sprintf("lib/alluxio-underfs-swift-%v.jar", version),
		fmt.Sprintf("lib/alluxio-underfs-wasb-%v.jar", version),
		"libexec/alluxio-config.sh",
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

	// Add links for previous jar locations for backwards compatibility
	for _, jar := range []string{"client", "server"} {
		oldLocation := filepath.Join(dstPath, fmt.Sprintf("assembly/%v/target/alluxio-assembly-%v-%v-jar-with-dependencies.jar", jar, jar, version))
		mkdir(filepath.Dir(oldLocation))
		symlink(fmt.Sprintf("../../alluxio-%v-%v.jar", jar, version), oldLocation)
	}
	mkdir(filepath.Join(dstPath, "assembly/server/target"))
}

func generateTarball(hadoopDistribution string) error {
	hadoopVersion := hadoopDistributions[hadoopDistribution]
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

	// Update the web app location.
	replace("core/common/src/main/java/alluxio/PropertyKey.java", webappDir, webappWar)
	// Update the assembly jar paths.
	replace("libexec/alluxio-config.sh", "assembly/client/target/alluxio-assembly-client-${VERSION}-jar-with-dependencies.jar", "assembly/alluxio-client-${VERSION}.jar")
	replace("libexec/alluxio-config.sh", "assembly/server/target/alluxio-assembly-server-${VERSION}-jar-with-dependencies.jar", "assembly/alluxio-server-${VERSION}.jar")
	// Update the FUSE jar path
	replace("integration/fuse/bin/alluxio-fuse", "target/alluxio-integration-fuse-${VERSION}-jar-with-dependencies.jar", "alluxio-fuse-${VERSION}.jar")

	mvnArgs := getCommonMvnArgs(hadoopVersion)
	run("compiling repo", "mvn", mvnArgs...)

	tarball := strings.Replace(targetFlag, versionMarker, version, 1)
	dstDir := strings.TrimSuffix(filepath.Base(tarball), ".tar.gz")
	dstPath := filepath.Join(cwd, dstDir)
	run(fmt.Sprintf("removing any existing %v", dstPath), "rm", "-rf", dstPath)
	fmt.Printf("Creating %s:\n", tarball)

	// Create the directory for the server jar.
	mkdir(filepath.Join(dstPath, "assembly"))
	// Create directories for the client jar.
	mkdir(filepath.Join(dstPath, "client"))
	mkdir(filepath.Join(dstPath, "logs"))
	// Create directories for the fuse connector
	mkdir(filepath.Join(dstPath, "integration", "fuse"))

	run("adding Alluxio client assembly jar", "mv", fmt.Sprintf("assembly/client/target/alluxio-assembly-client-%v-jar-with-dependencies.jar", version), filepath.Join(dstPath, "assembly", fmt.Sprintf("alluxio-client-%v.jar", version)))
	run("adding Alluxio server assembly jar", "mv", fmt.Sprintf("assembly/server/target/alluxio-assembly-server-%v-jar-with-dependencies.jar", version), filepath.Join(dstPath, "assembly", fmt.Sprintf("alluxio-server-%v.jar", version)))
	run("adding Alluxio FUSE jar", "mv", fmt.Sprintf("integration/fuse/target/alluxio-integration-fuse-%v-jar-with-dependencies.jar", version), filepath.Join(dstPath, "integration", "fuse", fmt.Sprintf("alluxio-fuse-%v.jar", version)))
	// Condense the webapp into a single .war file.
	run("jarring up webapp", "jar", "-cf", filepath.Join(dstPath, webappWar), "-C", webappDir, ".")

	if includeYarnIntegration(hadoopVersion) {
		// Update the YARN jar path
		replace("integration/yarn/bin/alluxio-yarn.sh", "target/alluxio-integration-yarn-${VERSION}-jar-with-dependencies.jar", "alluxio-yarn-${VERSION}.jar")
		// Create directories for the yarn integration
		mkdir(filepath.Join(dstPath, "integration", "yarn"))
		run("adding Alluxio YARN jar", "mv", fmt.Sprintf("integration/yarn/target/alluxio-integration-yarn-%v-jar-with-dependencies.jar", version), filepath.Join(dstPath, "integration", "yarn", fmt.Sprintf("alluxio-yarn-%v.jar", version)))
	}

	addAdditionalFiles(srcPath, dstPath, hadoopVersion, version)

	chdir(cwd)
	run("creating the distribution tarball", "tar", "-czvf", tarball, dstDir)
	run("removing the temporary repositories", "rm", "-rf", srcPath, dstPath)

	return nil
}
