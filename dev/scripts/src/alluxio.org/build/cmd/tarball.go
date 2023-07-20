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
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/palantir/stacktrace"

	"alluxio.org/common/command"
)

type assembledJarsInfo struct {
	generatedJarPath string                       // relative path of generated jar, before formatting with alluxio version string
	tarballJarPath   string                       // relative path of copied jar into the tarball, before formatting with alluxio version string
	fileReplacements map[string]map[string]string // location of file to execute replacement -> find -> replace
}

var (
	assembledJars = map[string]assembledJarsInfo{
		"client": {
			generatedJarPath: "assembly/client/target/alluxio-assembly-client-%v-jar-with-dependencies.jar",
			tarballJarPath:   "assembly/alluxio-client-%v.jar",
			fileReplacements: map[string]map[string]string{
				"libexec/alluxio-config.sh": {
					fmt.Sprintf("assembly/client/target/alluxio-assembly-client-%v-jar-with-dependencies.jar", versionPlaceholder): fmt.Sprintf("assembly/alluxio-client-%v.jar", versionPlaceholder),
				},
			},
		},
		"server": {
			generatedJarPath: "assembly/server/target/alluxio-assembly-server-%v-jar-with-dependencies.jar",
			tarballJarPath:   "assembly/alluxio-server-%v.jar",
			fileReplacements: map[string]map[string]string{
				"libexec/alluxio-config.sh": {
					fmt.Sprintf("assembly/server/target/alluxio-assembly-server-%v-jar-with-dependencies.jar", versionPlaceholder): fmt.Sprintf("assembly/alluxio-server-%v.jar", versionPlaceholder),
				},
			},
		},
		"fuseBundled": {
			generatedJarPath: "dora/integration/fuse/target/alluxio-integration-fuse-%v-jar-with-dependencies.jar",
			tarballJarPath:   "dora/integration/fuse/alluxio-fuse-%v.jar",
			fileReplacements: map[string]map[string]string{
				"dora/integration/fuse/bin/alluxio-fuse": {
					fmt.Sprintf("target/alluxio-integration-fuse-%v-jar-with-dependencies.jar", versionPlaceholder): fmt.Sprintf("alluxio-fuse-%v.jar", versionPlaceholder),
				},
			},
		},
		"fuseStandalone": {
			generatedJarPath: "dora/integration/fuse/target/alluxio-integration-fuse-%v-jar-with-dependencies.jar",
			tarballJarPath:   "lib/alluxio-fuse-%v.jar",
			fileReplacements: map[string]map[string]string{
				"dora/integration/fuse/bin/alluxio-fuse": {
					fmt.Sprintf("target/alluxio-integration-fuse-%v-jar-with-dependencies.jar", versionPlaceholder): fmt.Sprintf("../../../lib/alluxio-fuse-%v.jar", versionPlaceholder),
				},
			},
		},
	}
)

func collectTarballContents(opts *buildOpts, repoBuildDir, dstDir, alluxioVersion string) error {
	for _, f := range opts.tarball.FileList {
		if err := copyFileForTarball(filepath.Join(repoBuildDir, f), filepath.Join(dstDir, f)); err != nil {
			return stacktrace.Propagate(err, "error copying file %v", f)
		}
	}
	if opts.tarball.ClientJarName != "" {
		// copy client jar
		clientJarPath := opts.tarball.clientJarPath(alluxioVersion)
		if err := copyFileForTarball(filepath.Join(repoBuildDir, clientJarPath), filepath.Join(dstDir, clientJarPath)); err != nil {
			return stacktrace.Propagate(err, "error copying file %v", clientJarPath)
		}
	}

	// add assembly jars, rename jars, and update name used in scripts
	for _, n := range opts.tarball.AssemblyJars {
		a, ok := assembledJars[n]
		if !ok {
			return stacktrace.NewError("no assembly jar named %v", n)
		}
		src := filepath.Join(repoBuildDir, fmt.Sprintf(a.generatedJarPath, alluxioVersion))
		dst := filepath.Join(dstDir, fmt.Sprintf(a.tarballJarPath, alluxioVersion))
		if err := copyFileForTarball(src, dst); err != nil {
			return stacktrace.Propagate(err, "error copying file from %v to %v", src, dst)
		}

		// replace corresponding reference in scripts
		for filePath, replacements := range a.fileReplacements {
			replacementFile := filepath.Join(dstDir, filePath)
			stat, err := os.Stat(replacementFile)
			if err != nil {
				return stacktrace.Propagate(err, "error listing file at %v", replacementFile)
			}
			contents, err := ioutil.ReadFile(replacementFile)
			if err != nil {
				return stacktrace.Propagate(err, "error reading file at %v", replacementFile)
			}
			for find, replace := range replacements {
				// ex. "alluxio-assembly-client-${VERSION}-jar-with-dependencies.jar" -> "alluxio-assembly-client-${VERSION}.jar
				contents = bytes.ReplaceAll(contents, []byte(find), []byte(replace))
			}
			if err := ioutil.WriteFile(replacementFile, contents, stat.Mode()); err != nil {
				return stacktrace.Propagate(err, "error overwriting file at %v", replacementFile)
			}
		}
	}

	// create symlinks
	for linkFile, linkPath := range opts.tarball.Symlinks {
		dst := filepath.Join(dstDir, linkFile)
		if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
			return stacktrace.Propagate(err, "error creating parent directory of %v", dst)
		}
		log.Printf("Creating symlink at %v to %v", dst, linkPath)
		if err := command.RunF("ln -s %v %v", linkPath, dst); err != nil {
			return stacktrace.Propagate(err, "error creating symlink at %v to %v", dst, linkPath)
		}
	}

	// add module jars
	for _, l := range opts.libModules {
		if err := l.copyFileForTarball(repoBuildDir, dstDir, alluxioVersion); err != nil {
			return stacktrace.Propagate(err, "error copying file")
		}
	}
	for _, m := range opts.pluginModules {
		if err := m.copyFileForTarball(repoBuildDir, dstDir, alluxioVersion); err != nil {
			return stacktrace.Propagate(err, "error copying file")
		}
	}

	if !opts.tarball.SkipCopyWebUi {
		// copy web ui directories
		for _, d := range webUiDirs {
			src := filepath.Join(repoBuildDir, d, webuiBuildDir)
			dst := filepath.Join(dstDir, d)
			if err := copyDirForTarball(src, dst); err != nil {
				return stacktrace.Propagate(err, "error copying dir from %v to %v", src, dst)
			}
		}
	}

	// create empty directories
	for _, d := range opts.tarball.EmptyDirList {
		dst := filepath.Join(dstDir, d)
		log.Printf("Creating empty directory %v", dst)
		if err := os.MkdirAll(dst, 0755); err != nil {
			return stacktrace.Propagate(err, "error creating directory at %v", dst)
		}
	}

	// logs/user/ directory needs to be fully accessible any user
	// otherwise, preparation operations such as mounting ramdisk cannot write its logs
	logsUserDir := filepath.Join(dstDir, "logs", "user")
	if err := os.MkdirAll(logsUserDir, 0755); err != nil {
		return stacktrace.Propagate(err, "error creating directory %v", logsUserDir)
	}
	// for some reason, the mkdir command doesn't set 777 permissions, so use chmod
	if err := os.Chmod(logsUserDir, 0777); err != nil {
		return stacktrace.Propagate(err, "error setting permissions on %v", logsUserDir)
	}

	return nil
}

func copyFileForTarball(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return stacktrace.Propagate(err, "error creating parent directory of %v", dst)
	}
	log.Printf("Copying %v to %v", src, dst)
	if err := command.RunF("cp %v %v", src, dst); err != nil {
		return stacktrace.Propagate(err, "error copying %v to %v", src, dst)
	}
	return nil
}

func copyDirForTarball(src, dst string) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return stacktrace.Propagate(err, "error creating parent directory of %v", dst)
	}
	log.Printf("Copying %v to %v", src, dst)
	if err := command.RunF("cp -r %v %v", src, dst); err != nil {
		return stacktrace.Propagate(err, "error copying %v to %v", src, dst)
	}
	return nil
}
