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
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/palantir/stacktrace"

	"alluxio.org/build/artifact"
	"alluxio.org/command"
)

const (
	binSuffix      = "-bin"
	clientJarPathF = "client/alluxio-%v-client.jar"
	tarGzExt       = ".tar.gz"
	webuiBuildDir  = "build"
)

var webUiDirs = []string{
	"webui/master",
	"webui/worker",
}

func TarballF(args []string) error {
	cmd := flag.NewFlagSet(Tarball, flag.ExitOnError)
	// parse flags
	opts, err := parseTarballFlags(cmd, args)
	if err != nil {
		return stacktrace.Propagate(err, "error parsing build flags")
	}
	alluxioVersion, err := alluxioVersionFromPom()
	if err != nil {
		return stacktrace.Propagate(err, "error parsing version string")
	}
	if opts.artifactOutput != "" {
		a, err := artifact.NewArtifact(
			artifact.TarballArtifact,
			opts.outputDir,
			strings.ReplaceAll(opts.targetName, versionPlaceholder, alluxioVersion),
			alluxioVersion,
			nil,
		)
		if err != nil {
			return stacktrace.Propagate(err, "error adding artifact")
		}
		return a.WriteToFile(opts.artifactOutput)
	}
	if err := buildTarball(opts); err != nil {
		return stacktrace.Propagate(err, "error building tarball")
	}
	return nil
}

func buildTarball(opts *buildOpts) error {
	// prepare repository
	repoRoot := findRepoRoot()
	repoBuildDir := repoRoot
	if !opts.skipRepoCopy {
		// create temporary copy of repository to build from
		tempDir, err := copyRepoToTempDir(repoRoot)
		if tempDir == "" {
			defer os.RemoveAll(tempDir)
			log.Printf("Preparing temp repo dir at %v", tempDir)
		}
		if err != nil {
			return stacktrace.Propagate(err, "error copying repo to temp directory")
		}
		repoBuildDir = tempDir
	}
	alluxioVersion, err := alluxioVersionFromPom()
	if err != nil {
		return stacktrace.Propagate(err, "error parsing version string")
	}

	// build base project via maven
	baseMvnCmd := constructMavenCmd(opts.mavenArgs)
	if opts.dryRun {
		log.Printf("Skip running maven command:\n%v", baseMvnCmd)
		// need to create libexec/version.sh
		if _, err := os.Stat(filepath.Join(repoBuildDir, "libexec", "version.sh")); os.IsNotExist(err) {
			if err := command.New("./build/version/write_version.sh").WithDir(repoBuildDir).Run(); err != nil {
				return stacktrace.Propagate(err, "error creating libexec/version.sh")
			}
		}
		// mock creation of client, assembly, and lib jars
		mockFiles := []string{
			fmt.Sprintf(clientJarPathF, alluxioVersion),
		}
		for _, info := range assembledJars {
			mockFiles = append(mockFiles, fmt.Sprintf(info.generatedJarPath, alluxioVersion))
		}
		for _, l := range opts.libModules {
			mockFiles = append(mockFiles, strings.ReplaceAll(l.GeneratedJarPath, versionPlaceholder, alluxioVersion))
		}
		for _, f := range mockFiles {
			p := filepath.Join(repoBuildDir, f)
			if err := createPlaceholderFile(p); err != nil {
				return stacktrace.Propagate(err, "error creating placeholder file for %v", f)
			}
		}
		// mock creation of web ui directories
		for _, d := range webUiDirs {
			p := filepath.Join(repoBuildDir, d, webuiBuildDir)
			if err := os.MkdirAll(p, 0755); err != nil {
				return stacktrace.Propagate(err, "error creating directory at %v", p)
			}
		}

	} else {
		log.Printf("Running maven command from %v:\n%v", repoBuildDir, baseMvnCmd)
		cmd := command.New(baseMvnCmd).WithDir(repoBuildDir)
		if !opts.suppressMavenOutput {
			cmd = cmd.SetStdout(os.Stdout)
			cmd = cmd.SetStderr(os.Stderr)
		}
		if err := cmd.Run(); err != nil {
			return stacktrace.Propagate(err, "error building project with command %v", baseMvnCmd)
		}
	}

	// build plugin modules
	for _, m := range opts.pluginModules {
		moduleMvnCmd := baseMvnCmd + " " + m.MavenArgs
		if opts.dryRun {
			log.Printf("Skip running maven command:\n%v", moduleMvnCmd)
			p := filepath.Join(repoBuildDir, strings.ReplaceAll(m.TarballJarPath, versionPlaceholder, alluxioVersion))
			if err := createPlaceholderFile(p); err != nil {
				return stacktrace.Propagate(err, "error creating placeholder file for %v", p)
			}
		} else {
			log.Printf("Running maven command from %v:\n%v", repoBuildDir, moduleMvnCmd)
			cmd := command.New(moduleMvnCmd).WithDir(repoBuildDir)
			if !opts.suppressMavenOutput {
				cmd = cmd.SetStdout(os.Stdout)
				cmd = cmd.SetStderr(os.Stderr)
			}
			if err := cmd.Run(); err != nil {
				return stacktrace.Propagate(err, "error building module with command %v", moduleMvnCmd)
			}
			// the generated jar needs to be renamed in case the same module with different args is built later
			src := filepath.Join(repoBuildDir, strings.ReplaceAll(m.GeneratedJarPath, versionPlaceholder, alluxioVersion))
			dst := filepath.Join(repoBuildDir, strings.ReplaceAll(m.TarballJarPath, versionPlaceholder, alluxioVersion))
			if err := command.RunF("mv %v %v", src, dst); err != nil {
				return stacktrace.Propagate(err, "error moving file from %v to %v", src, dst)
			}
		}
	}

	// prepare tarball contents in a destination directory
	tarballPath := filepath.Join(opts.outputDir, strings.ReplaceAll(opts.targetName, versionPlaceholder, alluxioVersion))
	if err := os.RemoveAll(tarballPath); err != nil {
		return stacktrace.Propagate(err, "error deleting %v", tarballPath)
	}
	dstDir := strings.TrimSuffix(strings.TrimSuffix(tarballPath, tarGzExt), binSuffix)
	if err := os.RemoveAll(dstDir); err != nil {
		return stacktrace.Propagate(err, "error deleting %v", dstDir)
	}
	defer os.RemoveAll(dstDir)
	log.Printf("Preparing tarball directory at %v", dstDir)

	if err := collectTarballContents(opts, repoBuildDir, dstDir, alluxioVersion); err != nil {
		return stacktrace.Propagate(err, "error preparing tarball directory contents")
	}

	// create tarball
	log.Printf("Creating tarball")
	if err := os.RemoveAll(tarballPath); err != nil {
		return stacktrace.Propagate(err, "error deleting path %v", tarballPath)
	}
	if err := os.MkdirAll(filepath.Dir(tarballPath), os.ModePerm); err != nil {
		return stacktrace.Propagate(err, "error creating %v", filepath.Dir(tarballPath))
	}
	if err := command.NewF("tar -czvf %v %v", tarballPath, filepath.Base(dstDir)).
		WithDir(filepath.Dir(dstDir)).
		Env("COPYFILE_DISABLE", "1"). // mac-specific issue: https://superuser.com/questions/259703/get-mac-tar-to-stop-putting-filenames-in-tar-archives
		Run(); err != nil {
		return stacktrace.Propagate(err, "error creating tarball")
	}
	log.Printf("Tarball generated at %v", tarballPath)

	return nil
}

func constructMavenCmd(mvnArgs []string) string {
	cmd := []string{
		"mvn",
		"-am",                  // "also make": build dependent projects if a project list via `-pl` is specified
		"clean",                // remove previously generated files
		"install",              // maven build
		"-DskipTests",          // skip unit tests
		"-Dfindbugs.skip",      // skip findbugs static analysis check
		"-Dmaven.javadoc.skip", // skip javadoc generation
		"-Dcheckstyle.skip",    // skip checkstyle static check
		"-Prelease",            // release profile specified in root pom.xml, to build dependency-reduced-pom.xml generated by shading plugin
	}
	if len(mvnArgs) > 0 {
		cmd = append(cmd, mvnArgs...)
	}

	// Ensure that the "-T" parameter passed from "-mvn_args" can take effect,
	// because only the first -T parameter in "mvn" command will take effect.
	// If the -T parameter is not given in "-mvn_args", this configuration will take effect.
	cmd = append(cmd, "-T", "1")
	return strings.Join(cmd, " ")
}

func createPlaceholderFile(p string) error {
	log.Printf("Creating placeholder file for %v", p)
	if _, err := os.Stat(p); os.IsNotExist(err) {
		if err := os.MkdirAll(filepath.Dir(p), os.ModePerm); err != nil {
			return stacktrace.Propagate(err, "error creating parent directory of %v", p)
		}
		if err := command.RunF("touch %v", p); err != nil {
			return stacktrace.Propagate(err, "error creating placeholder file at %v", p)
		}
	}
	return nil
}
