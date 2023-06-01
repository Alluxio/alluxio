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
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/palantir/stacktrace"
	"gopkg.in/yaml.v3"

	"alluxio.org/build/artifact"
	"alluxio.org/command"
)

const (
	dockerYmlFile = "src/alluxio.org/build/docker.yml"

	tempAlluxioTarballName        = "alluxio-tmp.tar.gz"
	tempAlluxioTarballPlaceholder = "${ALLUXIO_TEMP_TARBALL}"
)

type DockerImage struct {
	BuildArgs  []string `yaml:"buildArgs,omitempty"`
	BuildDir   string   `yaml:"buildDir"`
	Dockerfile string   `yaml:"dockerfile"`
	Tag        string   `yaml:"tag"`
	TargetName string   `yaml:"targetName,omitempty"`
	Dependency string   `yaml:"dependency,omitempty"`

	outputTarball string `yaml:"-"`
}

type dockerBuildOpts struct {
	*buildOpts

	image       string
	tarballPath string
}

var (
	dockerImages = map[string]*DockerImage{}
)

func init() {
	wd, err := os.Getwd()
	if err != nil {
		panic(stacktrace.Propagate(err, "error getting current working directory"))
	}
	dockerYmlPath := filepath.Join(wd, dockerYmlFile)
	content, err := ioutil.ReadFile(dockerYmlPath)
	if err != nil {
		panic(stacktrace.Propagate(err, "error reading file at %v", dockerYmlPath))
	}
	if err := yaml.Unmarshal(content, &dockerImages); err != nil {
		panic(stacktrace.Propagate(err, "error unmarshalling docker images from:\n%v", string(content)))
	}
}

func DockerF(args []string) error {
	cmd := flag.NewFlagSet(Docker, flag.ExitOnError)
	opts := &dockerBuildOpts{}
	// docker flags
	cmd.StringVar(&opts.image, "image", "", fmt.Sprintf("Choose the docker image to build. See available images at %v", dockerYmlFile))
	cmd.StringVar(&opts.tarballPath, "tarballPath", "", "Set to use existing tarball for building docker image")
	// parse flags
	tOpts, err := parseTarballFlags(cmd, args)
	if err != nil {
		return stacktrace.Propagate(err, "error parsing build flags")
	}
	image, ok := dockerImages[opts.image]
	if !ok {
		return stacktrace.NewError("must provide valid 'image' arg")
	}
	alluxioVersion, err := alluxioVersionFromPom()
	if err != nil {
		return stacktrace.Propagate(err, "error parsing version string")
	}
	opts.buildOpts = tOpts
	if opts.artifactOutput != "" {
		artifact, err := artifact.NewArtifact(
			artifact.DockerArtifact,
			opts.outputDir,
			strings.ReplaceAll(dockerImages[opts.image].TargetName, versionPlaceholder, alluxioVersion),
			alluxioVersion,
			map[string]string{"image": opts.image})
		if err != nil {
			return stacktrace.Propagate(err, "error adding artifact")
		}
		return artifact.WriteToFile(opts.artifactOutput)
	}

	if opts.tarballPath == "" {
		if err := buildTarball(tOpts); err != nil {
			return stacktrace.Propagate(err, "error building tarball")
		}
		opts.tarballPath = filepath.Join(opts.outputDir,
			strings.ReplaceAll(opts.targetName, versionPlaceholder, alluxioVersion))
	}

	// docker logic
	if err != nil {
		return stacktrace.Propagate(err, "error finding repo root")
	}
	dockerWs := filepath.Join(findRepoRoot(), "integration", "docker")
	if err := command.RunF("cp %v %v", opts.tarballPath, filepath.Join(dockerWs, tempAlluxioTarballName)); err != nil {
		return stacktrace.Propagate(err, "error copying tarball to docker workspace")
	}
	defer os.RemoveAll(filepath.Join(dockerWs, tempAlluxioTarballName))

	if err := image.build(alluxioVersion, opts.outputDir, true); err != nil {
		return stacktrace.Propagate(err, "error building image %v", opts.image)
	}

	return nil
}

func (i *DockerImage) build(alluxioVersion, outputDir string, save bool) error {
	dockerWs := filepath.Join(findRepoRoot(), i.BuildDir)
	if i.Dependency != "" {
		if err := dockerImages[i.Dependency].build(alluxioVersion, outputDir, false); err != nil {
			return stacktrace.Propagate(err, "error building dep %v", i.Dependency)
		}
	}
	i.Tag = strings.ReplaceAll(i.Tag, versionPlaceholder, alluxioVersion)
	i.TargetName = strings.ReplaceAll(i.TargetName, versionPlaceholder, alluxioVersion)
	i.outputTarball = fmt.Sprintf("%v/%v",
		outputDir, strings.ReplaceAll(i.TargetName, versionPlaceholder, alluxioVersion))
	var buildArgs []string
	for _, a := range i.BuildArgs {
		buildArgs = append(buildArgs, fmt.Sprintf("--build-arg %v",
			strings.ReplaceAll(a, tempAlluxioTarballPlaceholder, tempAlluxioTarballName)))
	}
	cmds := []string{
		fmt.Sprintf("docker build -f %v -t %v %v %v",
			i.Dockerfile, i.Tag, strings.Join(buildArgs, ""), dockerWs),
	}
	if i.TargetName != "" && save {
		cmds = append(cmds, fmt.Sprintf("docker save %v -o %v", i.Tag, i.outputTarball))
	}
	for _, c := range cmds {
		log.Printf("Running: %v", c)
		if out, err := command.New(c).WithDir(findRepoRoot()).CombinedOutput(); err != nil {
			return stacktrace.Propagate(err, "error from running cmd: %v", string(out))
		}
	}
	return nil
}
