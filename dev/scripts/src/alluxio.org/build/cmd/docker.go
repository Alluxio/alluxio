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
	"alluxio.org/common/command"
	"alluxio.org/common/repo"
)

const (
	defaultDockerYmlFilePath = "src/alluxio.org/build/docker.yml"

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

	dockerImages  map[string]*DockerImage
	dockerYmlFile string
	image         string
	tarballPath   string
}

func DockerF(args []string) error {
	cmd := flag.NewFlagSet(Docker, flag.ExitOnError)
	opts := &dockerBuildOpts{}
	// docker flags
	cmd.StringVar(&opts.dockerYmlFile, "dockerYmlFile", defaultDockerYmlFilePath, "Path to docker.yml file")
	cmd.StringVar(&opts.image, "image", "", "Choose the docker image to build. See available images in docker.yml")
	cmd.StringVar(&opts.tarballPath, "tarballPath", "", "Set to use existing tarball for building docker image")
	// parse flags
	tOpts, err := parseTarballFlags(cmd, args)
	if err != nil {
		return stacktrace.Propagate(err, "error parsing build flags")
	}

	alluxioVersion, err := alluxioVersionFromPom()
	if err != nil {
		return stacktrace.Propagate(err, "error parsing version string")
	}

	// parse available docker images in docker.yml
	{
		wd, err := os.Getwd()
		if err != nil {
			return stacktrace.Propagate(err, "error getting current working directory")
		}
		dockerYmlPath := filepath.Join(wd, opts.dockerYmlFile)
		content, err := ioutil.ReadFile(dockerYmlPath)
		if err != nil {
			return stacktrace.Propagate(err, "error reading file at %v", dockerYmlPath)
		}
		if err := yaml.Unmarshal(content, &opts.dockerImages); err != nil {
			return stacktrace.Propagate(err, "error unmarshalling docker images from:\n%v", string(content))
		}
		for _, img := range opts.dockerImages {
			img.init(alluxioVersion)
		}
	}

	image, ok := opts.dockerImages[opts.image]
	if !ok {
		return stacktrace.NewError("must provide valid 'image' arg")
	}
	opts.buildOpts = tOpts
	if opts.artifactOutput != "" {
		artifact, err := artifact.NewArtifact(
			artifact.DockerArtifact,
			opts.outputDir,
			image.TargetName,
			alluxioVersion,
			map[string]string{"docker:tag": image.Tag})
		if err != nil {
			return stacktrace.Propagate(err, "error adding artifact")
		}
		return artifact.WriteToFile(opts.artifactOutput)
	}

	dockerWs := filepath.Join(repo.FindRepoRoot(), "integration", "docker")
	tmpTarballPath := filepath.Join(dockerWs, tempAlluxioTarballName)

	// create alluxio tarball and place directly in docker workdir
	if opts.tarballPath == "" {
		tmpOpts := *tOpts
		tmpOpts.outputDir = dockerWs
		tmpOpts.targetName = tempAlluxioTarballName
		if err := buildTarball(&tmpOpts); err != nil {
			return stacktrace.Propagate(err, "error building tarball")
		}
		opts.tarballPath = tmpTarballPath
	}

	// docker logic
	if opts.tarballPath != tmpTarballPath {
		if err := command.RunF("cp %v %v", opts.tarballPath, tmpTarballPath); err != nil {
			return stacktrace.Propagate(err, "error copying tarball to docker workspace")
		}
	}
	defer os.RemoveAll(tmpTarballPath)

	if err := image.build(opts, true); err != nil {
		return stacktrace.Propagate(err, "error building image %v", opts.image)
	}

	return nil
}

func (i *DockerImage) init(alluxioVersion string) {
	i.Tag = strings.ReplaceAll(i.Tag, versionPlaceholder, alluxioVersion)
	i.TargetName = strings.ReplaceAll(i.TargetName, versionPlaceholder, alluxioVersion)
}

func (i *DockerImage) build(opts *dockerBuildOpts, save bool) error {
	dockerWs := filepath.Join(repo.FindRepoRoot(), i.BuildDir)
	if i.Dependency != "" {
		dep, ok := opts.dockerImages[i.Dependency]
		if !ok {
			return stacktrace.NewError("%v not found in list of docker images", i.Dependency)
		}
		if err := dep.build(opts, false); err != nil {
			return stacktrace.Propagate(err, "error building dep %v", i.Dependency)
		}
	}
	i.outputTarball = fmt.Sprintf("%v/%v", opts.outputDir, i.TargetName)
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
		if out, err := command.New(c).WithDir(repo.FindRepoRoot()).CombinedOutput(); err != nil {
			return stacktrace.Propagate(err, "error from running cmd: %v", string(out))
		}
	}
	return nil
}
