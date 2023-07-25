package cmd

import (
	"alluxio.org/build/artifact"
	"flag"
	"fmt"
	"github.com/palantir/stacktrace"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

const (
	defaultPresetsYmlFilePath = "src/alluxio.org/build/presets.yml"
)

type preset struct {
	Tarball string   `yaml:"tarball,omitempty"`
	Docker  []string `yaml:"docker,omitempty"`
}

func PresetsF(args []string) error {
	cmd := flag.NewFlagSet(Presets, flag.ExitOnError)
	// preset flags
	var flagPresetName, flagPresetsYmlFile string
	cmd.StringVar(&flagPresetsYmlFile, "presetsYmlFile", defaultPresetsYmlFilePath, "Path to presets.yml file")
	cmd.StringVar(&flagPresetName, "name", "", "Choose the preset to build. See available presets in presets.yml")

	//parse flags
	opts, err := parseTarballFlags(cmd, args)
	if err != nil {
		return stacktrace.Propagate(err, "error parsing build flags")
	}

	// parse presets.yml
	var presets map[string]preset
	{
		wd, err := os.Getwd()
		if err != nil {
			return stacktrace.Propagate(err, "error getting current working directory")
		}
		dockerYmlPath := filepath.Join(wd, flagPresetsYmlFile)
		content, err := ioutil.ReadFile(dockerYmlPath)
		if err != nil {
			return stacktrace.Propagate(err, "error reading file at %v", dockerYmlPath)
		}
		if err := yaml.Unmarshal(content, &presets); err != nil {
			return stacktrace.Propagate(err, "error unmarshalling presets from:\n%v", string(content))
		}
	}

	p, ok := presets[flagPresetName]
	if !ok {
		return stacktrace.Propagate(err, "error finding preset named %v", flagPresetName)
	}

	alluxioVersion, err := alluxioVersionFromPom()
	if err != nil {
		return stacktrace.Propagate(err, "error parsing version string")
	}

	a, err := artifact.NewArtifactGroup(alluxioVersion)
	if err != nil {
		return stacktrace.Propagate(err, "error creating artifact group")
	}
	tarball := a.Add(Tarball,
		opts.outputDir,
		strings.ReplaceAll(opts.targetName, versionPlaceholder, alluxioVersion),
		nil)
	for _, dArgs := range p.Docker {
		dOpts, err := newDockerBuildOpts(strings.Split(dArgs, " "))
		if err != nil {
			return stacktrace.Propagate(err, "error creating docker build opts")
		}
		image, ok := dOpts.dockerImages[dOpts.image]
		if !ok {
			return stacktrace.NewError("must provide valid 'image' arg")
		}
		a.Add(Docker, dOpts.outputDir, image.TargetName, dOpts.metadata)
	}

	if opts.artifactOutput != "" {
		return a.WriteToFile(opts.artifactOutput)
	}

	if err := TarballF(strings.Split(p.Tarball, " ")); err != nil {
		return stacktrace.Propagate(err, "error building tarball")
	}

	for _, dArgs := range p.Docker {
		dArgs += fmt.Sprintf(" --tarballPath %v", tarball.Path)
		if err := DockerF(strings.Split(dArgs, " ")); err != nil {
			return stacktrace.Propagate(err, "error building docker image")
		}
	}

	return nil
}
