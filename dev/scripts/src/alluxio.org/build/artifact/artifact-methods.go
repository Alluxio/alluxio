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

package artifact

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/palantir/stacktrace"
	"gopkg.in/yaml.v3"

	"alluxio.org/common/command"
)

func NewArtifact(artifactType ArtifactType, outputDir, targetName, version string, metadata map[string]string) (*Artifact, error) {
	hOut, err := command.New("git rev-parse --short HEAD").Output()
	if err != nil {
		return nil, stacktrace.Propagate(err, "error getting commit hash")
	}

	return &Artifact{
		Type:     artifactType,
		Path:     filepath.Join(outputDir, targetName),
		Version:  version,
		Metadata: metadata,
		RepoMetadata: &RepoMetadata{
			CommitHash: strings.TrimSpace(string(hOut)),
			Version:    version,
		},
	}, nil
}

func (a *Artifact) WriteToFile(outputFile string) error {
	yOut, err := yaml.Marshal(a)
	if err != nil {
		return stacktrace.Propagate(err, "error marshalling artifact to yaml")
	}
	if err := os.WriteFile(outputFile, yOut, os.ModePerm); err != nil {
		return stacktrace.Propagate(err, "error writing to file")
	}
	return nil
}
