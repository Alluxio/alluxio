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

type ArtifactType string

const (
	TarballArtifact = ArtifactType("tarball")
	DockerArtifact  = ArtifactType("docker")
)

type RepoMetadata struct {
	CommitHash string `yaml:"CommitHash,omitempty"`
	Version    string `yaml:"Version,omitempty"`
}

type Artifact struct {
	Type     ArtifactType      `yaml:"Type,omitempty"`
	Path     string            `yaml:"Path,omitempty"`
	Version  string            `yaml:"Version,omitempty"`
	Metadata map[string]string `yaml:"Metadata,omitempty"`
}

type ArtifactGroup struct {
	Artifacts []*Artifact `yaml:"Artifacts"`

	RepoMetadata *RepoMetadata `yaml:"RepoMetadata,omitempty"`
}
