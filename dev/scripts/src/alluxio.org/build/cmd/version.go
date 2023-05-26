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
	"fmt"
	"io/ioutil"
	"path/filepath"
	"regexp"

	"github.com/palantir/stacktrace"
)

func VersionF() error {
	ver, err := alluxioVersionFromPom()
	if err != nil {
		return stacktrace.Propagate(err, "error finding alluxio version from pom")
	}
	fmt.Println()
	fmt.Println("Alluxio version from pom.xml:", ver)
	return nil
}

var versionRe = regexp.MustCompile(".*<version>(.*)</version>.*")

func alluxioVersionFromPom() (string, error) {
	repoRoot, err := findRepoRoot()
	if err != nil {
		return "", stacktrace.Propagate(err, "error finding repo root")
	}
	rootPomPath := filepath.Join(repoRoot, "pom.xml")
	contents, err := ioutil.ReadFile(rootPomPath)
	if err != nil {
		return "", stacktrace.Propagate(err, "error reading %v", rootPomPath)
	}
	matches := versionRe.FindStringSubmatch(string(contents))
	if len(matches) < 2 {
		return "", stacktrace.NewError("did not find any matching version tag in %v", rootPomPath)
	}
	return matches[1], nil
}
