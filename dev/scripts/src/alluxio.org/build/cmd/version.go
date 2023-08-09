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
	"strings"

	"github.com/palantir/stacktrace"

	"alluxio.org/common/repo"
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

// only one <version> tag is expected in the root pom.xml that is at the level directly under the root project node
var versionRe = regexp.MustCompile(`^  <version>(.*)</version>$`)

func alluxioVersionFromPom() (string, error) {
	rootPomPath := filepath.Join(repo.FindRepoRoot(), "pom.xml")
	contents, err := ioutil.ReadFile(rootPomPath)
	if err != nil {
		return "", stacktrace.Propagate(err, "error reading %v", rootPomPath)
	}
	for _, l := range strings.Split(string(contents), "\n") {
		matches := versionRe.FindStringSubmatch(l)
		if len(matches) == 2 {
			return matches[1], nil
		}
	}
	return "", stacktrace.NewError("did not find matching version tag in %v", rootPomPath)
}
