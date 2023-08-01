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
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/palantir/stacktrace"

	"alluxio.org/common/repo"
)

type UfsVersionDetails struct {
	VersionProp         string
	VersionEnumFilePath string
}

var (
	ufsTypeToVersionDetails = map[string]*UfsVersionDetails{
		"hdfs": {
			VersionProp:         "ufs.hadoop.version",
			VersionEnumFilePath: "dora/underfs/hdfs/src/main/java/alluxio/underfs/hdfs/HdfsVersion.java",
		},
	}
)

func UfsVersionCheckF(args []string) error {
	ret := &buildOpts{}
	cmd := flag.NewFlagSet(UfsVersionCheck, flag.ExitOnError)
	cmd.StringVar(&ret.modulesFile, "modulesFile", defaultModulesFilePath, `path to modules.yml file`)
	if err := cmd.Parse(args); err != nil {
		return stacktrace.Propagate(err, "error parsing flags")
	}
	modules, err := loadModules(ret.modulesFile)
	if err != nil {
		return stacktrace.Propagate(err, "error loading plugin modules from file at %v", ret.modulesFile)
	}

	parsedFiles := map[string]map[string]string{} // mapping of file path to version to regex pattern
	for name, m := range modules.PluginModules {
		u, ok := ufsTypeToVersionDetails[m.ModuleType]
		if !ok {
			continue
		}
		mavenVersionRe, err := regexp.Compile(fmt.Sprintf(`.*\-D%s=(?P<version>\S+).*`, regexp.QuoteMeta(u.VersionProp)))
		if err != nil {
			return stacktrace.Propagate(err, "error compiling regex pattern from version property %v", u.VersionProp)
		}
		match := mavenVersionRe.FindStringSubmatch(m.MavenArgs)
		if len(match) != 2 {
			return stacktrace.NewError("did not find the correct number of regex matches within %v for %v",
				m.MavenArgs, u.VersionProp)
		}
		// first match is the full string match, second is the named match
		compiledVersion := match[1]

		parsedVersions, ok := parsedFiles[u.VersionEnumFilePath]
		if !ok {
			parsed, err := parseEnumFile(filepath.Join(repo.FindRepoRoot(), u.VersionEnumFilePath))
			if err != nil {
				return stacktrace.Propagate(err, "error parsing enum file %v", u.VersionEnumFilePath)
			}
			parsedFiles[u.VersionEnumFilePath] = parsed
			parsedVersions = parsed
		}
		regexPattern, ok := parsedVersions[m.EnumName]
		if !ok {
			return stacktrace.NewError("versions file %v does not contain an enum for %v", u.VersionEnumFilePath, m.EnumName)
		}
		enumVersionRe, err := regexp.Compile(regexPattern)
		if err != nil {
			return stacktrace.Propagate(err, "could not compile regex pattern %v for version %v in file %v",
				regexPattern, compiledVersion, u.VersionEnumFilePath)
		}
		// the regex pattern defined in the enum file should match on version being used to compile the ufs jar
		if !enumVersionRe.MatchString(compiledVersion) {
			return stacktrace.NewError("regex %v defined for version %v in file %v does not match version defined in maven args %v",
				regexPattern, m.EnumName, u.VersionEnumFilePath, compiledVersion)
		}
		log.Printf("Verified plugin module for %v", name)
	}
	return nil
}

var (
	negativeLookaheadRe = regexp.MustCompile(`\(\?\![\w|]+\)`)
	versionEnumRe       = regexp.MustCompile(`\S+\("(?P<version>\S+)", "(?P<regex>\S+)"\),`)
)

func parseEnumFile(filePath string) (map[string]string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, stacktrace.Propagate(err, "error opening file at %v", filePath)
	}
	defer f.Close()

	foundHeader := false
	ret := map[string]string{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		// the parsing assumes that
		// - all enums are defined immediately after the definition header of the enum, comments allowed
		// - each enum line contains 2 parts, the version and regex pattern, comma separated and encapsulated within parenthesis
		// - the list of enums end with when encountering ;
		l := strings.TrimSpace(scanner.Text())
		if !foundHeader {
			if strings.HasPrefix(l, "public enum") {
				foundHeader = true
			}
			// skip lines until header is found
			continue
		} else if strings.HasPrefix(l, "//") {
			// ignore comments
			continue
		} else if l == ";" {
			// terminating line
			break
		} else {
			// parse matching line
			match := versionEnumRe.FindStringSubmatch(l)
			if len(match) != 3 {
				return nil, fmt.Errorf("did not find the correct number of regex matches within %v for pattern %v in file %v",
					filePath, l, versionEnumRe.String())
			}
			// first match is the full string match, subsequent are the named matches
			regexString := match[2]
			// unescape regex by replacing \\ with \
			regexString = strings.ReplaceAll(regexString, `\\`, `\`)
			// java regex supports negative lookahead via '?!' but golang does not, remove such expressions
			regexString = negativeLookaheadRe.ReplaceAllString(regexString, "")
			ret[match[1]] = regexString
		}
	}
	return ret, nil
}
