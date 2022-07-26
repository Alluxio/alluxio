package cmd

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"
)

type UfsVersionDetails struct {
	VersionProp         string
	VersionEnumFilePath string
}

var (
	ufsTypeToVersionDetails = map[string]*UfsVersionDetails{
		"hdfs": {
			VersionProp:         "ufs.hadoop.version",
			VersionEnumFilePath: "underfs/hdfs/src/main/java/alluxio/underfs/hdfs/HdfsVersion.java",
		},
	}
)

func CheckUfsVersions() error {
	parsedFiles := map[string]map[string]string{} // mapping of file path to version to regex pattern

	for _, m := range ufsModules {
		u, ok := ufsTypeToVersionDetails[m.ufsType]
		if !ok {
			continue
		}
		mavenVersionRe, err := regexp.Compile(fmt.Sprintf(`.*\-D%s=(?P<version>\S+).*`, regexp.QuoteMeta(u.VersionProp)))
		if err != nil {
			return fmt.Errorf("error compiling regex pattern from version property %v", u.VersionProp)
		}
		match := mavenVersionRe.FindStringSubmatch(m.mavenArgs)
		if len(match) != 2 {
			return fmt.Errorf("did not find the correct number of regex matches within %v for %v",
				m.mavenArgs, u.VersionProp)
		}
		// first match is the full string match, second is the named match
		compiledVersion := match[1]

		parsedVersions, ok := parsedFiles[u.VersionEnumFilePath]
		if !ok {
			parsed, err := parseEnumFile(u.VersionEnumFilePath)
			if err != nil {
				return err
			}
			parsedFiles[u.VersionEnumFilePath] = parsed
			parsedVersions = parsed
		}
		regexPattern, ok := parsedVersions[m.name]
		if !ok {
			return fmt.Errorf("versions file %v does not contain an enum for %v", u.VersionEnumFilePath, m.name)
		}
		enumVersionRe, err := regexp.Compile(regexPattern)
		if err != nil {
			return fmt.Errorf("could not compile regex pattern %v for version %v in file %v",
				regexPattern, compiledVersion, u.VersionEnumFilePath)
		}
		// the regex pattern defined in the enum file should match on version being used to compile the ufs jar
		if !enumVersionRe.MatchString(compiledVersion) {
			return fmt.Errorf("regex %v defined for version %v in file %v does not match version defined in maven args %v",
				regexPattern, m.name, u.VersionEnumFilePath, compiledVersion)
		}
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
		return nil, fmt.Errorf("error opening file at %v: %v", filePath, err)
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
