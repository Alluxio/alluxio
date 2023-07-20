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

package main

import (
	"bufio"
	"fmt"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"

	"bytes"
	"io"
)

const (
	htmlType = ".html"
	mdType   = ".md"
	enPath   = "en"
)

func main() {
	if err := run(); err != nil {
		log.Fatalln(err)
	}
	log.Println("Documentation check succeeded")
}

type checkContext struct {
	// inputs
	docsPath string // path to docs directory in repository

	// intermediate
	knownFiles    StringSet                  // file paths of files that can be referenced by markdown files
	markdownLinks map[string][]*relativeLink // list of relative links found in each markdown file

	// outputs
	markdownErrors map[string][]string // list of errors found in each markdown file
}

type relativeLink struct {
	line int
	path string
}

func (ctx *checkContext) addError(mdFile string, lineNum int, format string, args ...interface{}) {
	msg := fmt.Sprintf("%d: ", lineNum) + fmt.Sprintf(format, args...)
	ctx.markdownErrors[mdFile] = append(ctx.markdownErrors[mdFile], msg)
}

func run() error {
	// check that script is being run from repo root
	const docsDir, configYml, menuEnYml = "docs", "_config.yml", "_data/menu-en.yml"
	repoRoot, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("could not get current working directory: %v", err)
	}
	// copy contents of docs/ into tmp dir
	tmpDir, err := ioutil.TempDir("", "docsCheck")
	if err != nil {
		return fmt.Errorf("error creating temporary directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)
	if err := CopyDir(filepath.Join(repoRoot, docsDir), filepath.Join(tmpDir, docsDir)); err != nil {
		return fmt.Errorf("error copying docs directory to temp directory: %v", err)
	}

	docsPath, err := filepath.Abs(filepath.Join(tmpDir, docsDir))
	if err != nil {
		return fmt.Errorf("could not get absolute path of %v: %v", filepath.Join(repoRoot, docsDir), err)
	}
	configPath := filepath.Join(docsPath, configYml)
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return fmt.Errorf("expected to find %s in %s; script should be executed from repository root", configYml, docsDir)
	}
	menuPath := filepath.Join(docsPath, menuEnYml)
	if _, err := os.Stat(menuPath); os.IsNotExist(err) {
		return fmt.Errorf("error finding file %v", menuEnYml)
	}
	menuListOfURL, err := parseMenuUrl(menuPath)
	if err != nil {
		return fmt.Errorf("error reading menu.yml with message : \n %v", err)
	}
	if err := checkUrlMatch(docsPath, enPath, menuListOfURL); err != nil {
		return fmt.Errorf("error matching content of menu.yml with acutally list of docs in directory of docs/ with message : \n %v", err)
	}
	ctx := &checkContext{
		docsPath:       docsPath,
		knownFiles:     StringSet{},
		markdownLinks:  map[string][]*relativeLink{},
		markdownErrors: map[string][]string{},
	}

	// scan through markdown files
	for _, langDir := range []string{"en"} {
		if err := filepath.Walk(filepath.Join(docsPath, langDir),
			func(p string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if info.IsDir() {
					return nil
				}
				if strings.HasSuffix(info.Name(), ".md") {
					if err := checkFile(p, ctx); err != nil {
						return err
					}
				}
				return nil
			},
		); err != nil {
			return fmt.Errorf("error traversing through md files in %v: %v", filepath.Join(docsPath, langDir), err)
		}
	}
	// scan through img and resources directories to update known files
	for _, dir := range []string{"img", "resources"} {
		if err := filepath.Walk(filepath.Join(docsPath, dir),
			func(p string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if info.IsDir() {
					return nil
				}
				ctx.knownFiles.Add(strings.TrimPrefix(p, docsPath))
				return nil
			},
		); err != nil {
			return fmt.Errorf("error traversing through files in %v: %v", filepath.Join(docsPath, dir), err)
		}
	}

	ctx.checkLinks()

	if len(ctx.markdownErrors) > 0 {
		errLines := []string{"Errors found in documentation markdown"}
		for f, errs := range ctx.markdownErrors {
			errLines = append(errLines, fmt.Sprintf("  %v:", strings.TrimPrefix(f, repoRoot)))
			for _, err := range errs {
				errLines = append(errLines, fmt.Sprintf("    %s", err))
			}
		}
		return fmt.Errorf("%v", strings.Join(errLines, "\n"))
	}

	return nil
}

var (
	// general format of a relative link, where the link will be computed by a jekyll function encapsulated in {{ }}
	relativeLinkRe = regexp.MustCompile(`\[.+\]\({{.*}}(#.+)?\)`)
	// path encapsulated in ' ' could have an optional search query "?q=queryStr" and/or an optional anchor reference "#anchor"
	relativeLinkPagePathRe = regexp.MustCompile(`\[.+\]\({{ '(?P<path>[\w-./]+)(\?q=\w+)?(#.+)?' | relativize_url }}\)`)
	// accordion header must not contain a space in the header name
	accordionHeaderRe = regexp.MustCompile(`{% accordion (?P<name>.*) %}`)
	// navtab names should not contain colons
	navtabsHeaderRe     = regexp.MustCompile(`{% navtabs (?P<name>.*) %}`)
	navtabHeaderRe      = regexp.MustCompile(`{% navtab (?P<name>.*) %}`)
	invalidNavtabNameRe = regexp.MustCompile(`.*[:<>"'&].*`)
)

// checkFile parses the given markdown file and appends errors found in its contents
func checkFile(mdFile string, ctx *checkContext) error {
	f, err := os.Open(mdFile)
	if err != nil {
		return fmt.Errorf("error opening file at %v: %v", mdFile, err)
	}
	defer f.Close()

	headers := bytes.NewBuffer(nil)
	var relativeLinks []*relativeLink
	navtabsNames := map[string]struct{}{}
	navtabNames := map[string]struct{}{}
	inHeaderSection := true
	scanner := bufio.NewScanner(f)
	for i := 1; scanner.Scan(); i++ {
		l := scanner.Text()
		if inHeaderSection {
			// first empty line ends the header section
			if l == "" {
				inHeaderSection = false
			} else {
				headers.Write([]byte(l + "\n"))
			}
		}

		if relativeLinkRe.MatchString(l) {
			for _, lineMatches := range relativeLinkRe.FindAllStringSubmatch(l, -1) {
				if len(lineMatches) < 1 {
					return fmt.Errorf("expected to find at least one string submatch but found %d in line %v in file %v", len(lineMatches), l, mdFile)
				}
				relativeLinkStr := lineMatches[0]
				if !relativeLinkPagePathRe.MatchString(relativeLinkStr) {
					ctx.addError(mdFile, i, "relative link did not match expected pattern %q", relativeLinkStr)
					continue
				}
				namedMatch, err := getSingleRegexMatch(relativeLinkPagePathRe, relativeLinkStr)
				if err != nil {
					return err
				}
				relativeLinks = append(relativeLinks, &relativeLink{
					line: i,
					path: namedMatch,
				})
			}
		} else if accordionHeaderRe.MatchString(l) {
			namedMatch, err := getSingleRegexMatch(accordionHeaderRe, l)
			if err != nil {
				return err
			}
			if strings.Contains(namedMatch, " ") {
				return fmt.Errorf("accordion header %v on line %v in file %v must not contain a space", l, i, mdFile)
			}
		} else if navtabsHeaderRe.MatchString(l) {
			namedMatch, err := getSingleRegexMatch(navtabsHeaderRe, l)
			if err != nil {
				return err
			}
			if invalidNavtabNameRe.MatchString(namedMatch) {
				return fmt.Errorf("navtabs header %v on line %v in file %v must not contain invalid HTML characters or ':' only but was %v", l, i, mdFile, namedMatch)
			}
			if _, ok := navtabsNames[namedMatch]; ok {
				return fmt.Errorf("navtabs header %v on line %v in file %v is repeated", l, i, mdFile)
			}
			navtabsNames[namedMatch] = struct{}{}
			// clear existing navtab names
			navtabNames = map[string]struct{}{}
		} else if navtabHeaderRe.MatchString(l) {
			namedMatch, err := getSingleRegexMatch(navtabHeaderRe, l)
			if err != nil {
				return err
			}
			if invalidNavtabNameRe.MatchString(namedMatch) {
				return fmt.Errorf("navtab header %v on line %v in file %v must not contain invalid HTML characters or ':' only but was %v", l, i, mdFile, namedMatch)
			}
			if _, ok := navtabNames[namedMatch]; ok {
				return fmt.Errorf("navtab header %v on line %v in file %v is repeated", l, i, mdFile)
			}
			navtabNames[namedMatch] = struct{}{}
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error scanning file: %v", err)
	}

	ctx.addRelativeLinks(relativeLinks, mdFile)

	return nil
}

func getSingleRegexMatch(re *regexp.Regexp, l string) (string, error) {
	matches := re.FindStringSubmatch(l)
	if len(matches) < 2 {
		return "", fmt.Errorf("expected to find at least two string submatches but found %d = %v in link %v", len(matches), matches, l)
	}
	// note that first is the full match, second is the named match
	namedMatch := matches[1]
	if namedMatch == "" {
		return "", fmt.Errorf("encountered empty named match when parsing line %v", l)
	}
	return namedMatch, nil
}

type Subfile struct {
	Title string `yaml:"title"`
	URL   string `yaml:"url"`
}

type File struct {
	ButtonTitle string    `yaml:"buttonTitle"`
	Subfiles    []Subfile `yaml:"subfiles"`
	Subitems    []File    `yaml:"subitems"`
}

// get url from menuPath with two checks one is the buttonTitle check and the second is the url end checks
func parseMenuUrl(menuPath string) (map[string]struct{}, error) {
	content, err := ioutil.ReadFile(menuPath)
	if err != nil {
		return nil, fmt.Errorf("error reading file at %v with message : \n %v", menuPath, err)
	}
	var ret []File
	if err := yaml.Unmarshal(content, &ret); err != nil {
		return nil, fmt.Errorf("error unmarshalling config from:\n%v with message : \n %v", string(content), err)
	}
	menuMap := map[string]struct{}{}
	var errMsgs []string
	checkAndSaveUrl(ret, menuMap, errMsgs)
	if len(errMsgs) > 0 {
		return nil, fmt.Errorf("encountered errors parsing %v:\n%v", menuPath, strings.Join(errMsgs, "\n"))
	}
	return menuMap, nil
}

// recursion function for more levels of docs
func checkAndSaveUrl(files []File, menuMap map[string]struct{}, errMsgs []string) {
	for _, file := range files {
		// if buttonTitle have whitespace, the button for list-nav-item in html will not expand
		if strings.ContainsAny(file.ButtonTitle, " \t\n\r") {
			errMsgs = append(errMsgs, fmt.Sprintf("error msg: whitespace is not allow in buttonTitle %v, please replace whitespace with _", file.ButtonTitle))
		}
		for _, subfile := range file.Subfiles {
			// the url need to be ended with .html, otherwise the link will not work
			if strings.HasSuffix(subfile.URL, mdType) {
				errMsgs = append(errMsgs, fmt.Sprintf("error msg: docs %v with url %v is ended with %v, please replace %v with %v", subfile.Title, subfile.URL, mdType, mdType, htmlType))
			}
			// replace the url ending to .md in order to compare with actually list of docs in directory of docs
			subfilePath := strings.Replace(subfile.URL, htmlType, mdType, 1)
			menuMap[subfilePath] = struct{}{}
		}
		//recall the function until file.subitem is empty
		checkAndSaveUrl(file.Subitems, menuMap, errMsgs)
	}
}

// Check menu.yml URLs should match exactly with list of all markdown doc files
func checkUrlMatch(docsPath, checkPath string, menuListOfURL map[string]struct{}) error {
	// set of markdown file paths
	filePaths := make(map[string]struct{})
	// map of markdown file name -> file path, to detect duplicate file names
	fileNameMap := make(map[string]string)
	// go through files in the docs directory
	if err := filepath.Walk(filepath.Join(docsPath, checkPath), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// only look for files not directory
		if !info.IsDir() && info.Name() != "index.html" {
			// get the relativePath which is the latter part of path based on the base path.
			// For example "en/table/xxxx.md" from "/Users/zijianzhu/enterprise/submodules/alluxio/docs/en/table/xxxx.md"
			relativePath, err := filepath.Rel(docsPath, path)
			if err != nil {
				return fmt.Errorf("error getting the relative path from %v with message: \n %v", path, err)
			}
			// add `/` in front of relativePath to match the format of path in menu.yml
			relativePath = fmt.Sprintf("/%v", relativePath)
			filePaths[relativePath] = struct{}{}
			fileName := info.Name()
			if path, ok := fileNameMap[fileName]; ok {
				return fmt.Errorf("error having duplicate file name, the file %v at %v has the same name with file %v at %v", fileName, relativePath, fileName, path)
			} else {
				fileNameMap[fileName] = relativePath
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("error walking thorugh %v with message: \n %v", docsPath, err)
	}
	// check the diff
	switch {
	case len(menuListOfURL) > len(filePaths):
		result := compareDiff(menuListOfURL, filePaths)
		return fmt.Errorf("error matching menu.yml with list of docs in directory of docs, following docs in menu.yml are no longer in directory of docs: %v", result)
	case len(menuListOfURL) < len(filePaths):
		result := compareDiff(filePaths, menuListOfURL)
		return fmt.Errorf("error matching menu.yml with list of docs in directory of docs, following docs are not in the menu.yml: %v", result)
	case !reflect.DeepEqual(menuListOfURL, filePaths):
		extra := compareDiff(menuListOfURL, filePaths)
		missing := compareDiff(filePaths, menuListOfURL)
		return fmt.Errorf("error matching menu.yml with list of docs in directory of docs,  following docs in menu.yml are no longer in directory of docs: %v and following docs are not in the menu.yml: %v ", extra, missing)
	default:
		return nil
	}
}
func compareDiff(longMap, shortMap map[string]struct{}) []string {
	var diffList []string
	for key, _ := range longMap {
		if _, ok := shortMap[key]; !ok {
			diffList = append(diffList, key)
		}
	}
	return diffList
}

type Header struct {
	Layout   string `yaml:"layout" binding:"required"`
	Title    string `yaml:"title" binding:"required"`
	Nickname string `yaml:"nickname"`
	Group    string `yaml:"group" binding:"required"`
	Priority int    `yaml:"priority"`
}

// addRelativeLinks updates knownFiles and markdownLinks
func (ctx *checkContext) addRelativeLinks(relativeLinks []*relativeLink, mdFile string) {
	// find the relative path of the markdown file start from repoRoot/docs/ and replace .md with .html
	htmlPath := strings.TrimSuffix(strings.TrimPrefix(mdFile, ctx.docsPath), ".md") + ".html"
	ctx.knownFiles.Add(htmlPath)
	ctx.markdownLinks[mdFile] = relativeLinks
}

// checkLinks validates that each markdownLink corresponds to a known markdownFile
func (ctx *checkContext) checkLinks() {
	for mdFile, relativeLinks := range ctx.markdownLinks {
		for _, relativeLink := range relativeLinks {
			if _, ok := ctx.knownFiles[relativeLink.path]; !ok {
				ctx.addError(mdFile, relativeLink.line, "relative link pointed to unknown file %v", relativeLink.path)
			}
		}
	}
}

type StringSet map[string]struct{}

func (s StringSet) Add(key string) {
	s[key] = struct{}{}
}

func (s StringSet) String() string {
	var ret []string
	for k := range s {
		ret = append(ret, k)
	}
	return fmt.Sprintf("[%s]", strings.Join(ret, ", "))
}

// CopyDir copies a source directory to the given destination
func CopyDir(src, dst string) error {
	srcFile, err := os.Stat(src)
	if err != nil {
		return err
	}
	if !srcFile.IsDir() {
		return fmt.Errorf("source %q is not a directory", src)
	}

	// create dest dir
	if err := os.MkdirAll(dst, srcFile.Mode()); err != nil {
		return err
	}

	entries, err := ioutil.ReadDir(src)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		sfp := filepath.Join(src, entry.Name())
		dfp := filepath.Join(dst, entry.Name())
		if entry.IsDir() {
			if err := CopyDir(sfp, dfp); err != nil {
				return err
			}
		} else if entry.Mode()&os.ModeSymlink != 0 {
			if err := os.Symlink(sfp, dfp); err != nil {
				return err
			}
		} else {
			if err := CopyFile(sfp, dfp); err != nil {
				return err
			}
		}
	}
	return nil
}

// CopyFile copies a file from the given source to the given destination.
// The destination should not exist.
func CopyFile(src, dst string) error {
	info, err := os.Stat(src)
	if err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_RDWR|os.O_CREATE|os.O_TRUNC, info.Mode())
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err = io.Copy(out, in); err != nil {
		return err
	}
	return nil
}
