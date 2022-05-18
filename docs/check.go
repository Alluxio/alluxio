package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

func main() {
	if err := run(); err != nil {
		log.Fatalln(err)
	}
	log.Println("Documentation check succeeded")
}

type checkContext struct {
	// inputs
	categoryNames StringSet // category or group names defined in _config.yml
	docsPath      string    // path to docs directory in repository

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
	const docsDir, configYml = "docs", "_config.yml"
	repoRoot, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("could not get current working directory: %v", err)
	}
	docsPath, err := filepath.Abs(filepath.Join(repoRoot, docsDir))
	if err != nil {
		return fmt.Errorf("could not get absolute path of %v: %v", filepath.Join(repoRoot, docsDir), err)
	}
	configPath := filepath.Join(docsPath, configYml)
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return fmt.Errorf("expected to find %s in %s; script should be executed from repository root", configYml, docsDir)
	}

	// parse category names from config file
	categoryNames, err := parseCategoryNames(configPath)
	if err != nil {
		return fmt.Errorf("error parsing category names: %v", err)
	}

	ctx := &checkContext{
		categoryNames:  categoryNames,
		docsPath:       docsPath,
		knownFiles:     StringSet{},
		markdownLinks:  map[string][]*relativeLink{},
		markdownErrors: map[string][]string{},
	}

	// scan through markdown files
	for _, langDir := range []string{"en", "cn"} {
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

// parseCategoryNames parses the given config file for the list of category names
func parseCategoryNames(configPath string) (StringSet, error) {
	f, err := os.Open(configPath)
	if err != nil {
		return nil, fmt.Errorf("error opening file at %v: %v", configPath, err)
	}
	defer f.Close()

	const categoryListLine, categoryPrefix = "categoryList:", "  - "
	categoryNames := StringSet{}

	found := false
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		l := scanner.Text()
		if l == categoryListLine {
			found = true
			continue
		}
		if found {
			if !strings.HasPrefix(l, categoryPrefix) {
				found = false
				continue
			}
			categoryNames.Add(strings.TrimPrefix(l, categoryPrefix))
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error scanning file: %v", err)
	}
	return categoryNames, nil
}

var (
	// general format of a relative link, where the link will be computed by a jekyll function encapsulated in {{ }}
	relativeLinkRe = regexp.MustCompile(`\[.+\]\({{.*}}(#.+)?\)`)
	// path encapsulated in ' ' could have an optional search query "?q=queryStr" and/or an optional anchor reference "#anchor"
	relativeLinkPagePathRe = regexp.MustCompile(`\[.+\]\({{ '(?P<path>[\w-./]+)(\?q=\w+)?(#.+)?' | relativize_url }}\)`)
)

// checkFile parses the given markdown file and appends errors found in its contents
func checkFile(mdFile string, ctx *checkContext) error {
	f, err := os.Open(mdFile)
	if err != nil {
		return fmt.Errorf("error opening file at %v: %v", mdFile, err)
	}
	defer f.Close()

	var headers []string
	var relativeLinks []*relativeLink
	inHeaderSection := true
	scanner := bufio.NewScanner(f)
	for i := 1; scanner.Scan(); i++ {
		l := scanner.Text()
		if inHeaderSection {
			// first empty line ends the header section
			if l == "" {
				inHeaderSection = false
			} else {
				headers = append(headers, l)
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
				linkMatches := relativeLinkPagePathRe.FindStringSubmatch(relativeLinkStr)
				if len(linkMatches) < 2 {
					return fmt.Errorf("expected to find at least two string submatches but found %d = %v in link %v", len(linkMatches), linkMatches, relativeLinkStr)
				}
				// note that first is the full match, second is the named match
				namedMatch := linkMatches[1]
				if namedMatch == "" {
					return fmt.Errorf("encountered empty named match when parsing link from %v", relativeLinkStr)
				}
				relativeLinks = append(relativeLinks, &relativeLink{
					line: i,
					path: namedMatch,
				})
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error scanning file: %v", err)
	}

	ctx.checkHeader(headers, mdFile)
	ctx.addRelativeLinks(relativeLinks, mdFile)

	return nil
}

const groupKey = "group"

var headerKeys = StringSet{
	"layout":   {},
	"title":    {},
	"nickname": {},
	groupKey:   {},
	"priority": {},
}

// checkHeader validates the header lines
func (ctx *checkContext) checkHeader(headers []string, mdFile string) {
	for i := 0; i < len(headers); i++ {
		l := headers[i]
		if i == 0 || i == len(headers)-1 {
			if l != "---" {
				ctx.addError(mdFile, i, "header section should be surrounded by --- but was %v", l)
			}
			continue
		}
		split := strings.Split(l, ": ")
		if len(split) != 2 {
			ctx.addError(mdFile, i, "header line should be formatted as 'key: value' but was %v", l)
			continue
		}
		headerKey := split[0]
		if _, ok := headerKeys[headerKey]; !ok {
			ctx.addError(mdFile, i, "header keys should be one of %v but was key %q", headerKeys, headerKey)
		}
		if headerKey == groupKey {
			if _, ok := ctx.categoryNames[split[1]]; !ok {
				ctx.addError(mdFile, i, "group should be one of %v but was %q", ctx.categoryNames, split[1])
			}
		}
	}
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
