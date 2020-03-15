package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
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

	// outputs
	markdownErrors map[string][]string // list of errors found in each markdown file
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
			return fmt.Errorf("error traversing through md files in %v", filepath.Join(docsPath, langDir))
		}
	}

	if len(ctx.markdownErrors) > 0 {
		errLines := []string{"Errors found in documentation markdown"}
		for f, errs := range ctx.markdownErrors {

			errLines = append(errLines, fmt.Sprintf("  %v:", strings.TrimPrefix(repoRoot+"/", docsPath)))
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
			categoryNames[strings.TrimPrefix(l, categoryPrefix)] = struct{}{}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error scanning file: %v", err)
	}
	return categoryNames, nil
}

// checkFile parses the given markdown file and appends errors found in its contents
func checkFile(mdFile string, ctx *checkContext) error {
	f, err := os.Open(mdFile)
	if err != nil {
		return fmt.Errorf("error opening file at %v: %v", mdFile, err)
	}
	defer f.Close()

	var headers []string
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
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error scanning file: %v", err)
	}

	checkHeader(headers, mdFile, ctx)
	return nil
}

const groupKey = "group"
var headerKeys = StringSet{
	"layout":   {},
	"title":    {},
	"nickname": {},
	groupKey:    {},
	"priority": {},
}

// checkHeader validates the header lines
func checkHeader(headers []string, mdFile string, ctx *checkContext) {
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

type StringSet map[string]struct{}

func (s StringSet) String() string {
	var ret []string
	for k := range s {
		ret = append(ret, k)
	}
	return fmt.Sprintf("[%s]", strings.Join(ret, ", "))
}
